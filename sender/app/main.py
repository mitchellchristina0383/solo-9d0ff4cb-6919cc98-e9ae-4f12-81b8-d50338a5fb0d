"""发布值班发送端。

职责：登记“部署完成通知”，调用审计接收端接口，并仅凭接收端回执确认送达。
原则：
- 发送前先把“已发出、结果未知（待核）”落库，进程崩溃也不会丢失该事实；
- 只有拿到接收端内容一致的回执才置为“已确认”，绝不虚报成功；
- 丢回执 / 接收端不可达 / 发送端重启后，均可凭接收端回执核对补账。

并发的发送与核对交错时：
- “已确认”是不可撤销的终态：任何旧请求、启动核对、延迟返回的“无记录”都不得回退它；
- 每条通知同一时刻只允许一个发送在途（在途计数 `sending` 落库）；
- 发送在途期间，核对拿到的“无记录”只能说明“暂未查到”，不能据此判定可安全重发；
- 核对结论带状态版本号与在途检查，落后于已发生的状态变更时不落库；
- 已确认的行绝不保留回执号为空 / 错误残留等与状态矛盾的信息。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

DB_PATH = os.environ.get("DB_PATH", "/data/sender.db")
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://localhost:8001").rstrip("/")
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "3"))
STARTUP_RECONCILE = os.environ.get("STARTUP_RECONCILE", "1") == "1"
MAX_QUERY_DELAY = 10.0
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

STATUS_PENDING = "PENDING"        # 待发送：已登记，尚未发送或已确认对方未记账
STATUS_UNCERTAIN = "UNCERTAIN"    # 待核：已发出但回执缺失/对方不可达，是否送达未知
STATUS_ACKED = "ACKED"            # 已确认：收到接收端内容一致的回执（终态，不可撤销）
STATUS_CONFLICT = "CONFLICT"      # 冲突：同环境同发布号在接收端已存在不同内容

VALID_SIMULATE = {"lose_response", "slow", "slow_commit"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def content_hash(env: str, release_no: str, result: str) -> str:
    canonical = json.dumps(
        {"env": env, "release_no": release_no, "result": result},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


# 旧库升级：缺失列按默认值补齐
MIGRATION_COLUMNS = {
    "status_version": "INTEGER NOT NULL DEFAULT 0",  # 每次状态变更/新发送动作递增
    "sending": "INTEGER NOT NULL DEFAULT 0",          # 在途发送计数（0/1）
    "last_attempt_at": "TEXT",
}


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                result TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                receipt_id INTEGER,
                receipt_at TEXT,
                last_error TEXT,
                status_version INTEGER NOT NULL DEFAULT 0,
                sending INTEGER NOT NULL DEFAULT 0,
                last_attempt_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(env, release_no)
            )
            """
        )
        existing = {r["name"] for r in conn.execute("PRAGMA table_info(notifications)").fetchall()}
        for name, ddl in MIGRATION_COLUMNS.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE notifications ADD COLUMN {name} {ddl}")

        # 进程刚启动：此前不可能还有本进程的在途发送，清掉可能残留的在途计数，
        # 否则崩溃前留下的 sending=1 会让该通知永远无法再发送。
        conn.execute("UPDATE notifications SET sending=0 WHERE sending<>0")
        # 清理旧版本可能留下的矛盾数据：非已确认却挂着回执号/回执时间；已确认却带着错误。
        conn.execute(
            "UPDATE notifications SET receipt_id=NULL, receipt_at=NULL WHERE status<>?",
            (STATUS_ACKED,),
        )
        conn.execute("UPDATE notifications SET last_error=NULL WHERE status=?", (STATUS_ACKED,))
        conn.execute(
            "UPDATE notifications SET status=?, status_version=status_version+1, updated_at=? "
            "WHERE status=? AND sending=0 AND receipt_id IS NULL",
            (STATUS_UNCERTAIN, now_iso(), STATUS_ACKED),
        )
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def fetch_dict(conn: sqlite3.Connection, nid: int) -> dict:
    row = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
    return row_to_dict(row)


def set_fields(conn: sqlite3.Connection, nid: int, **fields) -> None:
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(
        f"UPDATE notifications SET {cols}, updated_at=? WHERE id=?",
        (*fields.values(), now_iso(), nid),
    )


def release_sending(conn: sqlite3.Connection, nid: int) -> None:
    """只释放在途计数，不改任何状态（用于发送结果被并发动作抢先的情形）。"""
    conn.execute(
        """UPDATE notifications
           SET sending=CASE WHEN sending>0 THEN sending-1 ELSE 0 END, updated_at=?
           WHERE id=?""",
        (now_iso(), nid),
    )


def apply_acked(conn: sqlite3.Connection, nid: int, receipt: dict, *, owned_send: bool) -> None:
    """写入“已确认”。这是终态：无条件覆盖任何非幂等派生的状态，但绝不反向被覆盖。

    owned_send=True 表示本次发送自己拿到了回执，可顺带释放在途计数；
    核对补账（owned_send=False）不动在途计数，交由在途发送收尾。
    """
    now = now_iso()
    if owned_send:
        conn.execute(
            """UPDATE notifications
               SET status=?, receipt_id=?, receipt_at=?, last_error=NULL,
                   sending=CASE WHEN sending>0 THEN sending-1 ELSE 0 END,
                   status_version=status_version+1, updated_at=?
               WHERE id=?""",
            (STATUS_ACKED, receipt.get("id"), receipt.get("received_at"), now, nid),
        )
    else:
        conn.execute(
            """UPDATE notifications
               SET status=?, receipt_id=?, receipt_at=?, last_error=NULL,
                   status_version=status_version+1, updated_at=?
               WHERE id=?""",
            (STATUS_ACKED, receipt.get("id"), receipt.get("received_at"), now, nid),
        )


# ---------------- 核心动作 ----------------

def do_send(nid: int, simulate: str | None = None) -> tuple[int, dict]:
    """发送（或重发）一条通知。只有拿到接收端内容一致的回执才置为已确认。

    并发约定：
    - 已确认的行直接返回，绝不重新发送、绝不回退状态；
    - 同一条通知同一时刻只允许一个发送在途，重复调用返回当前状态与说明；
    - 发送结果落库时检查“本次发送是否仍是在途者”（在途计数 + 状态版本），
      若期间已被核对确认，则保留“已确认”，只释放在途计数。
    """
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
        if row is None:
            return 404, {"detail": "通知不存在"}
        if row["status"] == STATUS_ACKED:
            out = row_to_dict(row)
            out["note"] = "已确认，无需重复发送"
            return 200, out

        # 1) 抢占发送权并先把“已发出、结果未知”落库：
        #    即使此刻进程崩溃，重启后也知道必须核对，而不是误以为未发或误报成功。
        #    status<>ACKED 是硬约束：读到的状态可能已被并发核对确认，绝不能被本次发送回退。
        now = now_iso()
        acquired = conn.execute(
            """UPDATE notifications
               SET sending=sending+1,
                   status=CASE WHEN status IN (?, ?) THEN ? ELSE status END,
                   attempt_count=attempt_count+1,
                   status_version=status_version+1,
                   last_attempt_at=?, updated_at=?
               WHERE id=? AND sending=0 AND status<>?""",
            (STATUS_PENDING, STATUS_UNCERTAIN, STATUS_UNCERTAIN, now, now, nid, STATUS_ACKED),
        )
        if acquired.rowcount == 0:
            out = fetch_dict(conn, nid)
            if out["status"] == STATUS_ACKED:
                out["note"] = "刚刚已被回执确认，无需重复发送"
            else:
                out["note"] = "已有发送在途，本次未重复发送；请等待结果或稍后核对"
            return 200, out

        row = fetch_dict(conn, nid)  # 以抢占后的最新状态版本为准
        version = row["status_version"]
        finalized = False

        def finalize_non_acked(status: str, error: str | None) -> None:
            """仅当没有更新的事件改写这一行时才落库（状态版本未变、发送仍在途）。"""
            nonlocal finalized
            res = conn.execute(
                """UPDATE notifications
                   SET status=?, last_error=?, sending=CASE WHEN sending>0 THEN sending-1 ELSE 0 END,
                       status_version=status_version+1, updated_at=?
                   WHERE id=? AND status_version=? AND sending>0 AND status<>?""",
                (status, error, now_iso(), nid, version, STATUS_ACKED),
            )
            finalized = True
            if res.rowcount == 0:
                # 期间已被回执确认（或出现更新的动作）：保留当前状态，只兜底释放在途计数。
                release_sending(conn, nid)

        try:
            # 2) 调用接收端
            payload = {"env": row["env"], "release_no": row["release_no"], "result": row["result"]}
            if simulate:
                payload["simulate"] = simulate
            try:
                resp = httpx.post(f"{RECEIVER_URL}/api/receipts", json=payload, timeout=HTTP_TIMEOUT)
            except httpx.HTTPError as exc:
                finalize_non_acked(
                    STATUS_UNCERTAIN,
                    f"接收端不可达或回执丢失（{exc.__class__.__name__}）",
                )
                return 200, fetch_dict(conn, nid)

            # 3) 只有接收端 2xx 回执且内容一致才算送达
            if resp.status_code in (200, 201):
                try:
                    receipt = resp.json()
                except ValueError:
                    receipt = None
                if receipt is None:
                    finalize_non_acked(STATUS_UNCERTAIN, "回执无法解析，视为回执未明")
                elif receipt.get("content_hash") == row["content_hash"] and receipt.get("id") is not None:
                    apply_acked(conn, nid, receipt, owned_send=True)
                    finalized = True
                elif receipt.get("content_hash") != row["content_hash"]:
                    finalize_non_acked(STATUS_CONFLICT, "回执内容与本地登记不一致")
                else:
                    finalize_non_acked(STATUS_UNCERTAIN, "回执缺少回执号，视为回执未明")
            elif resp.status_code == 409:
                try:
                    detail = resp.json().get("detail", "接收端已存在不同内容")
                except ValueError:
                    detail = "接收端已存在不同内容"
                finalize_non_acked(STATUS_CONFLICT, detail)
            else:
                finalize_non_acked(STATUS_UNCERTAIN, f"接收端返回 {resp.status_code}，回执缺失")
            return 200, fetch_dict(conn, nid)
        except Exception:
            # 非预期异常（如回执解析崩溃）：兜底释放在途计数，状态保持待核，可稍后核对。
            if not finalized:
                finalize_non_acked(STATUS_UNCERTAIN, "发送过程异常，回执未明")
            raise
    finally:
        conn.close()


def do_reconcile(nid: int, delay: float | None = None) -> tuple[int, dict]:
    """凭接收端回执核对一条通知的真实状态。

    - 已确认的行不再发起核对，直接返回（终态不可撤销）；
    - 接收端不可达 / 回执未明：保持原状态（待核仍为待核），只记录待定说明；
    - 接收端明确“无记录”：仅当核对结论仍然新鲜且没有发送在途时，才把待核回退为待发送；
    - 命中同内容回执：置为已确认（终态）。
    """
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
        if row is None:
            return 404, {"detail": "通知不存在"}
        if row["status"] == STATUS_ACKED:
            out = row_to_dict(row)
            out["note"] = "已确认，无需再核对"
            return 200, out

        version = row["status_version"]
        params = {"env": row["env"], "release_no": row["release_no"]}
        if delay:
            # 仅用于演示/自测：让本次核对的响应延迟返回（可能是过期的“无记录”）
            params["delay"] = min(delay, MAX_QUERY_DELAY)
        try:
            resp = httpx.get(
                f"{RECEIVER_URL}/api/receipts",
                params=params,
                timeout=HTTP_TIMEOUT,
            )
        except httpx.HTTPError:
            # 接收端暂不可查：保持现状（待核仍为待核），绝不虚报成功，也绝不撤销确认
            conn.execute(
                """UPDATE notifications SET last_error=?, updated_at=?
                   WHERE id=? AND status<>?""",
                ("核对待定：接收端暂不可达", now_iso(), nid, STATUS_ACKED),
            )
            out = fetch_dict(conn, nid)
            out["note"] = "接收端暂不可达，状态保持不变；恢复后可继续核对"
            return 200, out

        if resp.status_code == 404:
            # 接收端明确无此记录。但“无记录”只有在没有发送在途、且没有更新的状态变更时才可信；
            # 旧请求/启动核对拿到的“无记录”不能撤销已确认，也不能把在途发送判成可重发。
            applied = conn.execute(
                """UPDATE notifications
                   SET status=?, receipt_id=NULL, receipt_at=NULL, last_error=NULL,
                       status_version=status_version+1, updated_at=?
                   WHERE id=? AND status=? AND sending=0 AND status_version=?""",
                (STATUS_PENDING, now_iso(), nid, STATUS_UNCERTAIN, version),
            )
            out = fetch_dict(conn, nid)
            if applied.rowcount > 0 and out["status"] == STATUS_PENDING and not out["sending"]:
                out["note"] = "接收端无此记录，可安全重发"
            elif out["status"] == STATUS_ACKED:
                out["note"] = "已确认，核对到的旧“无记录”已忽略"
            elif out["sending"]:
                out["note"] = "发送仍在途，接收端暂未查到记录；保持待核，待发送结果或稍后再核对"
            elif out["status"] == STATUS_PENDING:
                out["note"] = "接收端无此记录，可安全重发"
            else:
                out["note"] = "接收端无此记录，但当前状态与核对前提不符，保持现状"
            return 200, out

        if resp.status_code == 200:
            try:
                receipt = resp.json()
            except ValueError:
                receipt = None
            if receipt is None:
                conn.execute(
                    "UPDATE notifications SET last_error=?, updated_at=? WHERE id=? AND status<>?",
                    ("核对失败：回执无法解析", now_iso(), nid, STATUS_ACKED),
                )
                return 200, fetch_dict(conn, nid)
            if receipt.get("content_hash") == row["content_hash"] and receipt.get("id") is not None:
                apply_acked(conn, nid, receipt, owned_send=False)
            elif receipt.get("content_hash") == row["content_hash"]:
                conn.execute(
                    "UPDATE notifications SET last_error=?, updated_at=? WHERE id=? AND status<>?",
                    ("核对命中回执但缺少回执号，保持待核", now_iso(), nid, STATUS_ACKED),
                )
                out = fetch_dict(conn, nid)
                out["note"] = "回执未明，状态保持不变；可稍后继续核对"
                return 200, out
            else:
                res = conn.execute(
                    """UPDATE notifications
                       SET status=?, last_error=?, status_version=status_version+1, updated_at=?
                       WHERE id=? AND status<>? AND sending=0""",
                    (STATUS_CONFLICT, "接收端已存在同环境同发布号的不同内容",
                     now_iso(), nid, STATUS_ACKED),
                )
                if res.rowcount == 0:
                    out = fetch_dict(conn, nid)
                    if out["status"] != STATUS_ACKED:
                        out["note"] = "检测到内容冲突，但有发送在途；待发送结果定案"
                    else:
                        out["note"] = "已确认，核对到的冲突结论已忽略"
                    return 200, out
            return 200, fetch_dict(conn, nid)

        conn.execute(
            "UPDATE notifications SET last_error=?, updated_at=? WHERE id=? AND status<>?",
            (f"核对失败：接收端返回 {resp.status_code}", now_iso(), nid, STATUS_ACKED),
        )
        out = fetch_dict(conn, nid)
        out["note"] = "核对未得到明确回执，状态保持不变"
        return 200, out
    finally:
        conn.close()


def reconcile_all_open() -> int:
    """核对所有未结案（待核/待发送）记录，返回处理条数。"""
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT id FROM notifications WHERE status IN (?, ?)",
            (STATUS_UNCERTAIN, STATUS_PENDING),
        ).fetchall()
    finally:
        conn.close()
    for r in rows:
        do_reconcile(r["id"])
    return len(rows)


def _startup_reconcile() -> None:
    """发送端重启后主动核对所有待核记录：只凭接收端回执定案。"""
    time.sleep(2)  # 等待接收端就绪
    try:
        n = reconcile_all_open()
        print(f"[startup] 重启核对完成，共处理 {n} 条待核/待发送记录", flush=True)
    except Exception as exc:  # noqa: BLE001 - 启动核对失败不致命，可稍后手动核对
        print(f"[startup] 重启核对失败（可稍后手动核对）: {exc}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if STARTUP_RECONCILE:
        threading.Thread(target=_startup_reconcile, daemon=True).start()
    yield


app = FastAPI(title="发布值班发送端", lifespan=lifespan)


class NotificationIn(BaseModel):
    env: str
    release_no: str
    result: str


@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/notifications")
def create_notification(body: NotificationIn):
    """登记通知。同 (env, release_no) 同内容幂等去重，内容不同报 409 冲突。"""
    env, release_no, result = body.env.strip(), body.release_no.strip(), body.result.strip()
    if not env or not release_no or not result:
        return JSONResponse(status_code=400, content={"detail": "env / release_no / result 均不能为空"})
    if len(env) > 64 or len(release_no) > 64 or len(result) > 64:
        return JSONResponse(status_code=400, content={"detail": "字段长度不能超过 64 字符"})

    ch = content_hash(env, release_no, result)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM notifications WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is not None:
                conn.execute("COMMIT")
                if row["content_hash"] == ch:
                    out = row_to_dict(row)
                    out["deduplicated"] = True
                    return JSONResponse(status_code=200, content=out)
                return JSONResponse(status_code=409, content={
                    "detail": "相同环境与发布号已存在不同内容",
                    "existing": row_to_dict(row),
                })
            now = now_iso()
            cur = conn.execute(
                """INSERT INTO notifications
                   (env, release_no, result, content_hash, status, attempt_count,
                    status_version, sending, created_at, updated_at)
                   VALUES (?,?,?,?,?,0,0,0,?,?)""",
                (env, release_no, result, ch, STATUS_PENDING, now, now),
            )
            nid = cur.lastrowid
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        return JSONResponse(status_code=201, content=fetch_dict(conn, nid))
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500, content={"detail": f"本地写入失败：{exc}"})
    finally:
        conn.close()


@app.get("/api/notifications")
def list_notifications():
    conn = connect()
    try:
        rows = conn.execute("SELECT * FROM notifications ORDER BY id DESC").fetchall()
        return {"items": [row_to_dict(r) for r in rows]}
    finally:
        conn.close()


@app.post("/api/notifications/{nid}/send")
def send_notification(nid: int, simulate: str | None = Query(default=None)):
    if simulate is not None and simulate not in VALID_SIMULATE:
        return JSONResponse(status_code=400, content={"detail": f"simulate 仅支持：{sorted(VALID_SIMULATE)}"})
    code, payload = do_send(nid, simulate)
    return JSONResponse(status_code=code, content=payload)


@app.post("/api/notifications/{nid}/reconcile")
def reconcile_notification(nid: int, delay: float | None = Query(default=None, ge=0)):
    code, payload = do_reconcile(nid, delay)
    return JSONResponse(status_code=code, content=payload)


@app.post("/api/reconcile-all")
def reconcile_all_endpoint():
    n = reconcile_all_open()
    return {"reconciled": n}


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
