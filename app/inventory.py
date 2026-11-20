"""库存入口：主库提交更正、按回执读存。

数据库角色由连接参数决定，而不是由 SQL 里的库名决定：
  - 主库连接（primary）：只执行更正（UPDATE inventory + INSERT inventory_corrections）；
  - 备库连接（standby）：只读（会话设为 default_transaction_read_only=on）。

一致性判据（关键）：
  回执里的 LSN 是“主库提交完成后的 WAL flush 位置”。备库要把回放推进到这个位置，
  才可能已经应用了这笔提交。仅凭页面上一瞬间看到的 replay_lsn 不可靠——查询返回时
  备库可能早已继续回放，但更重要的是“取数之前”必须已经达到回执 LSN。
  所以取数放在一个只读事务里，先读一次 pg_last_wal_replay_lsn() 再读库存；
  只有读前的回放位置 >= 回执 LSN，才允许把结果标记为“备库结果”。
"""
from __future__ import annotations

import os
import threading
import time

import psycopg2
from psycopg2 import errors as pgerrors

from receipts import Receipt, sign_receipt

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS inventory (
    product_id  int PRIMARY KEY,
    name        text NOT NULL,
    quantity    int  NOT NULL CHECK (quantity >= 0),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_corrections (
    id              bigserial PRIMARY KEY,
    product_id      int  NOT NULL REFERENCES inventory(product_id),
    quantity_before int  NOT NULL,
    quantity_after  int  NOT NULL,
    committed_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);
"""

SEED_SQL = """
INSERT INTO inventory (product_id, name, quantity) VALUES
    (1, '机械键盘', 100),
    (2, '无线鼠标', 250),
    (3, '27 寸显示器', 40)
ON CONFLICT (product_id) DO NOTHING;
"""

MAX_QUANTITY = 1_000_000
MIN_QUERY_TIMEOUT = 0.05
MAX_QUERY_TIMEOUT = 30.0


class InventoryError(Exception):
    """库存操作的业务错误（带 HTTP 状态码）。"""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


# ---------------------------------------------------------------- 连接参数

def conninfo_from_env(prefix: str = "", defaults: dict | None = None) -> dict:
    """按前缀读取 PGHOST/PGPORT/... 形式的连接参数。"""
    base = defaults or {}
    get = lambda name, fallback=None: os.environ.get(prefix + name, fallback)  # noqa: E731
    info = {
        "host": get("HOST", base.get("host", "localhost")),
        "port": int(get("PORT", base.get("port", "5432"))),
        "dbname": get("DATABASE", base.get("dbname", "sandbox")),
        "user": get("USER", base.get("user", "postgres")),
        "password": get("PASSWORD", base.get("password", "")),
        "connect_timeout": int(get("CONNECT_TIMEOUT", base.get("connect_timeout", 10))),
    }
    sslmode = get("SSLMODE", base.get("sslmode"))
    if sslmode:
        info["sslmode"] = sslmode
    return info


def inventory_conninfo(name: str, fallback: dict) -> dict:
    """取 INVENTORY_<NAME>_*；该组环境变量一个都没设时，回落到主库连接参数。"""
    prefix = f"INVENTORY_{name}_"
    env = {key: os.environ[key] for key in os.environ if key.startswith(prefix)}
    if not env:
        return dict(fallback)
    merged = {
        "host": env.get(prefix + "HOST", fallback["host"]),
        "port": int(env.get(prefix + "PORT", fallback["port"])),
        "dbname": env.get(prefix + "DATABASE", fallback["dbname"]),
        "user": env.get(prefix + "USER", fallback["user"]),
        "password": env.get(prefix + "PASSWORD", fallback["password"]),
        "connect_timeout": int(env.get(
            prefix + "CONNECT_TIMEOUT", fallback.get("connect_timeout", 10))),
    }
    sslmode = env.get(prefix + "SSLMODE", fallback.get("sslmode"))
    if sslmode:
        merged["sslmode"] = sslmode
    return merged


def connect(info: dict, readonly: bool = False, statement_timeout_ms: int | None = None):
    """建立连接；readonly=True 时会话级只读（对备库也安全）。"""
    conn = psycopg2.connect(**info)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        if readonly:
            cur.execute("SET default_transaction_read_only = on")
        if statement_timeout_ms is not None:
            cur.execute("SET statement_timeout = %s", (int(statement_timeout_ms),))
        cur.execute("SET application_name = 'inventory-ui'")
    finally:
        cur.close()
    return conn


# ---------------------------------------------------------------- 主库

def ensure_schema(primary_info: dict) -> None:
    """在主库建表并写入演示商品（幂等）。"""
    conn = connect(primary_info)
    try:
        cur = conn.cursor()
        cur.execute(SCHEMA_SQL)
        cur.execute("SELECT 1 FROM inventory LIMIT 1")
        if cur.fetchone() is None:
            cur.execute(SEED_SQL)
        cur.close()
    finally:
        conn.close()


def primary_identity(conn) -> dict:
    cur = conn.cursor()
    try:
        cur.execute("SELECT system_identifier::text FROM pg_control_system()")
        system_identifier = cur.fetchone()[0]
        cur.execute("SELECT pg_is_in_recovery()")
        in_recovery = cur.fetchone()[0]
        cur.execute("SELECT pg_current_wal_insert_lsn()::text")
        insert_lsn = cur.fetchone()[0]
    finally:
        cur.close()
    return {"system_identifier": system_identifier,
            "in_recovery": bool(in_recovery),
            "insert_lsn": insert_lsn}


def get_product(conn, product_id: int) -> dict | None:
    cur = conn.cursor()
    try:
        cur.execute("SELECT product_id, name, quantity, updated_at "
                    "FROM inventory WHERE product_id = %s", (product_id,))
        row = cur.fetchone()
    finally:
        cur.close()
    if row is None:
        return None
    return {"product_id": row[0], "name": row[1], "quantity": row[2],
            "updated_at": row[3].isoformat()}


def list_products(conn) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT product_id, name, quantity, updated_at "
                    "FROM inventory ORDER BY product_id")
        rows = cur.fetchall()
    finally:
        cur.close()
    return [{"product_id": r[0], "name": r[1], "quantity": r[2],
             "updated_at": r[3].isoformat()} for r in rows]


def read_primary(primary_info: dict, product_id: int, reason: str,
                 timeout_s: float | None = None) -> dict:
    """读主库：既是回执路径的兜底，也是页面上的“主库视图”。

    timeout_s 同时约束连接和语句，避免主库卡住时页面无限等待。
    """
    info = dict(primary_info)
    info["connect_timeout"] = max(1, int((timeout_s or 5.0) + 0.999))
    conn = connect(info, statement_timeout_ms=(
        int(timeout_s * 1000) if timeout_s else None))
    try:
        product = get_product(conn, product_id)
        if product is None:
            raise InventoryError(f"商品 {product_id} 不存在", 404)
        cur = conn.cursor()
        cur.execute("SELECT coalesce(max(id), 0) FROM inventory_corrections "
                    "WHERE product_id = %s", (product_id,))
        latest_correction_id = cur.fetchone()[0]
        cur.execute("SELECT pg_current_wal_insert_lsn()::text")
        observed_lsn = cur.fetchone()[0]
        cur.close()
    finally:
        conn.close()
    return {"source": "primary", "reason": reason, "product": product,
            "observed_lsn": observed_lsn, "replay_lsn": None,
            "latest_correction_id": latest_correction_id}


def apply_correction(primary_info: dict, secret: str, product_id: int, quantity: int):
    """在主库提交一次“把余量更正为 quantity”，返回 (令牌, 回执, 更正前余量)。

    UPDATE 和更正流水在同一事务提交；提交成功后立刻取 pg_current_wal_insert_lsn()。
    用插入位置而不是 flush 位置：synchronous_commit=off 时 flush LSN 可能落后于
    本次提交记录，会让两个不同更正拿到同一个回执位点，备库读回旧值。
    插入位置一定 >= 本事务提交记录的末尾，作为“备库至少回放到这里”的目标更保守。
    回执只在这之后签发，所以“有回执”就意味着“更正已提交”。
    """
    conn = connect(primary_info)
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SELECT product_id, name, quantity FROM inventory "
                    "WHERE product_id = %s FOR UPDATE", (product_id,))
        row = cur.fetchone()
        if row is None:
            cur.execute("ROLLBACK")
            raise InventoryError(f"商品 {product_id} 不存在", 404)
        before = row[2]
        cur.execute("UPDATE inventory SET quantity = %s, updated_at = clock_timestamp() "
                    "WHERE product_id = %s", (quantity, product_id))
        cur.execute("INSERT INTO inventory_corrections "
                    "(product_id, quantity_before, quantity_after) "
                    "VALUES (%s, %s, %s) RETURNING id, committed_at",
                    (product_id, before, quantity))
        correction_id, committed_at = cur.fetchone()
        cur.execute("COMMIT")
        cur.execute("SELECT pg_current_wal_insert_lsn()::text")
        lsn = cur.fetchone()[0]
        cur.execute("SELECT system_identifier::text FROM pg_control_system()")
        system_identifier = cur.fetchone()[0]
        cur.close()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()

    receipt = Receipt(
        version=1,
        correction_id=correction_id,
        product_id=product_id,
        quantity=quantity,
        lsn=lsn,
        committed_at=committed_at.isoformat(),
        system_identifier=system_identifier,
    )
    return sign_receipt(secret, receipt), receipt, before


# ---------------------------------------------------------------- 备库

def lsn_value(lsn: str | None) -> int:
    """把 'X/Y' 或 None 换成可比较的整数（None 视为还没回放）。"""
    if not lsn or "/" not in lsn:
        return 0
    hi, lo = lsn.split("/", 1)
    return (int(hi, 16) << 32) | int(lo, 16)


def _connect_within_deadline(info: dict, deadline: float):
    """在总时限内建立连接。

    libpq 的 connect_timeout 最小粒度是 1 秒；用户把共用时限设成 0.3 秒时，
    只靠它不足以守住预算（面对黑洞地址会拖到 1 秒以上）。所以连接放到工作线程里，
    主线程只等到 deadline；超时后连接若稍后才建立，工作线程会把它关掉，避免泄漏。

    返回 (conn, None) 或 (None, ("timeout"|"error", 说明))。
    """
    box = {}
    cancelled = threading.Event()

    def work():
        try:
            conn = psycopg2.connect(**info)
        except Exception as exc:  # 交给主线程转成失败原因
            box["error"] = exc
            return
        if cancelled.is_set():
            try:
                conn.close()
            except Exception:
                pass
        else:
            box["conn"] = conn

    thread = threading.Thread(target=work, daemon=True)
    thread.start()
    thread.join(max(deadline - time.monotonic(), 0.0))
    if thread.is_alive():
        cancelled.set()  # 之后建立的连接会被上面的 work 关掉
        return None, ("timeout", "连接备库超过共用时限")
    if "conn" in box:
        return box["conn"], None
    exc = box.get("error")
    return None, ("error", f"备库连接失败：{str(exc).strip()}" if exc else "备库连接失败")


def read_following_receipt(standby_info: dict, receipt: Receipt, timeout_s: float) -> dict:
    """按回执读备库；连接、等回放、取数共用同一个 timeout_s 时限。

    一致性判据：取数前备库的 pg_last_wal_replay_lsn() 必须 >= 回执 LSN。
    回执 LSN 取自主库提交后的 WAL 插入位置，备库回放到该位置即说明提交记录
    已被应用（回放是顺序的，不会跳过）。

    返回 dict：ok=True（读到了已回放到该提交的备库）或 ok=False + reason。
    不抛异常——任何失败都由调用方回落到主库。
    reason:
      lagging       时限内备库回放没追上回执 LSN
      timeout       连接/取数把时限耗尽（含 statement_timeout）
      unavailable   连接失败、权限/表缺失等
      not_standby   连接目标其实是主库
      system_mismatch 备库不是主库的副本（实例标识不同）
      not_found     备库上没有该商品
    """
    started = time.monotonic()
    deadline = started + timeout_s
    target = lsn_value(receipt.lsn)
    result = {
        "ok": False, "source": "standby", "reason": None, "detail": "",
        "product": None, "observed_lsn": None, "replay_lsn": None,
        "receive_lsn": None, "system_identifier": None,
        "waited_ms": None, "attempts": 0, "timeout_s": timeout_s,
    }
    conn = None
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            result.update(reason="timeout", detail="时限在连接前已用尽",
                          waited_ms=0)
            return result
        # 1) 连接：放在工作线程里等，硬性不超过剩余时限
        info = dict(standby_info)
        info["connect_timeout"] = max(
            1, min(int(info.get("connect_timeout", 10)), int(remaining + 0.999)))
        conn, failure = _connect_within_deadline(info, deadline)
        if failure is not None:
            kind, detail = failure
            result.update(
                reason="timeout" if kind == "timeout" else "unavailable",
                detail=detail,
                waited_ms=round((time.monotonic() - started) * 1000))
            return result
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SET default_transaction_read_only = on")
        cur.execute("SET application_name = 'inventory-ui-standby'")
        cur.execute("SET statement_timeout = %s", (max(1, int(remaining * 1000)),))
        cur.execute("SELECT pg_is_in_recovery()")
        in_recovery = cur.fetchone()[0]
        cur.execute("SELECT system_identifier::text FROM pg_control_system()")
        system_identifier = cur.fetchone()[0]
        cur.execute("SELECT pg_last_wal_replay_lsn()::text, "
                    "pg_last_wal_receive_lsn()::text")
        replay_lsn, receive_lsn = cur.fetchone()
        cur.close()
        result.update(replay_lsn=replay_lsn, receive_lsn=receive_lsn,
                      system_identifier=system_identifier)
        if not in_recovery:
            result.update(reason="not_standby",
                          detail="备库连接目标不是流复制备库（pg_is_in_recovery() = false）")
            return result
        if system_identifier != receipt.system_identifier:
            result.update(
                reason="system_mismatch",
                detail=(f"备库实例标识 {system_identifier} 与回执中的 "
                        f"{receipt.system_identifier} 不一致，回放位置不可比"))
            return result

        # 2) 等回放：未到回执 LSN 就小睡再查，直到时限
        while lsn_value(result["replay_lsn"]) < target:
            if time.monotonic() >= deadline:
                result.update(
                    reason="lagging",
                    detail=(f"备库回放位置 {result['replay_lsn'] or '（无）'} "
                            f"未达到回执 LSN {receipt.lsn}"),
                    waited_ms=round((time.monotonic() - started) * 1000))
                return result
            result["attempts"] += 1
            time.sleep(min(0.02, max(deadline - time.monotonic(), 0.0)))
            remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
            cur = conn.cursor()
            cur.execute("SET statement_timeout = %s", (remaining_ms,))
            cur.execute("SELECT pg_last_wal_replay_lsn()::text, "
                        "pg_last_wal_receive_lsn()::text")
            result["replay_lsn"], result["receive_lsn"] = cur.fetchone()
            cur.close()
        result["waited_ms"] = round((time.monotonic() - started) * 1000)

        # 3) 取数：只读事务里先确认回放位置，再读库存（读前 >= 回执 LSN 才算数）
        remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
        cur = conn.cursor()
        try:
            cur.execute("SET statement_timeout = %s", (remaining_ms,))
            cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cur.execute("SELECT pg_last_wal_replay_lsn()::text")
            replay_at_read = cur.fetchone()[0]
            cur.execute("SELECT product_id, name, quantity, updated_at "
                        "FROM inventory WHERE product_id = %s", (receipt.product_id,))
            row = cur.fetchone()
            # 同一快照里看该商品最新一条更正流水，用来核对“至少包含回执那次更正”。
            cur.execute("SELECT coalesce(max(id), 0) FROM inventory_corrections "
                        "WHERE product_id = %s", (receipt.product_id,))
            latest_correction_id = cur.fetchone()[0]
            cur.execute("COMMIT")
        finally:
            cur.close()
        result["replay_lsn"] = replay_at_read
        if lsn_value(replay_at_read) < target:
            result.update(
                reason="lagging",
                detail=(f"取数前备库回放位置 {replay_at_read} "
                        f"低于回执 LSN {receipt.lsn}"))
            return result
        if row is None:
            result.update(reason="not_found",
                          detail=f"商品 {receipt.product_id} 在备库不存在")
            return result
        result.update({
            "ok": True,
            "product": {"product_id": row[0], "name": row[1], "quantity": row[2],
                        "updated_at": row[3].isoformat()},
            "latest_correction_id": latest_correction_id,
            "waited_ms": round((time.monotonic() - started) * 1000),
        })
        return result
    except pgerrors.QueryCanceled as exc:
        result.update(reason="timeout",
                      detail=f"备库侧语句超过共用时限被取消：{str(exc).strip()}",
                      waited_ms=round((time.monotonic() - started) * 1000))
        return result
    except psycopg2.OperationalError as exc:
        result.update(reason="unavailable",
                      detail=f"备库连接/查询失败：{str(exc).strip()}",
                      waited_ms=round((time.monotonic() - started) * 1000))
        return result
    except Exception as exc:  # 权限、表不存在、只读库上的写等
        result.update(reason="unavailable",
                      detail=f"备库读取失败：{str(exc).strip()}",
                      waited_ms=round((time.monotonic() - started) * 1000))
        return result
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def standby_status(standby_info: dict, timeout_s: float = 2.0) -> dict:
    """备库当前状态（页面展示用）；任何失败都转成 available=false。"""
    out = {"available": False, "in_recovery": None, "replay_lsn": None,
           "receive_lsn": None, "paused": None, "pause_state": None, "detail": ""}
    conn = None
    try:
        info = dict(standby_info)
        info["connect_timeout"] = max(1, int(timeout_s + 0.999))
        conn = psycopg2.connect(**info)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SET statement_timeout = %s", (max(1, int(timeout_s * 1000)),))
        cur.execute("SELECT pg_is_in_recovery(), "
                    "pg_last_wal_replay_lsn()::text, "
                    "pg_last_wal_receive_lsn()::text")
        in_recovery, replay_lsn, receive_lsn = cur.fetchone()
        if in_recovery:
            cur.execute("SELECT pg_get_wal_replay_pause_state()")
            pause_state = cur.fetchone()[0]
        else:
            pause_state = None
        out.update({"available": True, "in_recovery": bool(in_recovery),
                    "replay_lsn": replay_lsn, "receive_lsn": receive_lsn,
                    "pause_state": pause_state,
                    "paused": (pause_state or "").startswith("paused")})
        cur.close()
    except Exception as exc:
        out["detail"] = str(exc).strip()
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return out


def replay_control(standby_info: dict, action: str) -> dict:
    """暂停 / 恢复备库回放。"""
    if action not in ("pause", "resume"):
        raise InventoryError("action 只能是 pause 或 resume", 400)
    try:
        conn = connect(standby_info)
    except psycopg2.Error as exc:
        raise InventoryError(f"连不上备库：{str(exc).strip()}", 503) from exc
    try:
        cur = conn.cursor()
        if action == "pause":
            cur.execute("SELECT pg_wal_replay_pause()")
        else:
            cur.execute("SELECT pg_wal_replay_resume()")
        cur.execute("SELECT pg_get_wal_replay_pause_state()")
        state = cur.fetchone()[0]
        cur.close()
    except psycopg2.Error as exc:
        raise InventoryError(f"备库回放控制失败：{str(exc).strip()}", 409) from exc
    finally:
        conn.close()
    return {"action": action, "pause_state": state,
            "paused": state.startswith("paused")}
