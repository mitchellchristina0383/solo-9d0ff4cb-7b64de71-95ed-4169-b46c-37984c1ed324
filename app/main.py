"""Flask 入口：页面 + 运行接口 + 库存入口。

环境变量：PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD / PORT
库存主备（可选，未设置时回落到主库参数）：
  INVENTORY_PRIMARY_HOST / _PORT / _DATABASE / _USER / _PASSWORD
  INVENTORY_STANDBY_HOST / _PORT / _DATABASE / _USER / _PASSWORD
  INVENTORY_SECRET       回执签名密钥（未设置则每次启动随机生成）
"""
import os
import secrets
import threading

import psycopg2
from flask import Flask, jsonify, render_template, request

import inventory
from engine import run_experiment
from presets import PRESETS
from receipts import ReceiptError, decode_receipt
from script_parser import ScriptError, parse_script

app = Flask(__name__)

# 所有请求共用一个沙箱库，同一时刻只允许跑一轮实验，避免互相重置数据
RUN_LOCK = threading.Lock()

DEFAULT_TIMEOUT = 5.0
MAX_TIMEOUT = 60.0

DEFAULT_READ_TIMEOUT = 1.0
MAX_READ_TIMEOUT = 30.0

RECEIPT_SECRET = os.environ.get("INVENTORY_SECRET") or secrets.token_urlsafe(32)
_SCHEMA_LOCK = threading.Lock()
_schema_ready = False


def conninfo():
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "sandbox"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
        "connect_timeout": 10,
    }


def inventory_conninfo():
    """库存主库 / 备库的连接参数；备库未单独配置时说明“没有备库”。"""
    primary = inventory.inventory_conninfo("PRIMARY", conninfo())
    standby = inventory.inventory_conninfo("STANDBY", conninfo())
    standalone = standby == primary
    return primary, standby, standalone


def ensure_schema_once():
    """首次访问库存页时在主库建表，失败则下次再试。"""
    global _schema_ready
    if _schema_ready:
        return
    with _SCHEMA_LOCK:
        if _schema_ready:
            return
        primary, _, _ = inventory_conninfo()
        inventory.ensure_schema(primary)
        _schema_ready = True


def error_json(exc):
    if isinstance(exc, inventory.InventoryError):
        return jsonify({"error": str(exc)}), exc.status
    if isinstance(exc, psycopg2.OperationalError):
        return jsonify({"error": f"数据库连接失败：{str(exc).strip()}"}), 503
    if isinstance(exc, psycopg2.Error):
        return jsonify({"error": f"数据库错误：{str(exc).strip()}"}), 500
    return jsonify({"error": str(exc)}), 500


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/inventory")
def inventory_page():
    return render_template("inventory.html")


@app.get("/api/presets")
def get_presets():
    return jsonify(PRESETS)


# ---------------------------------------------------------------- 库存接口

@app.get("/api/inventory")
def inventory_list():
    try:
        ensure_schema_once()
        primary, _, standalone = inventory_conninfo()
        conn = inventory.connect(primary)
        try:
            products = inventory.list_products(conn)
            identity = inventory.primary_identity(conn)
        finally:
            conn.close()
    except Exception as exc:
        return error_json(exc)
    return jsonify({"products": products, "primary": identity,
                    "standalone": standalone})


@app.post("/api/inventory/correction")
def inventory_correction():
    body = request.get_json(force=True, silent=True) or {}
    try:
        product_id = int(body.get("product_id"))
        quantity = int(body.get("quantity"))
    except (TypeError, ValueError):
        return jsonify({"error": "product_id 和 quantity 必须是整数"}), 400
    if not 0 <= quantity <= inventory.MAX_QUANTITY:
        return jsonify({"error": f"余量必须在 0..{inventory.MAX_QUANTITY} 之间"}), 400

    try:
        ensure_schema_once()
        primary, _, standalone = inventory_conninfo()
        token, receipt, before = inventory.apply_correction(
            primary, RECEIPT_SECRET, product_id, quantity)
    except Exception as exc:
        return error_json(exc)
    return jsonify({
        "receipt": receipt.public(token),
        "quantity_before": before,
        "standalone": standalone,
    })


@app.get("/api/inventory/standby")
def inventory_standby_status():
    _, standby, standalone = inventory_conninfo()
    if standalone:
        return jsonify({"available": False, "configured": False,
                        "detail": "未配置备库（INVENTORY_STANDBY_* 未设置）"})
    status = inventory.standby_status(standby)
    status["configured"] = True
    return jsonify(status)


@app.post("/api/inventory/standby/replay")
def inventory_standby_replay():
    body = request.get_json(force=True, silent=True) or {}
    _, standby, standalone = inventory_conninfo()
    if standalone:
        return jsonify({"error": "未配置备库（INVENTORY_STANDBY_* 未设置）"}), 400
    try:
        result = inventory.replay_control(standby, (body.get("action") or "").strip())
    except Exception as exc:
        return error_json(exc)
    return jsonify(result)


@app.post("/api/inventory/read")
def inventory_read():
    body = request.get_json(force=True, silent=True) or {}
    token = body.get("token") or ""
    try:
        timeout_s = float(body.get("timeout_s") or DEFAULT_READ_TIMEOUT)
    except (TypeError, ValueError):
        timeout_s = DEFAULT_READ_TIMEOUT
    timeout_s = min(max(timeout_s, 0.05), MAX_READ_TIMEOUT)

    try:
        receipt = decode_receipt(RECEIPT_SECRET, token)
    except ReceiptError as exc:
        return jsonify({"error": str(exc)}), 400

    primary, standby, standalone = inventory_conninfo()
    notes = []
    try:
        if standalone:
            notes.append("未配置备库，直接读主库")
            result = inventory.read_primary(primary, receipt.product_id,
                                            reason="standby_not_configured",
                                            timeout_s=min(timeout_s, 5.0))
        else:
            result = inventory.read_following_receipt(standby, receipt, timeout_s)
            if not result["ok"]:
                notes.append(f"备库读取未采用：{result['reason']} — {result['detail']}")
                fallback_timeout = min(timeout_s, 5.0)
                result = inventory.read_primary(primary, receipt.product_id,
                                                reason=result["reason"],
                                                timeout_s=fallback_timeout)
            else:
                notes.append("备库回放位置已达到回执 LSN，结果为备库读取")
    except Exception as exc:
        return error_json(exc)

    product = result.get("product")
    latest = result.get("latest_correction_id")
    result.update({
        "receipt": receipt.public(token),
        "verified": bool(result.get("ok")) if result["source"] == "standby" else None,
        # 读到的是“这次更正或之后”的值：结果快照里该商品的更正流水
        # 已包含回执对应的那一次（同一事务写入，id 单调）。
        "satisfies_receipt": latest is not None and latest >= receipt.correction_id,
        "notes": notes,
    })
    result.pop("latest_correction_id", None)
    result.pop("ok", None)
    result.pop("ok", None)
    return jsonify(result)


# ---------------------------------------------------------------- 沙箱实验

@app.post("/api/run")
def run():
    body = request.get_json(force=True, silent=True) or {}
    init_sql = (body.get("init_sql") or "").strip()
    script = (body.get("script") or "").strip()
    check_sql = (body.get("check_sql") or "").strip()
    try:
        timeout_s = float(body.get("timeout_s") or DEFAULT_TIMEOUT)
    except (TypeError, ValueError):
        timeout_s = DEFAULT_TIMEOUT
    timeout_s = min(max(timeout_s, 1.0), MAX_TIMEOUT)

    if not init_sql:
        return jsonify({"error": "初始数据 SQL 不能为空"}), 400
    if not check_sql:
        return jsonify({"error": "核对查询不能为空"}), 400
    try:
        steps = parse_script(script)
    except ScriptError as exc:
        return jsonify({"error": f"脚本解析失败：{exc}"}), 400

    if not RUN_LOCK.acquire(blocking=False):
        return jsonify({"error": "已有实验正在运行，请稍候（所有会话共用一个沙箱库）"}), 409
    try:
        rounds = run_experiment(conninfo(), init_sql, steps, check_sql, timeout_s)
    finally:
        RUN_LOCK.release()
    return jsonify({"rounds": rounds, "timeout_s": timeout_s})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), threaded=True)
