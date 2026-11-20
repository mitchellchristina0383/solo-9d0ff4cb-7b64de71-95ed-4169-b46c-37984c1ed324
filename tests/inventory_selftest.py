"""端到端自测：回执 + 主备读到“这次更正或之后”。

前置：
  - 一个可写主库，最好还有一个流复制备库；
  - 环境变量与 app/inventory.py 一致（INVENTORY_PRIMARY_* / INVENTORY_STANDBY_*），
    未设置时使用 PGHOST/PGPORT/... 并连同一个库（此时只测回执与主库兜底）。
  - 备库必须可连、可暂停回放（需要超级用户或 pg_wal_replay_pause 权限）。

用法：
  docker compose exec web python tests/inventory_selftest.py
  或本地：
  PYTHONPATH=app INVENTORY_PRIMARY_HOST=... INVENTORY_STANDBY_HOST=... python tests/inventory_selftest.py
"""
import importlib
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "app"))

import inventory  # noqa: E402
import receipts  # noqa: E402

FAILURES = []


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def conninfo(prefix):
    if any(k.startswith(prefix) for k in os.environ):
        return inventory.conninfo_from_env(prefix)
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "postgres"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
        "connect_timeout": 10,
    }


PRIMARY = conninfo("INVENTORY_PRIMARY_")
STANDBY = conninfo("INVENTORY_STANDBY_")
SECRET = "selftest-secret"
PRODUCT = 1


def standby_configured():
    return any(k.startswith("INVENTORY_STANDBY_") for k in os.environ)


def replay_state():
    return inventory.standby_status(STANDBY)


def wait_replay_idle(timeout=10.0):
    """等备库把已接收的 WAL 全部回放（用于测试前置/收尾）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = replay_state()
        if st.get("available") and st.get("replay_lsn") == st.get("receive_lsn"):
            return True
        time.sleep(0.05)
    return False


def correct(qty):
    token, receipt, before = inventory.apply_correction(PRIMARY, SECRET, PRODUCT, qty)
    return token, receipt, before


def read(token, timeout_s=1.0):
    receipt = receipts.decode_receipt(SECRET, token)
    result = inventory.read_following_receipt(STANDBY, receipt, timeout_s)
    if not result["ok"]:
        result = inventory.read_primary(PRIMARY, receipt.product_id,
                                        reason=result["reason"], timeout_s=5.0)
    return result


def test_receipt_and_primary():
    print("== 回执签发与主库读 ==")
    token, receipt, before = correct(10)
    check("回执可解码", receipts.decode_receipt(SECRET, token) == receipt)
    check("回执余量与提交一致", receipt.quantity == 10)
    check("回执带 LSN 与实例标识", bool(receipt.lsn) and receipt.system_identifier.isdigit())
    check("回执号稳定", receipts.receipt_id(token) == receipts.receipt_id(token))
    tampered = token[:-2] + ("AA" if not token.endswith("AA") else "BB")
    try:
        receipts.decode_receipt(SECRET, tampered)
        check("篡改回执被拒", False)
    except receipts.ReceiptError:
        check("篡改回执被拒", True)
    try:
        receipts.decode_receipt("other-secret", token)
        check("换密钥后回执被拒", False)
    except receipts.ReceiptError:
        check("换密钥后回执被拒", True)
    primary_view = inventory.read_primary(PRIMARY, PRODUCT, reason="test")
    check("主库读到本次更正", primary_view["product"]["quantity"] == 10,
          str(primary_view["product"]))

    print("== 连续更正的 LSN 严格递增 ==")
    lsn_a = correct(11)[1].lsn
    lsn_b = correct(12)[1].lsn
    check("后一次更正的 LSN 大于前一次",
          inventory.lsn_value(lsn_b) > inventory.lsn_value(lsn_a),
          f"{lsn_a} vs {lsn_b}")


def test_lag_and_catchup():
    print("== 备库滞后 → 追平 ==")
    if not standby_configured():
        print("  [SKIP] 未配置 INVENTORY_STANDBY_*，跳过主备测试")
        return
    wait_replay_idle()
    inventory.replay_control(STANDBY, "pause")
    check("暂停回放生效", inventory.standby_status(STANDBY).get("paused") is True)

    token, receipt, _ = correct(50)
    started = time.monotonic()
    result = read(token, timeout_s=1.0)
    elapsed = time.monotonic() - started
    check("滞后时回落到主库", result["source"] == "primary", str(result.get("reason")))
    check("回落原因标为 lagging", result.get("reason") == "lagging", str(result.get("reason")))
    check("主库读到本次更正值 50", result["product"]["quantity"] == 50)
    check("限时约 1 秒（未明显超时）", 0.5 <= elapsed <= 3.0, f"{elapsed:.2f}s")

    inventory.replay_control(STANDBY, "resume")
    check("恢复回放生效", inventory.standby_status(STANDBY).get("paused") is False)
    wait_replay_idle()
    token2, receipt2, _ = correct(51)
    result2 = read(token2, timeout_s=3.0)
    check("追平后读到备库", result2["source"] == "standby", str(result2))
    check("备库结果已含本次更正（51）", result2["product"]["quantity"] == 51,
          str(result2["product"]))
    check("备库读结果带 replay_lsn",
          inventory.lsn_value(result2["replay_lsn"]) >= inventory.lsn_value(receipt2.lsn))

    print("== 暂停期间连续更正，恢复后仍读到“这次或之后” ==")
    inventory.replay_control(STANDBY, "pause")
    token3, receipt3, _ = correct(60)
    correct(61)            # 之后又有一笔，备库若回放到这里则读到的可能是 61
    inventory.replay_control(STANDBY, "resume")
    result3 = read(token3, timeout_s=5.0)
    check("恢复后读到备库", result3["source"] == "standby", str(result3))
    check("读到的余量 >= 回执中的 60",
          result3["product"]["quantity"] >= 60, str(result3["product"]))


def test_deadline_budget():
    print("== 共用时限是硬上限 ==")
    if not standby_configured():
        print("  [SKIP] 未配置 INVENTORY_STANDBY_*，跳过时限测试")
        return
    wait_replay_idle()
    inventory.replay_control(STANDBY, "pause")
    try:
        token, receipt, _ = correct(80)
        for budget in (0.2, 0.5):
            started = time.monotonic()
            result = inventory.read_following_receipt(STANDBY, receipt, budget)
            elapsed = time.monotonic() - started
            check(f"回放暂停时 {budget}s 时限内返回",
                  elapsed <= budget + 0.5, f"{elapsed:.3f}s")
            check(f"{budget}s 预算按 lagging 回落", result["reason"] == "lagging",
                  str(result.get("reason")))
    finally:
        inventory.replay_control(STANDBY, "resume")

    # 不可达/黑洞地址：连接阶段的等待也不能突破时限
    blackhole = dict(STANDBY)
    blackhole.update({"host": "10.255.255.1", "port": 5432, "connect_timeout": 10})
    for budget in (0.2, 0.6):
        started = time.monotonic()
        result = inventory.read_following_receipt(blackhole, receipt, budget)
        elapsed = time.monotonic() - started
        check(f"黑洞地址 {budget}s 时限内返回",
              elapsed <= budget + 0.3, f"{elapsed:.3f}s")
        check(f"黑洞地址按 {budget}s 预算判为 timeout",
              result["reason"] == "timeout", str(result.get("reason")))


def test_disconnect():
    print("== 备库断连 ==")
    if not standby_configured():
        print("  [SKIP] 未配置 INVENTORY_STANDBY_*，跳过断连测试")
        return
    # 用一个不可达端口模拟断连，避免动真实的备库进程
    dead = dict(STANDBY)
    dead["host"] = "127.0.0.1"
    dead["port"] = 1
    dead["connect_timeout"] = 1
    token, receipt, _ = correct(70)
    started = time.monotonic()
    result = inventory.read_following_receipt(dead, receipt, 1.0)
    elapsed = time.monotonic() - started
    check("断连时备库路径失败", result["ok"] is False)
    check("失败原因标为 unavailable", result["reason"] == "unavailable",
          str(result.get("reason")))
    check("断连未拖过时限", elapsed <= 3.0, f"{elapsed:.2f}s")
    fallback = inventory.read_primary(PRIMARY, receipt.product_id,
                                      reason=result["reason"], timeout_s=5.0)
    check("回落主库读到 70", fallback["product"]["quantity"] == 70)

    # 非备库目标（连到主库）也要拒绝读备库
    token2, receipt2, _ = correct(71)
    on_primary = inventory.read_following_receipt(PRIMARY, receipt2, 1.0)
    check("连接目标是主库时拒绝当备库读", on_primary["reason"] == "not_standby",
          str(on_primary.get("reason")))


def main():
    inventory.ensure_schema(PRIMARY)
    test_receipt_and_primary()
    test_lag_and_catchup()
    test_deadline_budget()
    test_disconnect()
    print()
    if FAILURES:
        print(f"共 {len(FAILURES)} 项失败：")
        for name in FAILURES:
            print("  -", name)
        return 1
    print("库存自测全部通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
