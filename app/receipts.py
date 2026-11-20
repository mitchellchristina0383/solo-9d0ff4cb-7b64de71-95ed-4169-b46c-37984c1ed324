"""库存更正的“回执”：把主库提交位置（WAL flush LSN）签成一个可携带的令牌。

回执里带三样关键信息：
  - lsn：主库完成本次更正提交后所在的 WAL 位置（提交后取 pg_current_wal_insert_lsn()，
    该位置必在提交记录之后，备库回放到它即说明这笔提交已应用）；
  - system_identifier：主库实例标识，防止库重建后拿旧回执误判“已回放”；
  - product_id / quantity / committed_at：便于页面展示与核对。

令牌格式为 base64url(payload) "." base64url(HMAC-SHA256(payload))。
签名只用于防止客户端伪造 WAL 位置（避免把“读备库”骗成读任意旧位置），
不承担身份认证作用——这个沙箱本身不做鉴权。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
from dataclasses import asdict, dataclass

LSN_RE = re.compile(r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$")
MAX_TOKEN_LEN = 4096


class ReceiptError(ValueError):
    """回执格式错误、签名不合法或字段缺失。"""


@dataclass(frozen=True)
class Receipt:
    version: int
    correction_id: int
    product_id: int
    quantity: int
    lsn: str
    committed_at: str
    system_identifier: str

    def public(self, token: str) -> dict:
        """返回给页面的字段（不含签名逻辑）。"""
        return {
            "token": token,
            "id": receipt_id(token),
            "version": self.version,
            "correction_id": self.correction_id,
            "product_id": self.product_id,
            "quantity": self.quantity,
            "lsn": self.lsn,
            "committed_at": self.committed_at,
            "system_identifier": self.system_identifier,
        }


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def _sign(secret: str, body: bytes) -> bytes:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()


def receipt_id(token: str) -> str:
    """稳定、短的回执号，仅用于页面展示/日志，不参与校验。"""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]


def sign_receipt(secret: str, receipt: Receipt) -> str:
    if not secret:
        raise ReceiptError("回执签名密钥为空")
    body = json.dumps(asdict(receipt), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"{_b64encode(body)}.{_b64encode(_sign(secret, body))}"


def decode_receipt(secret: str, token: str) -> Receipt:
    if not isinstance(token, str) or not token.strip():
        raise ReceiptError("回执为空")
    token = token.strip()
    if len(token) > MAX_TOKEN_LEN:
        raise ReceiptError("回执过长")
    try:
        body_b64, sig_b64 = token.split(".")
    except ValueError as exc:
        raise ReceiptError("回执格式不正确（缺少签名段）") from exc
    try:
        body = _b64decode(body_b64)
        signature = _b64decode(sig_b64)
    except Exception as exc:
        raise ReceiptError("回执不是合法的 base64 编码") from exc
    if not hmac.compare_digest(signature, _sign(secret, body)):
        raise ReceiptError("回执签名校验失败（可能被篡改或不是本服务签发）")
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise ReceiptError("回执内容不是合法 JSON") from exc
    if not isinstance(payload, dict):
        raise ReceiptError("回执内容格式不正确")
    if payload.get("version") != 1:
        raise ReceiptError(f"不支持的回执版本：{payload.get('version')!r}")

    for field in ("correction_id", "product_id", "quantity"):
        value = payload.get(field)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ReceiptError(f"回执字段 {field} 不是整数")
    if payload["correction_id"] <= 0 or payload["product_id"] <= 0:
        raise ReceiptError("回执中的编号必须为正整数")
    if payload["quantity"] < 0:
        raise ReceiptError("回执中的余量为负数")

    lsn = payload.get("lsn")
    if not isinstance(lsn, str) or not LSN_RE.match(lsn):
        raise ReceiptError("回执中的 WAL 位置格式不正确")
    committed_at = payload.get("committed_at")
    if not isinstance(committed_at, str) or not committed_at:
        raise ReceiptError("回执缺少提交时间")
    system_identifier = payload.get("system_identifier")
    if not isinstance(system_identifier, str) or not system_identifier.isdigit():
        raise ReceiptError("回执缺少实例标识")

    return Receipt(
        version=1,
        correction_id=payload["correction_id"],
        product_id=payload["product_id"],
        quantity=payload["quantity"],
        lsn=lsn.upper(),
        committed_at=committed_at,
        system_identifier=system_identifier,
    )
