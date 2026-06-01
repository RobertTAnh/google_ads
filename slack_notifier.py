"""Gửi cảnh báo ngân sách Google Ads qua Slack Incoming Webhook (push trên điện thoại)."""

from __future__ import annotations

import httpx


def _format_cid_display(customer_id: str) -> str:
    digits = (customer_id or "").strip().replace("-", "")
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    return customer_id


def resolve_account_display_name(
    *,
    label: str = "",
    customer_name: str = "",
    customer_id: str = "",
) -> str:
    """
    Tên hiển thị trong Slack: ưu tiên label (ghi chú khi thêm CID theo dõi),
    sau đó tên từ Google Ads API, cuối cùng CID định dạng.
    """
    lab = (label or "").strip()
    if lab:
        return lab
    api_name = (customer_name or "").strip()
    if api_name:
        return api_name
    cid_fmt = _format_cid_display(customer_id)
    return cid_fmt or "Tài khoản"


def format_budget_alert_text(account_name: str, *, is_test: bool = False) -> str:
    name = (account_name or "").strip() or "Tài khoản"
    text = f"Tài khoản {name} ngân sách còn 4 ngày - Vui lòng nạp tiền"
    if is_test:
        text = f"[TEST] {text}"
    return text


def _post_slack_text(webhook_url: str, text: str, *, timeout_seconds: float = 15.0) -> None:
    url = (webhook_url or "").strip()
    if not url:
        raise ValueError("SLACK_WEBHOOK_URL chưa cấu hình.")
    payload = {"text": text}
    with httpx.Client(timeout=timeout_seconds) as client:
        resp = client.post(url, json=payload)
        resp.raise_for_status()


def send_budget_alert(
    webhook_url: str,
    *,
    account_name: str,
    timeout_seconds: float = 15.0,
) -> None:
    """Gửi cảnh báo ngân sách thực tế."""
    text = format_budget_alert_text(account_name, is_test=False)
    _post_slack_text(webhook_url, text, timeout_seconds=timeout_seconds)


def send_slack_test_message(
    webhook_url: str,
    *,
    account_name: str,
    timeout_seconds: float = 15.0,
) -> None:
    """Gửi tin test cùng format cảnh báo thật."""
    text = format_budget_alert_text(account_name, is_test=True)
    _post_slack_text(webhook_url, text, timeout_seconds=timeout_seconds)
