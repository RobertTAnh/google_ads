"""Gửi cảnh báo ngân sách Google Ads qua Slack Incoming Webhook (push trên điện thoại)."""

from __future__ import annotations

from typing import Optional

import httpx


def _format_cid_display(customer_id: str) -> str:
    digits = (customer_id or "").strip().replace("-", "")
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    return customer_id


def _micros_to_money(micros: Optional[int], currency: str = "USD") -> str:
    if micros is None:
        return "—"
    amount = micros / 1_000_000.0
    if currency.upper() in ("VND",):
        return f"{amount:,.0f} {currency}"
    return f"{amount:,.2f} {currency}"


def send_budget_alert(
    webhook_url: str,
    *,
    cid: str,
    name: str,
    total_daily_micros: int,
    remaining_micros: Optional[int],
    days_est: Optional[float],
    mcc_id: str,
    mcc_label: str = "",
    currency_code: str = "USD",
    timeout_seconds: float = 15.0,
) -> None:
    """POST message tới Slack Incoming Webhook."""
    url = (webhook_url or "").strip()
    if not url:
        raise ValueError("SLACK_WEBHOOK_URL chưa cấu hình.")

    cid_fmt = _format_cid_display(cid)
    account_line = f"{name} ({cid_fmt})" if name else cid_fmt
    days_line = f"~{days_est:.1f} ngày" if days_est is not None else "—"
    mcc_line = mcc_label or mcc_id
    if mcc_id and mcc_label and mcc_id not in mcc_label:
        mcc_line = f"{mcc_label} ({mcc_id})"
    elif mcc_id and not mcc_label:
        mcc_line = _format_cid_display(mcc_id)

    text = (
        "⚠️ *Ngân sách Google Ads còn dưới 4 ngày*\n"
        f"• Tài khoản: {account_line}\n"
        f"• Tổng NS ngày (ENABLED): {_micros_to_money(total_daily_micros, currency_code)}\n"
        f"• NS tài khoản còn lại: {_micros_to_money(remaining_micros, currency_code)}\n"
        f"• Ước tính: {days_line}\n"
        f"• MCC: {mcc_line}"
    )

    payload = {"text": text, "mrkdwn": True}
    with httpx.Client(timeout=timeout_seconds) as client:
        resp = client.post(url, json=payload)
        resp.raise_for_status()


def send_slack_test_message(webhook_url: str, *, timeout_seconds: float = 15.0) -> None:
    """Gửi tin test để xác nhận webhook + push Slack trên điện thoại."""
    url = (webhook_url or "").strip()
    if not url:
        raise ValueError("SLACK_WEBHOOK_URL chưa cấu hình.")
    text = (
        "✅ *Test cảnh báo Google Ads*\n"
        "Đây là tin nhắn thử từ dashboard. Nếu bạn thấy tin này trên Slack / điện thoại, "
        "webhook đã cấu hình đúng."
    )
    payload = {"text": text, "mrkdwn": True}
    with httpx.Client(timeout=timeout_seconds) as client:
        resp = client.post(url, json=payload)
        resp.raise_for_status()
