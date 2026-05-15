"""
MCP (stdio) — gọi HTTP API Flask `/mcp/v1/*` đã bật MCP_API_KEY.

Chạy từ thư mục gốc repo:
  python -m mcp_server

Biến môi trường:
  GOOGLE_ADS_MCP_BASE_URL  ví dụ https://your-app.up.railway.app  (không có / cuối)
  MCP_API_KEY              trùng với MCP_API_KEY trên server Flask
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional

import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    "Google Ads (HTTP bridge)",
    instructions=(
        "Tools gọi API Google Ads qua server đã deploy. "
        "Luôn có `customer_id` (CID tài khoản con 10 chữ số). "
        "Khi có nhiều MCC trong GOOGLE_ADS_MCC_CONFIGS, truyền `mcc_id` (MCC 10 chữ số)."
    ),
)


def _base_url() -> str:
    return (os.environ.get("GOOGLE_ADS_MCP_BASE_URL") or "").strip().rstrip("/")


def _api_key() -> str:
    return (os.environ.get("MCP_API_KEY") or "").strip()


def _get(path: str, params: Optional[dict[str, Any]] = None) -> str:
    base = _base_url()
    key = _api_key()
    if not base or not key:
        return json.dumps(
            {
                "ok": False,
                "error": "Thiếu GOOGLE_ADS_MCP_BASE_URL hoặc MCP_API_KEY trong env của MCP server (máy local).",
            },
            ensure_ascii=False,
        )
    params = {k: v for k, v in (params or {}).items() if v is not None and str(v).strip() != ""}
    url = f"{base}{path}"
    try:
        r = httpx.get(
            url,
            params=params,
            headers={"X-MCP-API-Key": key},
            timeout=httpx.Timeout(120.0, connect=30.0),
        )
        return r.text
    except httpx.HTTPError as e:
        return json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
def ads_mcp_health() -> str:
    """Kiểm tra server HTTP MCP (không cần API key trên một số bản triển khai). Trả JSON."""
    base = _base_url()
    if not base:
        return json.dumps({"ok": False, "error": "Chưa set GOOGLE_ADS_MCP_BASE_URL"}, ensure_ascii=False)
    try:
        r = httpx.get(f"{base}/mcp/v1/health", timeout=30.0)
        return r.text
    except httpx.HTTPError as e:
        return json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
def ads_list_child_accounts(mcc_id: str = "") -> str:
    """Liệt kê tài khoản con dưới MCC. `mcc_id` 10 chữ số; để trống nếu server chỉ có một MCC mặc định."""
    return _get("/mcp/v1/child_accounts", {"mcc_id": mcc_id or None})


@mcp.tool()
def ads_list_campaigns(customer_id: str, mcc_id: str = "") -> str:
    """Danh sách chiến dịch (id, tên, trạng thái, loại kênh) cho tài khoản con `customer_id`."""
    return _get("/mcp/v1/list_campaigns", {"customer_id": customer_id, "mcc_id": mcc_id or None})


@mcp.tool()
def ads_campaign_performance_yesterday(customer_id: str, mcc_id: str = "") -> str:
    """Hiệu suất theo chiến dịch — ngày hôm qua (theo múi giờ tài khoản Google Ads)."""
    return _get("/mcp/v1/campaign_performance", {"customer_id": customer_id, "mcc_id": mcc_id or None})


@mcp.tool()
def ads_customer_performance_yesterday(customer_id: str, mcc_id: str = "") -> str:
    """Hiệu suất cấp tài khoản — ngày hôm qua (tổng clicks, impressions, cost, conversions)."""
    return _get("/mcp/v1/customer_performance", {"customer_id": customer_id, "mcc_id": mcc_id or None})


@mcp.tool()
def ads_keyword_performance_yesterday(customer_id: str, mcc_id: str = "", limit: int = 500) -> str:
    """Hiệu suất theo từ khóa — ngày hôm qua (Search / keyword_view). `limit` tối đa mặc định 500."""
    return _get(
        "/mcp/v1/keyword_performance",
        {"customer_id": customer_id, "mcc_id": mcc_id or None, "limit": str(limit)},
    )


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
