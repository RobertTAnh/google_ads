"""
MCP (stdio) — gọi HTTP API Flask `/mcp/v1/*` đã bật MCP_API_KEY.

Chạy từ thư mục gốc repo:
  python -m mcp_server

Biến môi trường:
  GOOGLE_ADS_MCP_BASE_URL  ví dụ https://your-app.up.railway.app  (không có / cuối)
  MCP_API_KEY              trùng với MCP_API_KEY trên server Flask

Kỳ báo cáo:
  date_range: YESTERDAY | LAST_7_DAYS | LAST_14_DAYS | LAST_30_DAYS
  hoặc start_date + end_date (YYYY-MM-DD) — ưu tiên hơn date_range
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
        "Tools gọi Google Ads qua server deploy. Bắt buộc customer_id (CID 10 số). "
        "Nếu server có DATABASE_URL và đã lưu map CID→MCC, có thể bỏ qua mcc_id — dùng ads_resolve_mcc(customer_id) khi cần kiểm tra. "
        "Kỳ: date_range (YESTERDAY, LAST_7_DAYS, LAST_14_DAYS, LAST_30_DAYS) "
        "hoặc start_date + end_date (YYYY-MM-DD, ví dụ từ 2026-05-05). "
        "Nhiều MCC: truyền mcc_id. CPA trong JSON metrics = cost/conversions. "
        "Target CPA đã set trên campaign: ads_campaign_bidding (không cần date_range). "
        "Auction Insights (Search): ads_get_auction_insights."
    ),
)

_HTTP_TIMEOUT = httpx.Timeout(180.0, connect=30.0)


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
            timeout=_HTTP_TIMEOUT,
        )
        return r.text
    except httpx.HTTPError as e:
        return json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)


def _period_params(
    date_range: str = "YESTERDAY",
    start_date: str = "",
    end_date: str = "",
) -> dict[str, str]:
    p: dict[str, str] = {}
    if start_date.strip() and end_date.strip():
        p["start_date"] = start_date.strip()
        p["end_date"] = end_date.strip()
    elif date_range.strip():
        p["date_range"] = date_range.strip()
    return p


def _customer_params(
    customer_id: str,
    mcc_id: str = "",
    *,
    date_range: str = "YESTERDAY",
    start_date: str = "",
    end_date: str = "",
) -> dict[str, Any]:
    p: dict[str, Any] = {"customer_id": customer_id}
    if mcc_id.strip():
        p["mcc_id"] = mcc_id.strip()
    p.update(_period_params(date_range, start_date, end_date))
    return p


@mcp.tool()
def ads_mcp_health() -> str:
    """Kiểm tra server HTTP MCP; JSON gồm allowed_date_ranges và custom_date_range."""
    base = _base_url()
    if not base:
        return json.dumps({"ok": False, "error": "Chưa set GOOGLE_ADS_MCP_BASE_URL"}, ensure_ascii=False)
    try:
        r = httpx.get(f"{base}/mcp/v1/health", timeout=30.0)
        return r.text
    except httpx.HTTPError as e:
        return json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
def ads_resolve_mcc(customer_id: str) -> str:
    """Tra MCC cho CID từ DB map (không dùng MCC mặc định). Gọi trước khi cần chắc chắn đúng MCC."""
    return _get("/mcp/v1/resolve_mcc", {"customer_id": customer_id})


@mcp.tool()
def ads_list_child_accounts(mcc_id: str = "") -> str:
    """Liệt kê tài khoản con dưới MCC."""
    return _get("/mcp/v1/child_accounts", {"mcc_id": mcc_id or None})


@mcp.tool()
def ads_list_campaigns(customer_id: str, mcc_id: str = "") -> str:
    """Danh sách chiến dịch (metadata: trạng thái, loại kênh), không theo kỳ ngày."""
    return _get("/mcp/v1/list_campaigns", {"customer_id": customer_id, "mcc_id": mcc_id or None})


@mcp.tool()
def ads_campaign_bidding(customer_id: str, mcc_id: str = "") -> str:
    """Target CPA / target ROAS đang cấu hình trên từng chiến dịch (không phải CPA thực tế cost/conv)."""
    return _get("/mcp/v1/campaign_bidding", {"customer_id": customer_id, "mcc_id": mcc_id or None})


@mcp.tool()
def ads_customer_performance(
    customer_id: str,
    mcc_id: str = "",
    date_range: str = "YESTERDAY",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Tổng metrics cấp tài khoản trong kỳ + CPA (cost/conversions). Dùng start_date+end_date (YYYY-MM-DD) cho khoảng tùy chỉnh."""
    return _get(
        "/mcp/v1/customer_performance",
        _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date),
    )


@mcp.tool()
def ads_campaign_performance(
    customer_id: str,
    mcc_id: str = "",
    date_range: str = "YESTERDAY",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Metrics gộp theo từng chiến dịch (ENABLED+PAUSED) trong kỳ + CPA."""
    return _get(
        "/mcp/v1/campaign_performance",
        _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date),
    )


@mcp.tool()
def ads_keyword_performance(
    customer_id: str,
    mcc_id: str = "",
    date_range: str = "YESTERDAY",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Keyword (keyword_view) trong kỳ, sắp xếp theo cost; Search-heavy."""
    return _get(
        "/mcp/v1/keyword_performance",
        _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date),
    )


@mcp.tool()
def ads_search_term_performance(
    customer_id: str,
    mcc_id: str = "",
    date_range: str = "LAST_7_DAYS",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Cụm từ tìm kiếm thực tế (search_term_view) trong kỳ; mặc định 7 ngày."""
    return _get(
        "/mcp/v1/search_term_performance",
        _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date),
    )


@mcp.tool()
def ads_campaign_budget_metrics(
    customer_id: str,
    mcc_id: str = "",
    date_range: str = "LAST_30_DAYS",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Mỗi campaign: ngân sách ngày (xấp xỉ) + cost/clicks/impressions/conversions/CPA trong kỳ."""
    return _get(
        "/mcp/v1/campaign_budget_metrics",
        _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date),
    )


@mcp.tool()
def ads_get_ad_performance(
    customer_id: str,
    mcc_id: str = "",
    date_range: str = "LAST_7_DAYS",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Theo từng quảng cáo (ad_group_ad): type, cost, clicks, conv, CPA trong kỳ."""
    return _get(
        "/mcp/v1/ad_performance",
        _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date),
    )


@mcp.tool()
def ads_get_negative_keywords(customer_id: str, mcc_id: str = "") -> str:
    """Từ khóa phủ định (campaign + ad group), cấu hình hiện tại; không phụ thuộc date_range."""
    return _get("/mcp/v1/negative_keywords", {"customer_id": customer_id, "mcc_id": mcc_id or None})


@mcp.tool()
def ads_get_ad_group_performance(
    customer_id: str,
    mcc_id: str = "",
    date_range: str = "LAST_7_DAYS",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Metrics gộp theo nhóm quảng cáo trong kỳ + CPA."""
    return _get(
        "/mcp/v1/ad_group_performance",
        _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date),
    )


@mcp.tool()
def ads_get_keyword_quality_score(
    customer_id: str,
    mcc_id: str = "",
    date_range: str = "LAST_30_DAYS",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Quality score lịch sử (bucket) theo keyword; bản ghi segments.date mới nhất trong kỳ."""
    return _get(
        "/mcp/v1/keyword_quality_score",
        _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date),
    )


@mcp.tool()
def ads_get_audience_performance(
    customer_id: str,
    mcc_id: str = "",
    date_range: str = "LAST_7_DAYS",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Đối tượng (ad_group_audience_view): display_name, type, metrics, CPA trong kỳ."""
    return _get(
        "/mcp/v1/audience_performance",
        _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date),
    )


@mcp.tool()
def ads_get_asset_performance(
    customer_id: str,
    mcc_id: str = "",
    date_range: str = "LAST_30_DAYS",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Asset trong asset group (PMax…): type, metrics, CPA trong kỳ."""
    return _get(
        "/mcp/v1/asset_performance",
        _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date),
    )


@mcp.tool()
def ads_get_auction_insights(
    customer_id: str,
    mcc_id: str = "",
    campaign_id: str = "",
    date_range: str = "LAST_7_DAYS",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Search Auction Insights (đối thủ): domain, impression share, overlap, outranking, top/abs top, position above. Chỉ Search."""
    p = _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date)
    if campaign_id.strip():
        p["campaign_id"] = campaign_id.strip()
    return _get("/mcp/v1/auction_insights", p)


@mcp.tool()
def ads_get_change_history(
    customer_id: str,
    mcc_id: str = "",
    date_range: str = "LAST_7_DAYS",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Lịch sử thay đổi (change_event): thời điểm, loại resource, user, field đổi."""
    return _get(
        "/mcp/v1/change_history",
        _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date),
    )


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
