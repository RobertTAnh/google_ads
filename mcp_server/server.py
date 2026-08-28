"""
MCP (stdio) — gọi HTTP API Flask `/mcp/v1/*` đã bật MCP_API_KEY.

Chạy từ thư mục gốc repo:
  python -m mcp_server

Biến môi trường:
  GOOGLE_ADS_MCP_BASE_URL  ví dụ https://your-app.up.railway.app  (không có / cuối)
  MCP_API_KEY              trùng với MCP_API_KEY trên server Flask

Kỳ báo cáo:
  date_range: TODAY | YESTERDAY | LAST_7_DAYS | LAST_14_DAYS | LAST_30_DAYS
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
        "Kỳ: date_range (TODAY, YESTERDAY, LAST_7_DAYS, LAST_14_DAYS, LAST_30_DAYS) "
        "hoặc start_date + end_date (YYYY-MM-DD, ví dụ từ 2026-05-05). "
        "Nhiều MCC: truyền mcc_id. CPA trong JSON metrics = cost/conversions. "
        "Target CPA đã set trên campaign: ads_campaign_bidding (không cần date_range). "
        "Auction Insights (Search): ads_get_auction_insights. "
        "Search terms Search: ads_search_term_performance; PMax: ads_pmax_search_term_insights. "
        "Tạo campaign mới (mutate): ads_create_campaign (SEARCH hoặc PERFORMANCE_MAX; mặc định PAUSED). "
        "Thêm negative keywords: ads_add_negative_keywords. "
        "Thêm extensions (sitelink/callout/call): ads_add_campaign_extensions. "
        "Thêm ad group Search vào campaign có sẵn: ads_add_ad_group. "
        "Cập nhật RSA: ads_update_responsive_search_ad. "
        "Cập nhật ad group: ads_update_ad_group. "
        "Cập nhật keyword bid/status: ads_update_keyword_bids."
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


def _post(path: str, body: dict[str, Any]) -> str:
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
    payload = {k: v for k, v in body.items() if v is not None}
    url = f"{base}{path}"
    try:
        r = httpx.post(
            url,
            json=payload,
            headers={"X-MCP-API-Key": key, "Content-Type": "application/json"},
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
    """Cụm từ tìm kiếm thực tế (search_term_view) trong kỳ — chỉ Search, không phải PMax."""
    return _get(
        "/mcp/v1/search_term_performance",
        _customer_params(customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date),
    )


@mcp.tool()
def ads_pmax_search_term_insights(
    customer_id: str,
    mcc_id: str = "",
    campaign_id: str = "",
    date_range: str = "LAST_7_DAYS",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Search term insights PMax (campaign_search_term_insight); truyền campaign_id PMax khi có thể."""
    params = _customer_params(
        customer_id, mcc_id, date_range=date_range, start_date=start_date, end_date=end_date
    )
    if campaign_id.strip():
        params["campaign_id"] = campaign_id.strip()
    return _get("/mcp/v1/pmax_search_term_insights", params)


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
def ads_get_keyword_status(
    customer_id: str,
    mcc_id: str = "",
    ad_group_id: str = "",
    campaign_id: str = "",
) -> str:
    """
    Snapshot trạng thái keyword + CPC tối đa + ước tính first-page / top-of-page.
    Tương đương cột Trạng thái (Đủ điều kiện / Có giới hạn / Dưới giá thầu trang đầu) và CPC tối đa trên UI.
    Không phụ thuộc date_range. Có thể lọc ad_group_id hoặc campaign_id.
    """
    p: dict[str, Any] = {"customer_id": customer_id, "mcc_id": mcc_id or None}
    if ad_group_id.strip():
        p["ad_group_id"] = ad_group_id.strip()
    if campaign_id.strip():
        p["campaign_id"] = campaign_id.strip()
    return _get("/mcp/v1/keyword_status", p)


@mcp.tool()
def ads_generate_keyword_ideas(
    customer_id: str,
    keywords: str,
    mcc_id: str = "",
    page_url: str = "",
    language_id: str = "1040",
    location_ids: str = "2704",
    keyword_plan_network: str = "GOOGLE_SEARCH_AND_PARTNERS",
    page_size: int = 0,
) -> str:
    """
    Keyword Planner — khám phá từ khóa mới (GenerateKeywordIdeas) từ seed.
    keywords: từ khóa gốc, cách nhau bởi dấu phẩy (vd: 'máy lạnh,điều hòa').
    page_url: tùy chọn URL seed. language_id mặc định 1040 (VI), location_ids mặc định 2704 (VN).
    page_size: giới hạn số ý tưởng (0 = lấy hết).
    """
    p: dict[str, Any] = {
        "customer_id": customer_id,
        "keywords": keywords,
        "language_id": language_id or "1040",
        "location_ids": location_ids or "2704",
        "keyword_plan_network": keyword_plan_network or "GOOGLE_SEARCH_AND_PARTNERS",
    }
    if mcc_id.strip():
        p["mcc_id"] = mcc_id.strip()
    if page_url.strip():
        p["page_url"] = page_url.strip()
    if page_size and int(page_size) > 0:
        p["page_size"] = int(page_size)
    return _get("/mcp/v1/generate_keyword_ideas", p)


@mcp.tool()
def ads_create_campaign(
    customer_id: str,
    campaign_type: str,
    campaign_name: str,
    daily_budget: float,
    mcc_id: str = "",
    final_url: str = "",
    ad_group_name: str = "",
    headlines: str = "",
    long_headlines: str = "",
    descriptions: str = "",
    keywords_json: str = "",
    default_cpc: float = 0,
    max_cpc_ceiling: float = 0,
    bidding_strategy: str = "",
    target_cpa: float = 0,
    geo_target_constant_ids: str = "2704",
    business_name: str = "",
    enable_campaign: bool = False,
    payload_json: str = "",
) -> str:
    """
    Tạo campaign mới qua Google Ads API (mutate). Mặc định PAUSED.

    campaign_type: SEARCH | PERFORMANCE_MAX
    daily_budget / default_cpc / max_cpc_ceiling / target_cpa: theo đơn vị tiền tệ tài khoản (vd VND).

    SEARCH bidding_strategy:
    - MAXIMIZE_CLICKS + max_cpc_ceiling (vd 25000) = Tối đa hóa lượt nhấp có trần CPC
    - MANUAL_CPC + default_cpc = CPC thủ công
    - TARGET_CPA + target_cpa; mặc định MAXIMIZE_CONVERSIONS nếu không chỉ định
    Chỉ truyền max_cpc_ceiling (không default_cpc) cũng được → tự chọn MAXIMIZE_CLICKS.

    SEARCH — bắt buộc: final_url, headlines (>=3, cách nhau dấu phẩy), descriptions (>=2),
    keywords_json: JSON array [{\"text\":\"từ khóa\",\"match_type\":\"PHRASE\"}] hoặc CSV text.

    PERFORMANCE_MAX — cần final_url; headlines (>=3, cách nhau dấu phẩy), descriptions (>=2);
    long_headlines tùy chọn (cách nhau dấu phẩy, tối đa 90 ký tự). business_name, geo mặc định VN.
    Vẫn cần upload ảnh/logo trên Google Ads UI trước khi chạy đầy đủ.

    Hoặc truyền payload_json (JSON đầy đủ) để ghi đè các field trên.
    """
    if payload_json.strip():
        try:
            body = json.loads(payload_json)
        except json.JSONDecodeError as e:
            return json.dumps({"ok": False, "error": f"payload_json không hợp lệ: {e}"}, ensure_ascii=False)
        if not isinstance(body, dict):
            return json.dumps({"ok": False, "error": "payload_json phải là object JSON."}, ensure_ascii=False)
    else:
        body: dict[str, Any] = {
            "customer_id": customer_id,
            "campaign_type": campaign_type,
            "campaign_name": campaign_name,
            "daily_budget": daily_budget,
            "enable_campaign": enable_campaign,
        }
        if mcc_id.strip():
            body["mcc_id"] = mcc_id.strip()
        if final_url.strip():
            body["final_url"] = final_url.strip()
        if ad_group_name.strip():
            body["ad_group_name"] = ad_group_name.strip()
        if headlines.strip():
            body["headlines"] = [p.strip() for p in headlines.split(",") if p.strip()]
        if long_headlines.strip():
            body["long_headlines"] = [p.strip() for p in long_headlines.split(",") if p.strip()]
        if descriptions.strip():
            body["descriptions"] = [p.strip() for p in descriptions.split(",") if p.strip()]
        if keywords_json.strip():
            try:
                kw = json.loads(keywords_json)
            except json.JSONDecodeError as e:
                return json.dumps({"ok": False, "error": f"keywords_json không hợp lệ: {e}"}, ensure_ascii=False)
            body["keywords"] = kw
        if default_cpc and float(default_cpc) > 0:
            body["default_cpc"] = float(default_cpc)
        if max_cpc_ceiling and float(max_cpc_ceiling) > 0:
            body["max_cpc_ceiling"] = float(max_cpc_ceiling)
        if bidding_strategy.strip():
            body["bidding_strategy"] = bidding_strategy.strip()
        if target_cpa and float(target_cpa) > 0:
            body["target_cpa"] = float(target_cpa)
        if geo_target_constant_ids.strip():
            body["geo_target_constant_ids"] = [
                p.strip() for p in geo_target_constant_ids.split(",") if p.strip()
            ]
        if business_name.strip():
            body["business_name"] = business_name.strip()

    if "customer_id" not in body:
        body["customer_id"] = customer_id

    if not body.get("keywords") and body.get("keywords_json"):
        raw_kw_json = body.get("keywords_json")
        if isinstance(raw_kw_json, str) and raw_kw_json.strip():
            try:
                body["keywords"] = json.loads(raw_kw_json)
            except json.JSONDecodeError as e:
                return json.dumps({"ok": False, "error": f"keywords_json không hợp lệ: {e}"}, ensure_ascii=False)
        elif isinstance(raw_kw_json, list):
            body["keywords"] = raw_kw_json
    body.pop("keywords_json", None)

    return _post("/mcp/v1/create_campaign", body)


@mcp.tool()
def ads_add_negative_keywords(
    customer_id: str,
    campaign_id: str,
    keywords_json: str,
    level: str = "campaign",
    ad_group_id: str = "",
    mcc_id: str = "",
) -> str:
    """
    Thêm từ khóa phủ định lên campaign hoặc ad group có sẵn.
    level: campaign | ad_group. keywords_json: [{\"text\":\"...\",\"match_type\":\"PHRASE\"}, ...]
    """
    try:
        keywords = json.loads(keywords_json)
    except json.JSONDecodeError as e:
        return json.dumps({"ok": False, "error": f"keywords_json không hợp lệ: {e}"}, ensure_ascii=False)
    body: dict[str, Any] = {
        "customer_id": customer_id,
        "campaign_id": campaign_id,
        "level": level,
        "keywords": keywords,
    }
    if mcc_id.strip():
        body["mcc_id"] = mcc_id.strip()
    if ad_group_id.strip():
        body["ad_group_id"] = ad_group_id.strip()
    return _post("/mcp/v1/add_negative_keywords", body)


@mcp.tool()
def ads_add_campaign_extensions(
    customer_id: str,
    campaign_id: str,
    mcc_id: str = "",
    sitelinks_json: str = "",
    callouts: str = "",
    phone_number: str = "",
    phone_country_code: str = "VN",
    payload_json: str = "",
) -> str:
    """
    Gắn extension lên campaign có sẵn: Sitelink, Callout, Call (số điện thoại).

    sitelinks_json: [{\"link_text\":\"...\",\"final_url\":\"https://...\",\"description1\":\"?\",\"description2\":\"?\"}]
    callouts: chú thích, cách nhau dấu phẩy.
    phone_number + phone_country_code (mặc định VN) cho Call extension.
    Hoặc payload_json (JSON đầy đủ).
    """
    if payload_json.strip():
        try:
            body = json.loads(payload_json)
        except json.JSONDecodeError as e:
            return json.dumps({"ok": False, "error": f"payload_json không hợp lệ: {e}"}, ensure_ascii=False)
        if not isinstance(body, dict):
            return json.dumps({"ok": False, "error": "payload_json phải là object."}, ensure_ascii=False)
    else:
        body = {
            "customer_id": customer_id,
            "campaign_id": campaign_id,
            "phone_country_code": phone_country_code or "VN",
        }
        if mcc_id.strip():
            body["mcc_id"] = mcc_id.strip()
        if sitelinks_json.strip():
            try:
                body["sitelinks"] = json.loads(sitelinks_json)
            except json.JSONDecodeError as e:
                return json.dumps({"ok": False, "error": f"sitelinks_json không hợp lệ: {e}"}, ensure_ascii=False)
        if callouts.strip():
            body["callouts"] = [p.strip() for p in callouts.split(",") if p.strip()]
        if phone_number.strip():
            body["phone_number"] = phone_number.strip()
    if "customer_id" not in body:
        body["customer_id"] = customer_id
    if "campaign_id" not in body:
        body["campaign_id"] = campaign_id
    return _post("/mcp/v1/add_campaign_extensions", body)


@mcp.tool()
def ads_add_ad_group(
    customer_id: str,
    campaign_id: str,
    ad_group_name: str,
    final_url: str,
    headlines: str,
    descriptions: str,
    keywords_json: str,
    mcc_id: str = "",
    default_cpc: float = 0,
    enable_ad_group: bool = True,
    payload_json: str = "",
) -> str:
    """
    Thêm ad group Search (+ keywords + RSA) vào campaign Search đã có.

    headlines / descriptions: cách nhau dấu phẩy (>=3 / >=2).
    keywords_json: [{\"text\":\"...\",\"match_type\":\"PHRASE\"}, ...]
    default_cpc: chỉ khi campaign MANUAL_CPC; campaign MAXIMIZE_CLICKS thì bỏ qua.
    enable_ad_group: true = ENABLED, false = PAUSED.
    """
    if payload_json.strip():
        try:
            body = json.loads(payload_json)
        except json.JSONDecodeError as e:
            return json.dumps({"ok": False, "error": f"payload_json không hợp lệ: {e}"}, ensure_ascii=False)
        if not isinstance(body, dict):
            return json.dumps({"ok": False, "error": "payload_json phải là object."}, ensure_ascii=False)
    else:
        try:
            keywords = json.loads(keywords_json)
        except json.JSONDecodeError as e:
            return json.dumps({"ok": False, "error": f"keywords_json không hợp lệ: {e}"}, ensure_ascii=False)
        body = {
            "customer_id": customer_id,
            "campaign_id": campaign_id,
            "ad_group_name": ad_group_name,
            "final_url": final_url,
            "headlines": [p.strip() for p in headlines.split(",") if p.strip()],
            "descriptions": [p.strip() for p in descriptions.split(",") if p.strip()],
            "keywords": keywords,
            "enable_ad_group": enable_ad_group,
        }
        if mcc_id.strip():
            body["mcc_id"] = mcc_id.strip()
        if default_cpc and float(default_cpc) > 0:
            body["default_cpc"] = float(default_cpc)
    if "customer_id" not in body:
        body["customer_id"] = customer_id
    return _post("/mcp/v1/add_ad_group", body)


@mcp.tool()
def ads_update_responsive_search_ad(
    customer_id: str,
    ad_group_id: str,
    ad_id: str,
    mcc_id: str = "",
    final_url: str = "",
    headlines: str = "",
    descriptions: str = "",
    status: str = "",
    payload_json: str = "",
) -> str:
    """
    Cập nhật Responsive Search Ad có sẵn (headlines, descriptions, final_url, status).
    ad_id lấy từ ads_get_ad_performance. headlines/descriptions: cách nhau dấu phẩy (>=3 / >=2).
    """
    if payload_json.strip():
        try:
            body = json.loads(payload_json)
        except json.JSONDecodeError as e:
            return json.dumps({"ok": False, "error": f"payload_json không hợp lệ: {e}"}, ensure_ascii=False)
        if not isinstance(body, dict):
            return json.dumps({"ok": False, "error": "payload_json phải là object."}, ensure_ascii=False)
    else:
        body: dict[str, Any] = {
            "customer_id": customer_id,
            "ad_group_id": ad_group_id,
            "ad_id": ad_id,
        }
        if mcc_id.strip():
            body["mcc_id"] = mcc_id.strip()
        if final_url.strip():
            body["final_url"] = final_url.strip()
        if headlines.strip():
            body["headlines"] = [p.strip() for p in headlines.split(",") if p.strip()]
        if descriptions.strip():
            body["descriptions"] = [p.strip() for p in descriptions.split(",") if p.strip()]
        if status.strip():
            body["status"] = status.strip().upper()
    if "customer_id" not in body:
        body["customer_id"] = customer_id
    return _post("/mcp/v1/update_responsive_search_ad", body)


@mcp.tool()
def ads_update_ad_group(
    customer_id: str,
    ad_group_id: str,
    mcc_id: str = "",
    ad_group_name: str = "",
    status: str = "",
    default_cpc: float = 0,
    payload_json: str = "",
) -> str:
    """
    Cập nhật ad group Search: tên, status (ENABLED/PAUSED), default_cpc (chỉ MANUAL_CPC).
    """
    if payload_json.strip():
        try:
            body = json.loads(payload_json)
        except json.JSONDecodeError as e:
            return json.dumps({"ok": False, "error": f"payload_json không hợp lệ: {e}"}, ensure_ascii=False)
        if not isinstance(body, dict):
            return json.dumps({"ok": False, "error": "payload_json phải là object."}, ensure_ascii=False)
    else:
        body = {
            "customer_id": customer_id,
            "ad_group_id": ad_group_id,
        }
        if mcc_id.strip():
            body["mcc_id"] = mcc_id.strip()
        if ad_group_name.strip():
            body["ad_group_name"] = ad_group_name.strip()
        if status.strip():
            body["status"] = status.strip().upper()
        if default_cpc and float(default_cpc) > 0:
            body["default_cpc"] = float(default_cpc)
    if "customer_id" not in body:
        body["customer_id"] = customer_id
    return _post("/mcp/v1/update_ad_group", body)


@mcp.tool()
def ads_update_keyword_bids(
    customer_id: str,
    ad_group_id: str,
    keywords_json: str,
    mcc_id: str = "",
    payload_json: str = "",
) -> str:
    """
    Cập nhật bid/status keyword trong ad group.
    keywords_json: [{\"criterion_id\":\"...\",\"cpc_bid\":15000,\"status\":\"ENABLED\"}, ...]
    Hoặc {\"text\":\"...\",\"match_type\":\"PHRASE\", ...}. criterion_id từ ads_get_keyword_status.
    """
    if payload_json.strip():
        try:
            body = json.loads(payload_json)
        except json.JSONDecodeError as e:
            return json.dumps({"ok": False, "error": f"payload_json không hợp lệ: {e}"}, ensure_ascii=False)
        if not isinstance(body, dict):
            return json.dumps({"ok": False, "error": "payload_json phải là object."}, ensure_ascii=False)
    else:
        try:
            keywords = json.loads(keywords_json)
        except json.JSONDecodeError as e:
            return json.dumps({"ok": False, "error": f"keywords_json không hợp lệ: {e}"}, ensure_ascii=False)
        body = {
            "customer_id": customer_id,
            "ad_group_id": ad_group_id,
            "keywords": keywords,
        }
        if mcc_id.strip():
            body["mcc_id"] = mcc_id.strip()
    if "customer_id" not in body:
        body["customer_id"] = customer_id
    return _post("/mcp/v1/update_keyword_bids", body)


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
