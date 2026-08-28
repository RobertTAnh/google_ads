"""
HTTP API cho MCP / agent: JSON read-only, bảo vệ bằng MCP_API_KEY.
Prefix URL: /mcp/v1/...

Query kỳ: `date_range` (GAQL DURING) hoặc `start_date` + `end_date` (YYYY-MM-DD, GAQL BETWEEN).
"""

from __future__ import annotations

import os
from dataclasses import asdict
from typing import Any, Callable, Optional

from flask import Blueprint, jsonify, request

from cid_mcc_store import lookup_mcc_for_customer

from google_ads_helper import (
    ALLOWED_MCP_DATE_RANGES,
    DEFAULT_KEYWORD_PLAN_LANGUAGE_ID,
    DEFAULT_KEYWORD_PLAN_LOCATION_IDS,
    GoogleAdsHelperError,
    McpDateFilter,
    CHANGE_EVENT_GAQL_LIMIT,
    resolve_mcp_auction_insight_date_filter,
    resolve_mcp_date_filter,
    create_campaign_for_customer,
    generate_keyword_ideas,
    add_campaign_extensions,
    add_negative_keywords,
    add_search_ad_group_to_campaign,
    update_responsive_search_ad,
    update_ad_group,
    update_keyword_bids,
    get_ad_group_metrics_for_date_range,
    get_auction_insights_for_campaigns,
    get_ad_performance_for_date_range,
    get_asset_performance_for_date_range,
    get_audience_performance_for_date_range,
    get_campaign_budget_metrics_for_date_range,
    get_campaign_metrics_for_date_range,
    get_change_events_for_date_range,
    get_customer_metrics_for_date_range,
    get_keyword_metrics_for_date_range,
    get_keyword_quality_scores_for_date_range,
    get_pmax_search_term_insights_for_date_range,
    get_search_term_metrics_for_date_range,
    list_campaign_bidding_for_customers,
    list_campaigns_for_customers,
    list_child_accounts_under_mcc,
    list_keyword_status_for_customer,
    list_negative_keywords_for_customer,
)


def _mcp_api_key_expected() -> str:
    return (os.getenv("MCP_API_KEY") or "").strip()


def _mcp_extract_key() -> str:
    key = (request.headers.get("X-MCP-API-Key") or "").strip()
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        key = auth[7:].strip()
    return key


def _mcp_auth_error_response():
    expected = _mcp_api_key_expected()
    if not expected:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Server chưa cấu hình MCP_API_KEY. Thêm biến này trên Railway / .env rồi deploy lại.",
                }
            ),
            503,
        )
    if _mcp_extract_key() != expected:
        return jsonify({"ok": False, "error": "Unauthorized. Gửi header X-MCP-API-Key hoặc Authorization: Bearer."}), 401
    return None


def _parse_date_filter_arg() -> McpDateFilter:
    try:
        return resolve_mcp_date_filter(
            date_range=request.args.get("date_range"),
            start_date=request.args.get("start_date"),
            end_date=request.args.get("end_date"),
        )
    except GoogleAdsHelperError as e:
        raise ValueError(str(e)) from e


def _parse_auction_insight_date_filter_arg() -> McpDateFilter:
    try:
        return resolve_mcp_auction_insight_date_filter(
            date_range=request.args.get("date_range"),
            start_date=request.args.get("start_date"),
            end_date=request.args.get("end_date"),
        )
    except GoogleAdsHelperError as e:
        raise ValueError(str(e)) from e


def _date_filter_call_kwargs(df: McpDateFilter) -> dict[str, str]:
    if df.is_custom:
        return {"start_date": df.start_date or "", "end_date": df.end_date or ""}
    return {"date_range": df.label}


def _date_filter_json(df: McpDateFilter) -> dict[str, Any]:
    out: dict[str, Any] = {"date_range": df.label}
    if df.is_custom:
        out["start_date"] = df.start_date
        out["end_date"] = df.end_date
    return out


def register_mcp_routes(
    app: Any,
    *,
    build_google_ads_client_for_mcc: Callable[[str], Any],
    normalize_customer_id: Callable[[str], str],
    default_mcc_id: str,
    database_url: Optional[str] = None,
) -> None:
    bp = Blueprint("mcp", __name__, url_prefix="/mcp/v1")

    _MCC_ERR = (
        "Thiếu MCC: truyền ?mcc_id= hoặc lưu CID→MCC trong DB (bảng customer_mcc_map; "
        "trang web /cid-mcc-map khi đã cấu hình DATABASE_URL)."
    )

    def _resolve_mcc_pair(*, use_db_lookup: bool) -> tuple[str, str]:
        raw = (request.args.get("mcc_id") or "").strip()
        if raw:
            return normalize_customer_id(raw), "query_param"
        cid = normalize_customer_id(request.args.get("customer_id", "") or "")
        if use_db_lookup and database_url and cid:
            mcc = lookup_mcc_for_customer(database_url, cid)
            if mcc:
                return normalize_customer_id(mcc), "db_map"
        fb = normalize_customer_id(default_mcc_id or "")
        if fb:
            return fb, "default"
        return "", "missing"

    @bp.get("/health")
    def health():
        configured = bool(_mcp_api_key_expected())
        return jsonify(
            {
                "ok": True,
                "service": "google-ads-mcp-http",
                "mcp_data_routes_enabled": configured,
                "allowed_date_ranges": list(ALLOWED_MCP_DATE_RANGES),
                "custom_date_range": {
                    "start_date": "YYYY-MM-DD",
                    "end_date": "YYYY-MM-DD",
                    "note": "Truyền cả start_date và end_date; ưu tiên hơn date_range. Không giới hạn độ dài khoảng ngày phía app.",
                },
                "customer_mcc_map_enabled": bool(database_url),
                "hint": "Nếu có DATABASE_URL và đã lưu map CID→MCC, có thể bỏ qua mcc_id khi gọi các route có customer_id.",
            }
        )

    @bp.get("/resolve_mcc")
    def resolve_mcc():
        """Tra cứu MCC cho CID: ưu tiên ?mcc_id=, sau đó bảng map; không dùng MCC mặc định env (tránh nhầm)."""
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        raw = (request.args.get("mcc_id") or "").strip()
        if raw:
            return jsonify(
                {
                    "ok": True,
                    "customer_id": cid,
                    "mcc_customer_id": normalize_customer_id(raw),
                    "mcc_resolved_via": "query_param",
                }
            )
        if database_url:
            mcc = lookup_mcc_for_customer(database_url, cid)
            if mcc:
                return jsonify(
                    {
                        "ok": True,
                        "customer_id": cid,
                        "mcc_customer_id": normalize_customer_id(mcc),
                        "mcc_resolved_via": "db_map",
                    }
                )
        return (
            jsonify(
                {
                    "ok": False,
                    "customer_id": cid,
                    "error": "Chưa có map CID→MCC trong DB. Thêm tại /cid-mcc-map (web) hoặc truyền ?mcc_id=.",
                }
            ),
            404,
        )

    @bp.get("/child_accounts")
    def child_accounts():
        err = _mcp_auth_error_response()
        if err:
            return err
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=False)
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id (query) và không có MCC mặc định trong cấu hình."}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            children = list_child_accounts_under_mcc(client, mcc_id)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "count": len(children),
                    "accounts": [asdict(a) for a in children],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/list_campaigns")
    def list_campaigns():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id (tài khoản con, 10 chữ số)."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = list_campaigns_for_customers(client, [cid])
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "campaigns": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/campaign_bidding")
    def campaign_bidding():
        """Target CPA/ROAS đang cấu hình trên campaign (không phải CPA thực tế từ metrics)."""
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = list_campaign_bidding_for_customers(client, [cid])
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "note": (
                        "target_cpa / target_roas là mục tiêu bidding đã set trên chiến dịch (hoặc portfolio). "
                        "cpa trong campaign_performance / campaign_budget_metrics là cost/conversions thực tế trong kỳ."
                    ),
                    "campaigns": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/campaign_performance")
    def campaign_performance():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_campaign_metrics_for_date_range(client, [cid], **_date_filter_call_kwargs(df))
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "reference_calendar_note": "Metrics theo định nghĩa GAQL của Google Ads cho kỳ đã chọn.",
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/customer_performance")
    def customer_performance():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_customer_metrics_for_date_range(client, [cid], **_date_filter_call_kwargs(df))
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/keyword_performance")
    def keyword_performance():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_keyword_metrics_for_date_range(client, [cid], **_date_filter_call_kwargs(df))
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/search_term_performance")
    def search_term_performance():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_search_term_metrics_for_date_range(client, [cid], **_date_filter_call_kwargs(df))
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/pmax_search_term_insights")
    def pmax_search_term_insights():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        raw_cap = (request.args.get("campaign_id") or "").strip()
        campaign_id = None
        if raw_cap:
            digits = "".join(ch for ch in raw_cap if ch.isdigit())
            if not digits:
                return jsonify({"ok": False, "error": "campaign_id không hợp lệ."}), 400
            campaign_id = digits
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_pmax_search_term_insights_for_date_range(
                client,
                [cid],
                **_date_filter_call_kwargs(df),
                campaign_id=campaign_id,
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "campaign_id": campaign_id,
                    **_date_filter_json(df),
                    "channel": "PERFORMANCE_MAX",
                    "note": (
                        "Dữ liệu từ campaign_search_term_insight; nếu campaign-level không có term "
                        "thì fallback customer_search_term_insight (Google thường chỉ trả insight id=0 ở campaign). "
                        "cost/CPA không có từ resource này (Google không hỗ trợ cost_micros)."
                    ),
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/campaign_budget_metrics")
    def campaign_budget_metrics():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_campaign_budget_metrics_for_date_range(client, [cid], **_date_filter_call_kwargs(df))
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "note": "daily_budget = max(amount_micros) quan sát được trong stream kỳ (xấp xỉ budget ngày hiện tại).",
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/negative_keywords")
    def negative_keywords():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = list_negative_keywords_for_customer(client, [cid])
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "note": "Danh sách cấu hình hiện tại; query date_range (nếu có) không áp dụng cho negative keywords.",
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/keyword_status")
    def keyword_status():
        """Snapshot trạng thái keyword + CPC tối đa + ước tính first-page (không phụ thuộc date_range)."""
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400

        raw_ag = (request.args.get("ad_group_id") or "").strip()
        ad_group_id = None
        if raw_ag:
            digits = "".join(ch for ch in raw_ag if ch.isdigit())
            if not digits:
                return jsonify({"ok": False, "error": "ad_group_id không hợp lệ."}), 400
            ad_group_id = digits

        raw_cap = (request.args.get("campaign_id") or "").strip()
        campaign_id = None
        if raw_cap:
            digits = "".join(ch for ch in raw_cap if ch.isdigit())
            if not digits:
                return jsonify({"ok": False, "error": "campaign_id không hợp lệ."}), 400
            campaign_id = digits

        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = list_keyword_status_for_customer(
                client,
                [cid],
                ad_group_id=ad_group_id,
                campaign_id=campaign_id,
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "ad_group_id": ad_group_id,
                    "campaign_id": campaign_id,
                    "note": (
                        "Snapshot cấu hình hiện tại (ad_group_criterion). "
                        "primary_status / primary_status_reasons / first_page_cpc ≈ cột Trạng thái UI; "
                        "cpc_bid ≈ CPC tối đa. date_range không áp dụng."
                    ),
                    "count": len(rows),
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/ad_performance")
    def ad_performance():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_ad_performance_for_date_range(client, [cid], **_date_filter_call_kwargs(df))
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/ad_group_performance")
    def ad_group_performance():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_ad_group_metrics_for_date_range(client, [cid], **_date_filter_call_kwargs(df))
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/keyword_quality_score")
    def keyword_quality_score():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_keyword_quality_scores_for_date_range(client, [cid], **_date_filter_call_kwargs(df))
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "note": "Giá trị *_quality_score là bucket lịch sử (enum); lấy segments.date mới nhất trong kỳ cho mỗi keyword.",
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/audience_performance")
    def audience_performance():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_audience_performance_for_date_range(client, [cid], **_date_filter_call_kwargs(df))
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/asset_performance")
    def asset_performance():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_asset_performance_for_date_range(client, [cid], **_date_filter_call_kwargs(df))
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/change_history")
    def change_history():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_change_events_for_date_range(client, [cid], **_date_filter_call_kwargs(df))
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "change_event_gaql_limit": CHANGE_EVENT_GAQL_LIMIT,
                    "note": (
                        "Google chỉ lưu change_event ~30 ngày; GAQL bắt buộc LIMIT "
                        f"(tối đa {CHANGE_EVENT_GAQL_LIMIT} — trần nền tảng)."
                    ),
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/auction_insights")
    def auction_insights():
        err = _mcp_auth_error_response()
        if err:
            return err
        cid = normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=True)
        if not mcc_id:
            return jsonify({"ok": False, "error": _MCC_ERR}), 400
        raw_cap = (request.args.get("campaign_id") or "").strip()
        campaign_id = None
        if raw_cap:
            digits = "".join(ch for ch in raw_cap if ch.isdigit())
            if not digits:
                return jsonify({"ok": False, "error": "campaign_id không hợp lệ."}), 400
            campaign_id = digits
        try:
            df = _parse_auction_insight_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_auction_insights_for_campaigns(
                client,
                [cid],
                **_date_filter_call_kwargs(df),
                campaign_id=campaign_id,
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "campaign_id": campaign_id,
                    **_date_filter_json(df),
                    "level": "campaign",
                    "channel": "SEARCH",
                    "note": (
                        "Tỷ lệ metrics là thập phân 0–1 (nhân 100 để ra %). Chỉ Search. "
                        "display_domain rỗng có thể là hàng «Bạn». Dữ liệu lịch sử phụ thuộc Google."
                    ),
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    def _parse_keyword_ideas_params() -> dict[str, Any]:
        """Đọc params từ query (GET) hoặc JSON body (POST)."""
        body: dict[str, Any] = {}
        if request.method == "POST" and request.is_json:
            body = request.get_json(silent=True) or {}

        def _pick(name: str, default: Any = None) -> Any:
            if name in body and body[name] is not None:
                return body[name]
            return request.args.get(name, default)

        raw_kw = _pick("keywords", "")
        keywords: list[str] = []
        if isinstance(raw_kw, list):
            keywords = [str(x).strip() for x in raw_kw if str(x).strip()]
        elif isinstance(raw_kw, str) and raw_kw.strip():
            keywords = [p.strip() for p in raw_kw.split(",") if p.strip()]

        page_url = str(_pick("page_url", "") or "").strip()
        language_id = str(_pick("language_id", DEFAULT_KEYWORD_PLAN_LANGUAGE_ID) or DEFAULT_KEYWORD_PLAN_LANGUAGE_ID).strip()

        raw_locs = _pick("location_ids", "")
        location_ids: list[str] = []
        if isinstance(raw_locs, list):
            location_ids = [str(x).strip() for x in raw_locs if str(x).strip()]
        elif isinstance(raw_locs, str) and raw_locs.strip():
            location_ids = [p.strip() for p in raw_locs.split(",") if p.strip()]
        if not location_ids:
            location_ids = list(DEFAULT_KEYWORD_PLAN_LOCATION_IDS)

        network = str(_pick("keyword_plan_network", "GOOGLE_SEARCH_AND_PARTNERS") or "GOOGLE_SEARCH_AND_PARTNERS").strip()
        adult_raw = _pick("include_adult_keywords", False)
        if isinstance(adult_raw, bool):
            include_adult = adult_raw
        else:
            include_adult = str(adult_raw or "").strip().lower() in ("1", "true", "yes", "on")

        page_size_raw = _pick("page_size", "0")
        try:
            page_size = int(page_size_raw or 0)
        except (TypeError, ValueError):
            page_size = 0

        return {
            "keywords": keywords,
            "page_url": page_url,
            "language_id": language_id,
            "location_ids": location_ids,
            "keyword_plan_network": network,
            "include_adult_keywords": include_adult,
            "page_size": page_size,
        }

    def _generate_keyword_ideas_handler():
        err = _mcp_auth_error_response()
        if err:
            return err
        body = request.get_json(silent=True) if request.method == "POST" else None
        body = body if isinstance(body, dict) else {}
        cid = normalize_customer_id(
            str(request.args.get("customer_id", "") or body.get("customer_id", "") or "")
        )
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400

        raw_mcc = str(request.args.get("mcc_id", "") or body.get("mcc_id", "") or "").strip()
        if raw_mcc:
            mcc_id, mcc_resolved_via = normalize_customer_id(raw_mcc), "query_param"
        elif database_url:
            mapped = lookup_mcc_for_customer(database_url, cid)
            if mapped:
                mcc_id, mcc_resolved_via = normalize_customer_id(mapped), "db_map"
            else:
                mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=False)
                if not mcc_id:
                    return jsonify({"ok": False, "error": _MCC_ERR}), 400
        else:
            mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=False)
            if not mcc_id:
                return jsonify({"ok": False, "error": _MCC_ERR}), 400

        try:
            params = _parse_keyword_ideas_params()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        if not params["keywords"] and not params["page_url"]:
            return jsonify(
                {
                    "ok": False,
                    "error": "Cần keywords (seed, cách nhau bởi dấu phẩy) hoặc page_url.",
                }
            ), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = generate_keyword_ideas(client, cid, **params)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "seed_keywords": params["keywords"],
                    "page_url": params["page_url"] or None,
                    "language_id": params["language_id"],
                    "location_ids": params["location_ids"],
                    "keyword_plan_network": params["keyword_plan_network"],
                    "include_adult_keywords": params["include_adult_keywords"],
                    "note": (
                        "Tương đương Keyword Planner «Khám phá các từ khóa mới» "
                        "(KeywordPlanIdeaService.GenerateKeywordIdeas). "
                        "Bid đơn vị tiền tài khoản. Mặc định language=1040 (VI), location=2704 (VN)."
                    ),
                    "count": len(rows),
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.get("/generate_keyword_ideas")
    def generate_keyword_ideas_get():
        return _generate_keyword_ideas_handler()

    @bp.post("/generate_keyword_ideas")
    def generate_keyword_ideas_post():
        return _generate_keyword_ideas_handler()

    def _resolve_customer_mcc_from_request(body: dict[str, Any]) -> tuple[str, str, str] | tuple[None, None, Any]:
        cid = normalize_customer_id(
            str(request.args.get("customer_id", "") or body.get("customer_id", "") or "")
        )
        if not cid:
            return None, None, (jsonify({"ok": False, "error": "Thiếu customer_id."}), 400)

        raw_mcc = str(request.args.get("mcc_id", "") or body.get("mcc_id", "") or "").strip()
        if raw_mcc:
            mcc_id, mcc_resolved_via = normalize_customer_id(raw_mcc), "query_param"
        elif database_url:
            mapped = lookup_mcc_for_customer(database_url, cid)
            if mapped:
                mcc_id, mcc_resolved_via = normalize_customer_id(mapped), "db_map"
            else:
                mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=False)
                if not mcc_id:
                    return None, None, (jsonify({"ok": False, "error": _MCC_ERR}), 400)
        else:
            mcc_id, mcc_resolved_via = _resolve_mcc_pair(use_db_lookup=False)
            if not mcc_id:
                return None, None, (jsonify({"ok": False, "error": _MCC_ERR}), 400)
        return cid, mcc_id, mcc_resolved_via

    def _parse_string_list(raw: Any) -> list[str]:
        if isinstance(raw, list):
            return [str(x).strip() for x in raw if str(x).strip()]
        if isinstance(raw, str) and raw.strip():
            return [p.strip() for p in raw.split(",") if p.strip()]
        return []

    def _parse_keyword_specs(raw: Any) -> list[dict[str, str]]:
        if isinstance(raw, list):
            out: list[dict[str, str]] = []
            for item in raw:
                if isinstance(item, dict):
                    text = str(item.get("text", "") or "").strip()
                    if text:
                        out.append(
                            {
                                "text": text,
                                "match_type": str(item.get("match_type", "PHRASE") or "PHRASE"),
                            }
                        )
                elif str(item).strip():
                    out.append({"text": str(item).strip(), "match_type": "PHRASE"})
            return out
        if isinstance(raw, str) and raw.strip():
            return [{"text": p.strip(), "match_type": "PHRASE"} for p in raw.split(",") if p.strip()]
        return []

    def _parse_keyword_update_specs(raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return []
        out: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            spec: dict[str, Any] = {}
            crit = str(item.get("criterion_id", "") or "").strip().replace("-", "")
            if crit:
                spec["criterion_id"] = crit
            text = str(item.get("text", "") or item.get("keyword_text", "") or "").strip()
            if text:
                spec["text"] = text
                spec["match_type"] = str(item.get("match_type", "PHRASE") or "PHRASE")
            cpc_raw = item.get("cpc_bid", item.get("default_cpc"))
            if cpc_raw not in (None, "", 0, "0"):
                spec["cpc_bid"] = float(cpc_raw)
            st = item.get("status")
            if st:
                spec["status"] = str(st).strip().upper()
            if spec:
                out.append(spec)
        return out

    def _parse_sitelink_specs(raw: Any) -> list[dict[str, str]]:
        if not isinstance(raw, list):
            return []
        out: list[dict[str, str]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            link_text = str(item.get("link_text", "") or item.get("text", "") or "").strip()
            final_url = str(item.get("final_url", "") or item.get("url", "") or "").strip()
            if not link_text or not final_url:
                continue
            out.append(
                {
                    "link_text": link_text,
                    "final_url": final_url,
                    "description1": str(item.get("description1", "") or "").strip(),
                    "description2": str(item.get("description2", "") or "").strip(),
                }
            )
        return out

    def _parse_create_campaign_body(body: dict[str, Any]) -> dict[str, Any]:
        campaign_type = str(body.get("campaign_type", "") or "").strip().upper()
        campaign_name = str(body.get("campaign_name", "") or "").strip()
        if not campaign_type:
            raise ValueError("Thiếu campaign_type (SEARCH hoặc PERFORMANCE_MAX).")
        if not campaign_name:
            raise ValueError("Thiếu campaign_name.")

        try:
            daily_budget = float(body.get("daily_budget", 0) or 0)
        except (TypeError, ValueError) as e:
            raise ValueError("daily_budget không hợp lệ.") from e
        if daily_budget <= 0:
            raise ValueError("daily_budget phải > 0.")

        target_cpa_raw = body.get("target_cpa")
        target_cpa: float | None = None
        if target_cpa_raw not in (None, "", 0, "0"):
            target_cpa = float(target_cpa_raw)

        default_cpc_raw = body.get("default_cpc")
        default_cpc: float | None = None
        if default_cpc_raw not in (None, "", 0, "0"):
            default_cpc = float(default_cpc_raw)

        max_cpc_raw = body.get("max_cpc_ceiling", body.get("cpc_bid_ceiling"))
        max_cpc_ceiling: float | None = None
        if max_cpc_raw not in (None, "", 0, "0"):
            max_cpc_ceiling = float(max_cpc_raw)

        bidding_strategy = str(body.get("bidding_strategy", "") or "").strip() or None

        geo_ids: list[int] = []
        raw_geo = body.get("geo_target_constant_ids", body.get("location_ids"))
        if isinstance(raw_geo, list):
            for g in raw_geo:
                if str(g).strip().isdigit():
                    geo_ids.append(int(str(g).strip()))
        elif isinstance(raw_geo, str) and raw_geo.strip():
            for part in raw_geo.split(","):
                part = part.strip()
                if part.isdigit():
                    geo_ids.append(int(part))

        enable_raw = body.get("enable_campaign", False)
        if isinstance(enable_raw, bool):
            enable_campaign = enable_raw
        else:
            enable_campaign = str(enable_raw or "").strip().lower() in ("1", "true", "yes", "on")

        return {
            "campaign_type": campaign_type,
            "campaign_name": campaign_name,
            "daily_budget": daily_budget,
            "target_cpa": target_cpa,
            "default_cpc": default_cpc,
            "max_cpc_ceiling": max_cpc_ceiling,
            "bidding_strategy": bidding_strategy,
            "geo_target_constant_ids": geo_ids or None,
            "enable_campaign": enable_campaign,
            "final_url": str(body.get("final_url", "") or "").strip(),
            "ad_group_name": str(body.get("ad_group_name", "") or "").strip() or None,
            "headlines": _parse_string_list(body.get("headlines")),
            "long_headlines": _parse_string_list(body.get("long_headlines")),
            "descriptions": _parse_string_list(body.get("descriptions")),
            "keywords": _parse_keyword_specs(body.get("keywords")),
            "business_name": str(body.get("business_name", "") or "").strip() or "Local Service Business",
        }

    @bp.post("/create_campaign")
    def create_campaign():
        """
        Tạo campaign mới (mutate). Hỗ trợ SEARCH (đủ ad group + keywords + RSA) và PERFORMANCE_MAX.
        Mặc định PAUSED trừ khi enable_campaign=true.
        """
        err = _mcp_auth_error_response()
        if err:
            return err

        body = request.get_json(silent=True) if request.is_json else {}
        body = body if isinstance(body, dict) else {}

        resolved = _resolve_customer_mcc_from_request(body)
        if resolved[0] is None:
            return resolved[2]
        cid, mcc_id, mcc_resolved_via = resolved

        try:
            params = _parse_create_campaign_body(body)
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400

        ctype = params["campaign_type"]
        if ctype in ("SEARCH", "SEARCH_NETWORK"):
            if not params["final_url"]:
                return jsonify({"ok": False, "error": "SEARCH cần final_url."}), 400
            if len(params["headlines"]) < 3:
                return jsonify({"ok": False, "error": "SEARCH cần ít nhất 3 headlines."}), 400
            if len(params["descriptions"]) < 2:
                return jsonify({"ok": False, "error": "SEARCH cần ít nhất 2 descriptions."}), 400
            if not params["keywords"]:
                return jsonify({"ok": False, "error": "SEARCH cần keywords (mảng {text, match_type})."}), 400
            bs = (params.get("bidding_strategy") or "").upper()
            if bs == "MANUAL_CPC" and not params.get("default_cpc"):
                return jsonify({"ok": False, "error": "MANUAL_CPC cần default_cpc > 0."}), 400
            if bs == "TARGET_CPA" and not params.get("target_cpa"):
                return jsonify({"ok": False, "error": "TARGET_CPA cần target_cpa > 0."}), 400
        elif ctype in ("PERFORMANCE_MAX", "PMAX", "PERFORMANCEMAX"):
            if not params["final_url"]:
                return jsonify({"ok": False, "error": "PERFORMANCE_MAX cần final_url."}), 400
            if params["headlines"]:
                if len(params["headlines"]) < 3:
                    return jsonify({"ok": False, "error": "PMax cần ít nhất 3 headlines."}), 400
                if len(params["descriptions"]) < 2:
                    return jsonify({"ok": False, "error": "PMax cần ít nhất 2 descriptions khi truyền headlines."}), 400

        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            result = create_campaign_for_customer(client, cid, **params)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "note": (
                        "Campaign mới tạo ở trạng thái PAUSED (trừ khi enable_campaign=true với Search). "
                        "SEARCH: ad group + keywords + RSA. bidding_strategy: MANUAL_CPC | MAXIMIZE_CLICKS | "
                        "MAXIMIZE_CONVERSIONS | TARGET_CPA; max_cpc_ceiling = trần CPC (MAXIMIZE_CLICKS), "
                        "default_cpc chỉ cho MANUAL_CPC. PMax: text assets từ headlines/descriptions. "
                        "Số tiền theo đơn vị tiền tệ tài khoản Google Ads."
                    ),
                    "result": asdict(result),
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.post("/add_negative_keywords")
    def add_negative_keywords_route():
        """Thêm từ khóa phủ định lên campaign hoặc ad group có sẵn."""
        err = _mcp_auth_error_response()
        if err:
            return err

        body = request.get_json(silent=True) if request.is_json else {}
        body = body if isinstance(body, dict) else {}

        resolved = _resolve_customer_mcc_from_request(body)
        if resolved[0] is None:
            return resolved[2]
        cid, mcc_id, mcc_resolved_via = resolved

        level = str(body.get("level", "") or "").strip().lower()
        campaign_id = "".join(ch for ch in str(body.get("campaign_id", "") or "") if ch.isdigit())
        ad_group_id = "".join(ch for ch in str(body.get("ad_group_id", "") or "") if ch.isdigit())
        keywords = _parse_keyword_specs(body.get("keywords"))

        if level not in ("campaign", "ad_group"):
            return jsonify({"ok": False, "error": "level phải là campaign hoặc ad_group."}), 400
        if not campaign_id:
            return jsonify({"ok": False, "error": "Thiếu campaign_id."}), 400
        if level == "ad_group" and not ad_group_id:
            return jsonify({"ok": False, "error": "Thiếu ad_group_id khi level=ad_group."}), 400
        if not keywords:
            return jsonify({"ok": False, "error": "Cần keywords (mảng {text, match_type})."}), 400

        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            result = add_negative_keywords(
                client,
                cid,
                level=level,
                campaign_id=campaign_id,
                ad_group_id=ad_group_id or None,
                keywords=keywords,
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "result": asdict(result),
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.post("/add_campaign_extensions")
    def add_campaign_extensions_route():
        """Gắn Sitelink, Callout, Call extension lên campaign có sẵn."""
        err = _mcp_auth_error_response()
        if err:
            return err

        body = request.get_json(silent=True) if request.is_json else {}
        body = body if isinstance(body, dict) else {}

        resolved = _resolve_customer_mcc_from_request(body)
        if resolved[0] is None:
            return resolved[2]
        cid, mcc_id, mcc_resolved_via = resolved

        campaign_id = "".join(ch for ch in str(body.get("campaign_id", "") or "") if ch.isdigit())
        if not campaign_id:
            return jsonify({"ok": False, "error": "Thiếu campaign_id."}), 400

        sitelinks = _parse_sitelink_specs(body.get("sitelinks"))
        callouts = _parse_string_list(body.get("callouts"))

        phone_number = ""
        phone_country_code = "VN"
        call_raw = body.get("call") or body.get("phone")
        if isinstance(call_raw, dict):
            phone_number = str(call_raw.get("phone_number", "") or call_raw.get("number", "") or "").strip()
            phone_country_code = str(call_raw.get("country_code", "VN") or "VN").strip()
        elif isinstance(call_raw, str):
            phone_number = call_raw.strip()
        if not phone_number:
            phone_number = str(body.get("phone_number", "") or "").strip()
        if body.get("phone_country_code"):
            phone_country_code = str(body.get("phone_country_code") or "VN").strip()

        if not sitelinks and not callouts and not phone_number:
            return jsonify(
                {
                    "ok": False,
                    "error": "Cần ít nhất một trong: sitelinks, callouts, call/phone_number.",
                }
            ), 400

        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            result = add_campaign_extensions(
                client,
                cid,
                campaign_id,
                sitelinks=sitelinks,
                callouts=callouts,
                phone_number=phone_number,
                phone_country_code=phone_country_code,
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "campaign_id": campaign_id,
                    "note": "Đã tạo Asset và gắn vào campaign qua CampaignAsset.",
                    "result": asdict(result),
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.post("/add_ad_group")
    def add_ad_group_route():
        """Thêm ad group Search (+ keywords + RSA) vào campaign Search có sẵn."""
        err = _mcp_auth_error_response()
        if err:
            return err

        body = request.get_json(silent=True) if request.is_json else {}
        body = body if isinstance(body, dict) else {}

        resolved = _resolve_customer_mcc_from_request(body)
        if resolved[0] is None:
            return resolved[2]
        cid, mcc_id, mcc_resolved_via = resolved

        campaign_id = "".join(ch for ch in str(body.get("campaign_id", "") or "") if ch.isdigit())
        ad_group_name = str(body.get("ad_group_name", "") or "").strip()
        final_url = str(body.get("final_url", "") or "").strip()
        headlines = _parse_string_list(body.get("headlines"))
        descriptions = _parse_string_list(body.get("descriptions"))
        keywords = _parse_keyword_specs(body.get("keywords"))

        default_cpc: float | None = None
        raw_cpc = body.get("default_cpc")
        if raw_cpc not in (None, "", 0, "0"):
            default_cpc = float(raw_cpc)

        enable_raw = body.get("enable_ad_group", True)
        if isinstance(enable_raw, bool):
            enable_ad_group = enable_raw
        else:
            enable_ad_group = str(enable_raw or "").strip().lower() in ("1", "true", "yes", "on")

        if not campaign_id:
            return jsonify({"ok": False, "error": "Thiếu campaign_id."}), 400
        if not ad_group_name:
            return jsonify({"ok": False, "error": "Thiếu ad_group_name."}), 400
        if not final_url:
            return jsonify({"ok": False, "error": "Thiếu final_url."}), 400
        if len(headlines) < 3:
            return jsonify({"ok": False, "error": "Cần ít nhất 3 headlines."}), 400
        if len(descriptions) < 2:
            return jsonify({"ok": False, "error": "Cần ít nhất 2 descriptions."}), 400
        if not keywords:
            return jsonify({"ok": False, "error": "Cần keywords (mảng {text, match_type})."}), 400

        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            result = add_search_ad_group_to_campaign(
                client,
                cid,
                campaign_id,
                ad_group_name=ad_group_name,
                final_url=final_url,
                headlines=headlines,
                descriptions=descriptions,
                keywords=keywords,
                default_cpc=default_cpc,
                enable_ad_group=enable_ad_group,
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "campaign_id": campaign_id,
                    "note": (
                        "Chỉ campaign Search. default_cpc chỉ dùng khi campaign MANUAL_CPC; "
                        "MAXIMIZE_CLICKS/CONVERSIONS thì bỏ default_cpc."
                    ),
                    "result": asdict(result),
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.post("/update_responsive_search_ad")
    def update_responsive_search_ad_route():
        """Cập nhật RSA: headlines, descriptions, final_url, status."""
        err = _mcp_auth_error_response()
        if err:
            return err

        body = request.get_json(silent=True) if request.is_json else {}
        body = body if isinstance(body, dict) else {}

        resolved = _resolve_customer_mcc_from_request(body)
        if resolved[0] is None:
            return resolved[2]
        cid, mcc_id, mcc_resolved_via = resolved

        ad_group_id = "".join(ch for ch in str(body.get("ad_group_id", "") or "") if ch.isdigit())
        ad_id = "".join(ch for ch in str(body.get("ad_id", "") or "") if ch.isdigit())
        final_url = str(body.get("final_url", "") or "").strip()
        headlines = _parse_string_list(body.get("headlines"))
        descriptions = _parse_string_list(body.get("descriptions"))
        status = str(body.get("status", "") or "").strip().upper() or None

        if not ad_group_id or not ad_id:
            return jsonify({"ok": False, "error": "Thiếu ad_group_id hoặc ad_id."}), 400
        if not final_url and not headlines and not descriptions and not status:
            return jsonify(
                {
                    "ok": False,
                    "error": "Cần ít nhất một field: final_url, headlines, descriptions hoặc status.",
                }
            ), 400
        if headlines and len(headlines) < 3:
            return jsonify({"ok": False, "error": "headlines cần ít nhất 3 dòng khi cập nhật."}), 400
        if descriptions and len(descriptions) < 2:
            return jsonify({"ok": False, "error": "descriptions cần ít nhất 2 dòng khi cập nhật."}), 400

        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            result = update_responsive_search_ad(
                client,
                cid,
                ad_group_id,
                ad_id,
                final_url=final_url or None,
                headlines=headlines or None,
                descriptions=descriptions or None,
                status=status,
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "note": "ad_id lấy từ ads_get_ad_performance.",
                    "result": asdict(result),
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.post("/update_ad_group")
    def update_ad_group_route():
        """Cập nhật ad group Search: tên, status, default_cpc (MANUAL_CPC)."""
        err = _mcp_auth_error_response()
        if err:
            return err

        body = request.get_json(silent=True) if request.is_json else {}
        body = body if isinstance(body, dict) else {}

        resolved = _resolve_customer_mcc_from_request(body)
        if resolved[0] is None:
            return resolved[2]
        cid, mcc_id, mcc_resolved_via = resolved

        ad_group_id = "".join(ch for ch in str(body.get("ad_group_id", "") or "") if ch.isdigit())
        ad_group_name = str(body.get("ad_group_name", "") or "").strip() or None
        status = str(body.get("status", "") or "").strip().upper() or None

        default_cpc: float | None = None
        raw_cpc = body.get("default_cpc")
        if raw_cpc not in (None, "", 0, "0"):
            default_cpc = float(raw_cpc)

        if not ad_group_id:
            return jsonify({"ok": False, "error": "Thiếu ad_group_id."}), 400
        if not ad_group_name and not status and default_cpc is None:
            return jsonify(
                {
                    "ok": False,
                    "error": "Cần ít nhất một field: ad_group_name, status hoặc default_cpc.",
                }
            ), 400

        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            result = update_ad_group(
                client,
                cid,
                ad_group_id,
                ad_group_name=ad_group_name,
                status=status,
                default_cpc=default_cpc,
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "note": "default_cpc chỉ áp dụng khi campaign MANUAL_CPC.",
                    "result": asdict(result),
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @bp.post("/update_keyword_bids")
    def update_keyword_bids_route():
        """Cập nhật bid/status keyword trong ad group."""
        err = _mcp_auth_error_response()
        if err:
            return err

        body = request.get_json(silent=True) if request.is_json else {}
        body = body if isinstance(body, dict) else {}

        resolved = _resolve_customer_mcc_from_request(body)
        if resolved[0] is None:
            return resolved[2]
        cid, mcc_id, mcc_resolved_via = resolved

        ad_group_id = "".join(ch for ch in str(body.get("ad_group_id", "") or "") if ch.isdigit())
        keywords = _parse_keyword_update_specs(body.get("keywords"))

        if not ad_group_id:
            return jsonify({"ok": False, "error": "Thiếu ad_group_id."}), 400
        if not keywords:
            return jsonify(
                {
                    "ok": False,
                    "error": "Cần keywords: [{criterion_id?, text?, match_type?, cpc_bid?, status?}].",
                }
            ), 400

        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            result = update_keyword_bids(client, cid, ad_group_id, keywords)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    "note": "criterion_id từ ads_get_keyword_status; cpc_bid chỉ khi campaign MANUAL_CPC.",
                    "result": asdict(result),
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    app.register_blueprint(bp)
