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
    GoogleAdsHelperError,
    McpDateFilter,
    mcp_custom_date_max_days,
    resolve_mcp_date_filter,
    get_ad_group_metrics_for_date_range,
    get_ad_performance_for_date_range,
    get_asset_performance_for_date_range,
    get_audience_performance_for_date_range,
    get_campaign_budget_metrics_for_date_range,
    get_campaign_metrics_for_date_range,
    get_change_events_for_date_range,
    get_customer_metrics_for_date_range,
    get_keyword_metrics_for_date_range,
    get_keyword_quality_scores_for_date_range,
    get_search_term_metrics_for_date_range,
    list_campaign_bidding_for_customers,
    list_campaigns_for_customers,
    list_child_accounts_under_mcc,
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


def _parse_limit(default: int, cap: int = 5000) -> int:
    raw = (request.args.get("limit") or str(default)).strip()
    try:
        n = int(raw)
    except ValueError:
        n = default
    return max(1, min(cap, n))


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
                    "max_span_days": mcp_custom_date_max_days(),
                    "note": "Truyền cả start_date và end_date; ưu tiên hơn date_range. Env MCP_CUSTOM_DATE_MAX_DAYS.",
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
        limit = _parse_limit(500)
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_keyword_metrics_for_date_range(
                client, [cid], **_date_filter_call_kwargs(df), limit_per_customer=limit
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "limit": limit,
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
        limit = _parse_limit(400, cap=5000)
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_search_term_metrics_for_date_range(
                client, [cid], **_date_filter_call_kwargs(df), limit_per_customer=limit
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "limit": limit,
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
        limit = _parse_limit(200, cap=2000)
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_ad_performance_for_date_range(
                client, [cid], **_date_filter_call_kwargs(df), limit_per_customer=limit
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "limit": limit,
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
        limit = _parse_limit(300, cap=2000)
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_audience_performance_for_date_range(
                client, [cid], **_date_filter_call_kwargs(df), limit_per_customer=limit
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "limit": limit,
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
        limit = _parse_limit(300, cap=2000)
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_asset_performance_for_date_range(
                client, [cid], **_date_filter_call_kwargs(df), limit_per_customer=limit
            )
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "limit": limit,
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
        limit = _parse_limit(500, cap=10000)
        try:
            df = _parse_date_filter_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_change_events_for_date_range(client, [cid], **_date_filter_call_kwargs(df), limit=limit)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "mcc_resolved_via": mcc_resolved_via,
                    "customer_id": cid,
                    **_date_filter_json(df),
                    "limit": limit,
                    "note": "Google Ads giới hạn change_event trong cửa sổ gần đây (~30 ngày).",
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    app.register_blueprint(bp)
