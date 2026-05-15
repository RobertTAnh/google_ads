"""
HTTP API cho MCP / agent: JSON read-only, bảo vệ bằng MCP_API_KEY.
Prefix URL: /mcp/v1/...

Query `date_range` (GAQL): YESTERDAY | LAST_7_DAYS | LAST_14_DAYS | LAST_30_DAYS
"""

from __future__ import annotations

import os
from dataclasses import asdict
from typing import Any, Callable

from flask import Blueprint, jsonify, request

from google_ads_helper import (
    ALLOWED_MCP_DATE_RANGES,
    GoogleAdsHelperError,
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
    list_campaigns_for_customers,
    list_child_accounts_under_mcc,
    list_negative_keywords_for_customer,
    normalize_mcp_date_range,
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


def _parse_date_range_arg() -> str:
    raw = (request.args.get("date_range") or "YESTERDAY").strip()
    try:
        return normalize_mcp_date_range(raw)
    except GoogleAdsHelperError as e:
        raise ValueError(str(e)) from e


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
) -> None:
    bp = Blueprint("mcp", __name__, url_prefix="/mcp/v1")

    def _resolve_mcc_id() -> str:
        raw = (request.args.get("mcc_id") or "").strip()
        if raw:
            return normalize_customer_id(raw)
        return normalize_customer_id(default_mcc_id or "")

    @bp.get("/health")
    def health():
        configured = bool(_mcp_api_key_expected())
        return jsonify(
            {
                "ok": True,
                "service": "google-ads-mcp-http",
                "mcp_data_routes_enabled": configured,
                "allowed_date_ranges": list(ALLOWED_MCP_DATE_RANGES),
                "hint": "Các route metrics nhận query date_range; mặc định YESTERDAY. Cần X-MCP-API-Key cho route dữ liệu.",
            }
        )

    @bp.get("/child_accounts")
    def child_accounts():
        err = _mcp_auth_error_response()
        if err:
            return err
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id (query) và không có MCC mặc định trong cấu hình."}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            children = list_child_accounts_under_mcc(client, mcc_id)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = list_campaigns_for_customers(client, [cid])
            return jsonify({"ok": True, "mcc_customer_id": mcc_id, "customer_id": cid, "campaigns": [asdict(r) for r in rows]})
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        try:
            dr = _parse_date_range_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_campaign_metrics_for_date_range(client, [cid], dr)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "date_range": dr,
                    "reference_calendar_note": "Metrics theo định nghĩa GAQL của Google Ads cho date_range.",
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        try:
            dr = _parse_date_range_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_customer_metrics_for_date_range(client, [cid], dr)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "date_range": dr,
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        limit = _parse_limit(500)
        try:
            dr = _parse_date_range_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_keyword_metrics_for_date_range(client, [cid], dr, limit_per_customer=limit)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "date_range": dr,
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        limit = _parse_limit(400, cap=5000)
        try:
            dr = _parse_date_range_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_search_term_metrics_for_date_range(client, [cid], dr, limit_per_customer=limit)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "date_range": dr,
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        try:
            dr = _parse_date_range_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_campaign_budget_metrics_for_date_range(client, [cid], dr)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "date_range": dr,
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = list_negative_keywords_for_customer(client, [cid])
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        limit = _parse_limit(200, cap=2000)
        try:
            dr = _parse_date_range_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_ad_performance_for_date_range(client, [cid], dr, limit_per_customer=limit)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "date_range": dr,
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        try:
            dr = _parse_date_range_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_ad_group_metrics_for_date_range(client, [cid], dr)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "date_range": dr,
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        try:
            dr = _parse_date_range_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_keyword_quality_scores_for_date_range(client, [cid], dr)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "date_range": dr,
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        limit = _parse_limit(300, cap=2000)
        try:
            dr = _parse_date_range_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_audience_performance_for_date_range(client, [cid], dr, limit_per_customer=limit)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "date_range": dr,
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        limit = _parse_limit(300, cap=2000)
        try:
            dr = _parse_date_range_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_asset_performance_for_date_range(client, [cid], dr, limit_per_customer=limit)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "date_range": dr,
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
        mcc_id = _resolve_mcc_id()
        if not mcc_id:
            return jsonify({"ok": False, "error": "Thiếu mcc_id."}), 400
        limit = _parse_limit(500, cap=10000)
        try:
            dr = _parse_date_range_arg()
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_change_events_for_date_range(client, [cid], dr, limit=limit)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "date_range": dr,
                    "limit": limit,
                    "note": "Google Ads giới hạn change_event trong cửa sổ gần đây (~30 ngày).",
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    app.register_blueprint(bp)
