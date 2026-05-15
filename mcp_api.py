"""
HTTP API cho MCP / agent: JSON read-only, bảo vệ bằng MCP_API_KEY.
Prefix URL: /mcp/v1/...
"""

from __future__ import annotations

import os
from dataclasses import asdict
from typing import Any, Callable

from flask import Blueprint, jsonify, request

from google_ads_helper import (
    GoogleAdsHelperError,
    get_yesterday_campaign_performance,
    get_yesterday_customer_performance,
    get_yesterday_keyword_performance,
    list_campaigns_for_customers,
    list_child_accounts_under_mcc,
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


def register_mcp_routes(
    app: Any,
    *,
    build_google_ads_client_for_mcc: Callable[[str], Any],
    normalize_customer_id: Callable[[str], str],
    local_yesterday_iso: Callable[[], str],
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
                "hint": "Các route khác cần header X-MCP-API-Key khi MCP_API_KEY đã được cấu hình.",
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
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_yesterday_campaign_performance(client, [cid])
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "report_date": local_yesterday_iso(),
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
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_yesterday_customer_performance(client, [cid])
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "report_date": local_yesterday_iso(),
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
        limit_raw = (request.args.get("limit") or "500").strip()
        try:
            limit = int(limit_raw)
        except ValueError:
            limit = 500
        try:
            client = build_google_ads_client_for_mcc(mcc_id)
            rows = get_yesterday_keyword_performance(client, [cid], limit_per_customer=limit)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": mcc_id,
                    "customer_id": cid,
                    "report_date": local_yesterday_iso(),
                    "limit": limit,
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    app.register_blueprint(bp)
