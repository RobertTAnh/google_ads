from __future__ import annotations

import base64
import os
import re
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

from dataclasses import asdict

from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash

from google_ads_helper import (
    GoogleAdsHelperError,
    create_performance_max_campaign_for_local_leads,
    format_vnd_thousands,
    get_yesterday_campaign_performance,
    get_customer_name,
    load_google_ads_client,
    list_child_accounts_under_mcc,
    optimize_budgets_by_cpa,
)

_ACCOUNT_CACHE: dict = {"ts": 0.0, "mcc_id": "", "mcc_name": "", "children": []}


def _env_list(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]

def _normalize_customer_id(raw: str) -> str:
    """
    Google Ads customer IDs are 10 digits. Accepts "240-746-9372", "2407469372",
    or datalist values like "Company … MST … (3787956462)" — must not merge every
    digit in the label (e.g. tax ID + level + real ID) into one invalid ID.
    """
    s = (raw or "").strip()
    if not s:
        return ""
    # Prefer the last parenthetical segment that contains exactly 10 digits (handles " (240-746-9372)").
    last_paren_id = ""
    for m in re.finditer(r"\(([^)]*)\)", s):
        inner = "".join(ch for ch in m.group(1) if ch.isdigit())
        if len(inner) == 10:
            last_paren_id = inner
    if last_paren_id:
        return last_paren_id
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) == 10:
        return digits
    if len(digits) > 10:
        return digits[-10:]
    return digits

def _format_customer_id_display(customer_id: str) -> str:
    digits = _normalize_customer_id(customer_id)
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    return customer_id

def _read_login_customer_id_from_yaml(yaml_path: str) -> str:
    """
    Lightweight parser to read `login_customer_id` from google-ads.yaml.
    Avoids requiring extra YAML dependencies.
    """
    try:
        with open(yaml_path, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if stripped.startswith("login_customer_id"):
                    _, value = stripped.split(":", 1)
                    return _normalize_customer_id(value.strip().strip("'\""))
    except OSError:
        return ""
    return ""

def _maybe_bootstrap_google_ads_yaml(project_root: Path) -> str:
    """
    Railway-friendly bootstrap:
    - ưu tiên file local `google-ads.yaml` nếu đã tồn tại
    - hoặc tạo file từ env `GOOGLE_ADS_YAML_B64` / `GOOGLE_ADS_YAML_TEXT`
    """
    yaml_path = project_root / "google-ads.yaml"
    if yaml_path.exists():
        return str(yaml_path)

    raw_b64 = (os.getenv("GOOGLE_ADS_YAML_B64") or "").strip()
    raw_text = os.getenv("GOOGLE_ADS_YAML_TEXT")
    content = ""
    if raw_b64:
        try:
            content = base64.b64decode(raw_b64).decode("utf-8")
        except Exception as ex:
            raise RuntimeError(f"Invalid GOOGLE_ADS_YAML_B64: {ex}") from ex
    elif raw_text:
        content = raw_text

    if content:
        yaml_path.write_text(content, encoding="utf-8")
    return str(yaml_path)


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
    app.jinja_env.filters["vnd"] = format_vnd_thousands
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = os.getenv("SESSION_COOKIE_SECURE", "1").strip().lower() in (
        "1",
        "true",
        "yes",
    )

    project_root = Path(__file__).resolve().parent
    google_ads_yaml = _maybe_bootstrap_google_ads_yaml(project_root)

    # Your MCC ID should live in google-ads.yaml as `login_customer_id`.
    # If you want to hard-enforce it from environment, set GOOGLE_ADS_LOGIN_CUSTOMER_ID.
    mcc_login_customer_id = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or None
    configured_mcc_id = _normalize_customer_id(
        mcc_login_customer_id or _read_login_customer_id_from_yaml(google_ads_yaml)
    )

    # Predefined client customer IDs (comma-separated) for the dashboard.
    # Example: CLIENT_CUSTOMER_IDS=1234567890,0987654321
    dashboard_customer_ids = _env_list("CLIENT_CUSTOMER_IDS", "")
    admin_username = (os.getenv("ADMIN_USERNAME") or "admin").strip()
    admin_password_hash = (os.getenv("ADMIN_PASSWORD_HASH") or "").strip()
    admin_password_plain = os.getenv("ADMIN_PASSWORD")

    @app.context_processor
    def inject_auth_context():
        return {"is_logged_in": bool(session.get("is_authenticated")), "auth_user": session.get("auth_user", "")}

    @app.before_request
    def require_login():
        public_endpoints = {"login", "healthz", "static"}
        if request.endpoint in public_endpoints:
            return None
        if session.get("is_authenticated"):
            return None
        return redirect(url_for("login", next=request.url))

    @app.context_processor
    def inject_mcc_context():
        """
        Provides MCC + child accounts for the UI "account switcher".
        Cached briefly to avoid calling the API on every request.
        """
        import time

        if not session.get("is_authenticated"):
            return {
                "mcc_name": "",
                "mcc_id": "",
                "child_accounts": [],
                "mcc_context_error": "",
            }
        try:
            client = load_google_ads_client(
                google_ads_yaml, default_login_customer_id=mcc_login_customer_id
            )
            mcc_id = configured_mcc_id
            if not mcc_id:
                return {
                    "mcc_name": "",
                    "mcc_id": "",
                    "child_accounts": [],
                    "mcc_context_error": "Thiếu login_customer_id trong cấu hình.",
                }

            now = time.time()
            if (
                _ACCOUNT_CACHE["mcc_id"] == mcc_id
                and (now - float(_ACCOUNT_CACHE["ts"])) < 60
                and _ACCOUNT_CACHE["children"]
            ):
                return {
                    "mcc_name": _ACCOUNT_CACHE["mcc_name"],
                    "mcc_id": _format_customer_id_display(_ACCOUNT_CACHE["mcc_id"]),
                    "child_accounts": _ACCOUNT_CACHE["children"],
                    "mcc_context_error": "",
                }

            mcc_name = get_customer_name(client, mcc_id)
            children = list_child_accounts_under_mcc(client, mcc_id)
            _ACCOUNT_CACHE.update(
                {"ts": now, "mcc_id": mcc_id, "mcc_name": mcc_name, "children": children}
            )
            return {
                "mcc_name": mcc_name,
                "mcc_id": _format_customer_id_display(mcc_id),
                "child_accounts": children,
                "mcc_context_error": "",
            }
        except Exception as ex:
            # Keep MCC visible even if account-list lookup fails.
            return {
                "mcc_name": "",
                "mcc_id": _format_customer_id_display(configured_mcc_id),
                "child_accounts": [],
                "mcc_context_error": str(ex),
            }

    @app.get("/")
    def index():
        return redirect(url_for("dashboard"))

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if session.get("is_authenticated"):
            return redirect(url_for("dashboard"))

        next_url = request.args.get("next") or url_for("dashboard")
        if request.method == "POST":
            username = (request.form.get("username") or "").strip()
            password = request.form.get("password") or ""

            username_ok = username == admin_username
            password_ok = False
            if admin_password_hash:
                try:
                    password_ok = check_password_hash(admin_password_hash, password)
                except ValueError:
                    password_ok = False
            elif admin_password_plain is not None:
                password_ok = password == admin_password_plain

            if username_ok and password_ok:
                session.clear()
                session["is_authenticated"] = True
                session["auth_user"] = username
                flash("Đăng nhập thành công.", "success")
                return redirect(next_url)
            flash("Sai tài khoản hoặc mật khẩu.", "danger")

        return render_template("login.html", next_url=next_url)

    @app.post("/logout")
    def logout():
        session.clear()
        flash("Đã đăng xuất.", "info")
        return redirect(url_for("login"))

    @app.get("/healthz")
    def healthz():
        return jsonify({"ok": True}), 200

    @app.get("/api/mcc-accounts")
    def api_mcc_accounts():
        """
        JSON test endpoint: child accounts under the configured MCC (GAQL customer_client).
        """
        if not configured_mcc_id:
            return jsonify({"ok": False, "error": "Thiếu login_customer_id trong cấu hình."}), 400
        try:
            client = load_google_ads_client(
                google_ads_yaml, default_login_customer_id=mcc_login_customer_id
            )
            children = list_child_accounts_under_mcc(client, configured_mcc_id)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": configured_mcc_id,
                    "count": len(children),
                    "accounts": [asdict(a) for a in children],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @app.get("/api/yesterday-report")
    def api_yesterday_report():
        """
        JSON cho nút « Lấy báo cáo » (một tài khoản đã chọn).
        Dữ liệu: ngày hôm qua theo GAQL YESTERDAY (múi giờ tài khoản).
        """
        cid = _normalize_customer_id(request.args.get("customer_id", ""))
        if not cid:
            return jsonify({"ok": False, "error": "Thiếu customer_id."}), 400
        try:
            client = load_google_ads_client(
                google_ads_yaml, default_login_customer_id=mcc_login_customer_id
            )
            rows = get_yesterday_campaign_performance(client, [cid])
            rdate = (date.today() - timedelta(days=1)).isoformat()
            return jsonify(
                {
                    "ok": True,
                    "report_date": rdate,
                    "rows": [asdict(r) for r in rows],
                }
            )
        except GoogleAdsHelperError as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    @app.get("/dashboard")
    def dashboard():
        # Chọn tài khoản (URL ?customer_id=... hoặc CLIENT_CUSTOMER_IDS), sau đó bấm « Lấy báo cáo »
        # hoặc mở ?report=1 để tải báo cáo (SSR).
        requested_customer_id_raw = (request.args.get("customer_id") or "").strip()
        requested_customer_id = _normalize_customer_id(requested_customer_id_raw)
        want_report = request.args.get("report", "").strip().lower() in ("1", "true", "yes")

        if requested_customer_id:
            customer_ids = [requested_customer_id]
        else:
            customer_ids = dashboard_customer_ids

        if not customer_ids:
            flash(
                "Nhập Customer ID (tk con) phía trên hoặc set CLIENT_CUSTOMER_IDS để dùng dashboard.",
                "warning",
            )
            return render_template(
                "dashboard.html",
                rows=[],
                error=None,
                current_customer_id="",
                want_report=False,
                normalized_customer_id="",
                report_url="",
                report_date=None,
                can_fetch_report_js=False,
                show_report_cta=False,
            )

        report_q: dict = {"report": "1"}
        if requested_customer_id_raw:
            report_q["customer_id"] = requested_customer_id_raw
        report_url = url_for("dashboard", **report_q)

        rows = []
        error = None
        report_date = None
        if want_report:
            try:
                client = load_google_ads_client(
                    google_ads_yaml, default_login_customer_id=mcc_login_customer_id
                )
                rows = get_yesterday_campaign_performance(client, customer_ids)
                report_date = (date.today() - timedelta(days=1)).isoformat()
            except GoogleAdsHelperError as e:
                error = str(e)

        show_report_cta = (not want_report) or bool(error)

        return render_template(
            "dashboard.html",
            rows=rows,
            error=error,
            current_customer_id=requested_customer_id_raw,
            want_report=want_report,
            normalized_customer_id=requested_customer_id,
            report_url=report_url,
            report_date=report_date,
            can_fetch_report_js=bool(requested_customer_id),
            show_report_cta=show_report_cta,
        )

    @app.get("/create-campaign")
    def create_campaign_form():
        return render_template("create_campaign.html")

    @app.post("/create-campaign")
    def create_campaign_submit():
        customer_id = (request.form.get("customer_id") or "").strip()
        campaign_name = (request.form.get("campaign_name") or "").strip()
        business_name = (request.form.get("business_name") or "").strip() or "Local Service Business"
        final_url = (request.form.get("final_url") or "").strip() or "https://example.com"

        daily_budget = float(request.form.get("daily_budget") or 0)
        target_cpa_raw = (request.form.get("target_cpa") or "").strip()
        target_cpa: Optional[float] = float(target_cpa_raw) if target_cpa_raw else None

        geo_ids_raw = (request.form.get("geo_target_constant_ids") or "").strip()
        geo_ids = (
            [int(x.strip()) for x in geo_ids_raw.split(",") if x.strip()]
            if geo_ids_raw
            else None
        )

        if not customer_id or not campaign_name:
            flash("Customer ID and Campaign Name are required.", "danger")
            return redirect(url_for("create_campaign_form"))

        try:
            client = load_google_ads_client(
                google_ads_yaml, default_login_customer_id=mcc_login_customer_id
            )
            result = create_performance_max_campaign_for_local_leads(
                client,
                customer_id,
                campaign_name=campaign_name,
                daily_budget=daily_budget,
                target_cpa=target_cpa,
                geo_target_constant_ids=geo_ids,
                final_url=final_url,
                business_name=business_name,
            )
            flash(
                f"Created PMax campaign. Campaign: {result['campaign_resource_name']}",
                "success",
            )
            return redirect(url_for("create_campaign_form"))
        except (ValueError, GoogleAdsHelperError) as e:
            flash(str(e), "danger")
            return redirect(url_for("create_campaign_form"))

    @app.get("/optimize-budgets")
    def optimize_budgets():
        # Which client account to optimize (defaults to first in CLIENT_CUSTOMER_IDS)
        customer_id = (request.args.get("customer_id") or "").strip() or (
            dashboard_customer_ids[0] if dashboard_customer_ids else ""
        )
        target_cpa = float(request.args.get("target_cpa") or os.getenv("TARGET_CPA", "0") or 0)

        if not customer_id:
            flash("Provide ?customer_id=... or set CLIENT_CUSTOMER_IDS.", "warning")
            return redirect(url_for("dashboard"))
        if target_cpa <= 0:
            flash(
                "Provide a target CPA via ?target_cpa=25.00 or set TARGET_CPA env var.",
                "warning",
            )
            return redirect(url_for("dashboard"))

        try:
            client = load_google_ads_client(
                google_ads_yaml, default_login_customer_id=mcc_login_customer_id
            )
            result = optimize_budgets_by_cpa(
                client, customer_id, target_cpa=target_cpa, date_range="LAST_30_DAYS", increase_pct=0.10
            )
            flash(
                f"Optimization complete. Updated: {len(result['updated'])}, Skipped: {len(result['skipped'])}.",
                "success",
            )
            return render_template(
                "dashboard.html",
                rows=[],
                error=None,
                optimization=result,
                optimized_customer_id=customer_id,
                target_cpa=target_cpa,
                current_customer_id=customer_id,
                want_report=False,
                normalized_customer_id=_normalize_customer_id(customer_id),
                report_url=url_for("dashboard", customer_id=customer_id, report=1),
                report_date=None,
                can_fetch_report_js=bool(_normalize_customer_id(customer_id)),
                show_report_cta=False,
            )
        except (ValueError, GoogleAdsHelperError) as e:
            flash(str(e), "danger")
            return redirect(url_for("dashboard"))

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=os.getenv("FLASK_DEBUG", "0").strip().lower() in ("1", "true", "yes"),
    )

