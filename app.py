from __future__ import annotations

import base64
import hashlib
import json
import os
import random
import re
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

from dataclasses import asdict

from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash
import psycopg
from psycopg.types.json import Jsonb

from google_ads_helper import (
    GoogleAdsHelperError,
    create_performance_max_campaign_for_local_leads,
    format_vnd_thousands,
    get_yesterday_campaign_performance,
    get_customer_name,
    load_google_ads_client,
    load_google_ads_client_from_dict,
    list_child_accounts_under_mcc,
    optimize_budgets_by_cpa,
)
from sheets_reporter import push_yesterday_report_to_sheet

_ACCOUNT_CACHE_BY_MCC: dict[str, dict] = {}
_REPORT_PROJECTS_LOCK = threading.Lock()
_REPORT_SCHEDULER_STARTED = False
_REPORT_SCHEDULER_START_LOCK = threading.Lock()


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


def _load_mcc_configs_from_env(shared_defaults: Optional[Dict[str, str]] = None) -> Dict[str, Dict[str, str]]:
    """
    Optional multi-MCC config from env JSON:
    Backward-compatible accepted shapes:
    1) Flat map (legacy):
       {
         "8776919182": {"label":"MCC A", "developer_token":"...", "client_id":"...", "client_secret":"...", "refresh_token":"...", "login_customer_id":"8776919182"}
       }
    2) Shared + per-MCC:
       {
         "shared": {"developer_token":"...", "client_id":"...", "client_secret":"...", "refresh_token":"..."},
         "mccs": {
           "8776919182": {"label":"MCC A", "login_customer_id":"8776919182"},
           "1234567890": {"label":"MCC B", "login_customer_id":"1234567890"}
         }
       }
    """
    raw = (os.getenv("GOOGLE_ADS_MCC_CONFIGS") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    shared = dict(shared_defaults or {})
    mcc_items = parsed
    if isinstance(parsed.get("shared"), dict):
        for key in ("developer_token", "client_id", "client_secret", "refresh_token"):
            v = str(parsed["shared"].get(key, "")).strip()
            if v:
                shared[key] = v
    if isinstance(parsed.get("mccs"), dict):
        mcc_items = parsed["mccs"]

    out: Dict[str, Dict[str, str]] = {}
    for raw_key, cfg in mcc_items.items():
        if not isinstance(cfg, dict):
            continue
        mcc_id = _normalize_customer_id(
            str(cfg.get("login_customer_id", "") or cfg.get("mcc_id", "") or raw_key)
        )
        if not mcc_id:
            continue
        out[mcc_id] = {
            "mcc_id": mcc_id,
            "label": str(cfg.get("label", "") or "").strip(),
            "developer_token": str(cfg.get("developer_token", "") or shared.get("developer_token", "")).strip(),
            "client_id": str(cfg.get("client_id", "") or shared.get("client_id", "")).strip(),
            "client_secret": str(cfg.get("client_secret", "") or shared.get("client_secret", "")).strip(),
            "refresh_token": str(cfg.get("refresh_token", "") or shared.get("refresh_token", "")).strip(),
        }
    return out


def _is_complete_mcc_config(cfg: Dict[str, str]) -> bool:
    required = ("developer_token", "client_id", "client_secret", "refresh_token", "mcc_id")
    return all(str(cfg.get(k, "")).strip() for k in required)

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


def _report_projects_path(project_root: Path) -> Path:
    return project_root / "report_projects.json"


def _normalize_database_url(url: str) -> str:
    # Railway may expose postgres://, psycopg expects postgresql://
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://") :]
    return url


def _init_report_projects_table(database_url: str) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS report_projects (
      id TEXT PRIMARY KEY,
      project_name TEXT NOT NULL DEFAULT '',
      mcc TEXT NOT NULL,
      cid TEXT NOT NULL,
      sheet_spreadsheet_id TEXT NOT NULL,
      sheet_tab_name TEXT NOT NULL,
      schedule_time TEXT NOT NULL,
      time_zone TEXT NOT NULL,
      active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TEXT NOT NULL,
      last_run_date TEXT NOT NULL DEFAULT '',
      last_run_at TEXT NOT NULL DEFAULT '',
      last_status TEXT NOT NULL DEFAULT '',
      last_error TEXT NOT NULL DEFAULT '',
      last_result JSONB,
      run_logs JSONB
    );
    """
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
            # Backward-compatible migration for old tables created before project_name existed.
            cur.execute("ALTER TABLE report_projects ADD COLUMN IF NOT EXISTS project_name TEXT NOT NULL DEFAULT ''")
            cur.execute("ALTER TABLE report_projects ADD COLUMN IF NOT EXISTS run_logs JSONB")


def _normalize_run_logs(raw: object, *, limit: int = 7) -> list[dict]:
    if not isinstance(raw, list):
        return []
    out: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "run_at": str(item.get("run_at", "")),
                "status": str(item.get("status", "")),
                "message": str(item.get("message", "")),
                "report_date": str(item.get("report_date", "")),
                "cells": int(item.get("cells", 0) or 0),
            }
        )
    if limit > 0:
        return out[:limit]
    return out


def _append_project_run_log(
    project: dict,
    *,
    run_at: str,
    status: str,
    message: str,
    report_date: str = "",
    cells: int = 0,
    keep: int = 7,
) -> None:
    entry = {
        "run_at": str(run_at or ""),
        "status": str(status or ""),
        "message": str(message or ""),
        "report_date": str(report_date or ""),
        "cells": int(cells or 0),
    }
    logs = _normalize_run_logs(project.get("run_logs"), limit=0)
    logs.insert(0, entry)
    project["run_logs"] = logs[: max(1, keep)]


def _db_report_projects_count(database_url: str) -> int:
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM report_projects")
            return int(cur.fetchone()[0] or 0)


def _migrate_report_projects_file_to_db(path: Path, database_url: str) -> int:
    """
    One-time best-effort migration:
    - If DB is empty but `report_projects.json` has items, upsert them into DB.
    """
    try:
        if _db_report_projects_count(database_url) > 0:
            return 0
    except Exception:
        return 0

    file_items = _load_report_projects(path, database_url=None)
    if not file_items:
        return 0
    try:
        _save_report_projects(path, file_items, database_url=database_url)
        return len(file_items)
    except Exception:
        return 0

def _load_report_projects(path: Path, database_url: Optional[str] = None) -> list[dict]:
    if database_url:
        query = """
            SELECT
              id, project_name, mcc, cid, sheet_spreadsheet_id, sheet_tab_name, schedule_time, time_zone,
              active, created_at, last_run_date, last_run_at, last_status, last_error, last_result, run_logs
            FROM report_projects
            ORDER BY created_at DESC
        """
        out: list[dict] = []
        with psycopg.connect(database_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                for row in cur.fetchall():
                    out.append(
                        {
                            "id": row[0],
                            "project_name": row[1] or row[5] or "",
                            "mcc": row[2],
                            "cid": row[3],
                            "sheet_spreadsheet_id": row[4],
                            "sheet_tab_name": row[5],
                            "schedule_time": row[6],
                            "time_zone": row[7],
                            "active": bool(row[8]),
                            "created_at": row[9],
                            "last_run_date": row[10] or "",
                            "last_run_at": row[11] or "",
                            "last_status": row[12] or "",
                            "last_error": row[13] or "",
                            "last_result": row[14] if isinstance(row[14], dict) else {},
                            "run_logs": _normalize_run_logs(row[15], limit=7),
                        }
                    )
        return out

    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    return [x for x in raw if isinstance(x, dict)]


def _save_report_projects(path: Path, projects: list[dict], database_url: Optional[str] = None) -> None:
    if database_url:
        def _row(p: dict) -> dict:
            # Ensure all placeholders exist to avoid KeyError in psycopg execute.
            return {
                "id": str(p.get("id", "")),
                "project_name": str(p.get("project_name", p.get("sheet_tab_name", ""))),
                "mcc": str(p.get("mcc", "")),
                "cid": str(p.get("cid", "")),
                "sheet_spreadsheet_id": str(p.get("sheet_spreadsheet_id", "")),
                "sheet_tab_name": str(p.get("sheet_tab_name", "")),
                "schedule_time": str(p.get("schedule_time", "06:00") or "06:00"),
                "time_zone": str(p.get("time_zone", "Asia/Ho_Chi_Minh") or "Asia/Ho_Chi_Minh"),
                "active": bool(p.get("active", True)),
                "created_at": str(p.get("created_at", "")),
                "last_run_date": str(p.get("last_run_date", "")),
                "last_run_at": str(p.get("last_run_at", "")),
                "last_status": str(p.get("last_status", "")),
                "last_error": str(p.get("last_error", "")),
                # psycopg needs Json/Jsonb wrapper for dict/list
                "last_result": (
                    Jsonb(p.get("last_result")) if isinstance(p.get("last_result"), (dict, list)) else None
                ),
                "run_logs": Jsonb(_normalize_run_logs(p.get("run_logs"), limit=7)),
            }

        upsert = """
            INSERT INTO report_projects (
              id, project_name, mcc, cid, sheet_spreadsheet_id, sheet_tab_name, schedule_time, time_zone,
              active, created_at, last_run_date, last_run_at, last_status, last_error, last_result, run_logs
            ) VALUES (
              %(id)s, %(project_name)s, %(mcc)s, %(cid)s, %(sheet_spreadsheet_id)s, %(sheet_tab_name)s, %(schedule_time)s, %(time_zone)s,
              %(active)s, %(created_at)s, %(last_run_date)s, %(last_run_at)s, %(last_status)s, %(last_error)s, %(last_result)s, %(run_logs)s
            )
            ON CONFLICT (id) DO UPDATE SET
              project_name = EXCLUDED.project_name,
              mcc = EXCLUDED.mcc,
              cid = EXCLUDED.cid,
              sheet_spreadsheet_id = EXCLUDED.sheet_spreadsheet_id,
              sheet_tab_name = EXCLUDED.sheet_tab_name,
              schedule_time = EXCLUDED.schedule_time,
              time_zone = EXCLUDED.time_zone,
              active = EXCLUDED.active,
              created_at = EXCLUDED.created_at,
              last_run_date = EXCLUDED.last_run_date,
              last_run_at = EXCLUDED.last_run_at,
              last_status = EXCLUDED.last_status,
              last_error = EXCLUDED.last_error,
              last_result = EXCLUDED.last_result,
              run_logs = EXCLUDED.run_logs
        """
        ids = [str(p.get("id", "")) for p in projects if str(p.get("id", ""))]
        with psycopg.connect(database_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                for p in projects:
                    cur.execute(upsert, _row(p))
                if ids:
                    cur.execute("DELETE FROM report_projects WHERE id <> ALL(%s)", (ids,))
                else:
                    cur.execute("DELETE FROM report_projects")
        return

    path.write_text(json.dumps(projects, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_tz(tz_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("Asia/Ho_Chi_Minh")


def _local_yesterday_iso(tz_name: str = "Asia/Ho_Chi_Minh") -> str:
    """Ngày 'hôm qua' theo lịch múi giờ (khớp sheet / kỳ vọng người dùng VN khi server chạy UTC)."""
    tz = _safe_tz(tz_name)
    return (datetime.now(tz).date() - timedelta(days=1)).isoformat()


def _daily_minute_slots_for_projects(
    projects: List[dict],
    *,
    day_key: str,
    default_schedule: str = "06:00",
) -> dict[str, int]:
    """
    +1 phút / 1 project cho mốc 06:00.
    Mỗi ngày random thứ tự project nhưng ổn định trong ngày đó.
    """
    spread_enabled = str(os.getenv("REPORT_ENABLE_SPREAD", "1")).strip().lower() in ("1", "true", "yes")
    if not spread_enabled:
        return {}

    candidate_ids: List[str] = []
    for p in projects:
        if not p.get("active", True):
            continue
        base = str(p.get("schedule_time", default_schedule) or default_schedule).strip()
        if base == "06:00":
            pid = str(p.get("id", "")).strip()
            if pid:
                candidate_ids.append(pid)

    if len(candidate_ids) <= 1:
        return {}

    seed_src = f"{day_key}|{','.join(sorted(candidate_ids))}"
    seed = int(hashlib.sha256(seed_src.encode("utf-8")).hexdigest()[:16], 16)
    rng = random.Random(seed)
    shuffled = candidate_ids[:]
    rng.shuffle(shuffled)
    return {pid: idx for idx, pid in enumerate(shuffled)}


def _effective_schedule_time(
    project: dict,
    *,
    default_schedule: str = "06:00",
    minute_slots: Optional[dict[str, int]] = None,
) -> str:
    """
    Stagger jobs around base time to avoid thundering herd at exactly 06:00.
    Applies spread only when base time is 06:00.
    """
    base = str(project.get("schedule_time", default_schedule) or default_schedule).strip()
    if not re.match(r"^\d{2}:\d{2}$", base):
        base = default_schedule
    hh, mm = [int(x) for x in base.split(":")]
    total = hh * 60 + mm

    if base == "06:00" and minute_slots:
        project_id = str(project.get("id", "")).strip()
        total += int(minute_slots.get(project_id, 0))

    total %= 24 * 60
    return f"{total // 60:02d}:{total % 60:02d}"


def _acquire_scheduler_leader(database_url: str) -> Optional[psycopg.Connection]:
    """
    Acquire cross-instance scheduler lock in Postgres.
    Returns a live connection that HOLDS the advisory lock.
    """
    conn = psycopg.connect(database_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (8104202601,))
            ok = bool(cur.fetchone()[0])
        if ok:
            return conn
        conn.close()
        return None
    except Exception:
        conn.close()
        return None


def _maybe_start_report_scheduler(path: Path, database_url: Optional[str]) -> None:
    global _REPORT_SCHEDULER_STARTED
    with _REPORT_SCHEDULER_START_LOCK:
        if _REPORT_SCHEDULER_STARTED:
            return
        _REPORT_SCHEDULER_STARTED = True

    def _runner() -> None:
        throttle_seconds = int((os.getenv("REPORT_JOB_THROTTLE_SECONDS") or "8").strip() or 8)
        while True:
            leader_conn: Optional[psycopg.Connection] = None
            try:
                # In production (Railway), only ONE instance should process schedules.
                if database_url:
                    leader_conn = _acquire_scheduler_leader(database_url)
                    if leader_conn is None:
                        time.sleep(10)
                        continue

                with _REPORT_PROJECTS_LOCK:
                    projects = _load_report_projects(path, database_url)

                now_utc = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
                minute_slots = _daily_minute_slots_for_projects(
                    projects,
                    day_key=now_utc.date().isoformat(),
                    default_schedule="06:00",
                )
                due_queue: list[tuple[datetime, dict, str, str]] = []
                for p in projects:
                    if not p.get("active", True):
                        continue
                    tz_name = str(p.get("time_zone", "Asia/Ho_Chi_Minh"))
                    tz = _safe_tz(tz_name)
                    now_local = now_utc.astimezone(tz)
                    today_local = now_local.date().isoformat()
                    if str(p.get("last_run_date", "")) == today_local:
                        continue
                    effective_sched = _effective_schedule_time(
                        p,
                        default_schedule="06:00",
                        minute_slots=minute_slots,
                    )
                    if not re.match(r"^\d{2}:\d{2}$", effective_sched):
                        continue
                    hh, mm = [int(x) for x in effective_sched.split(":")]
                    due_at = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
                    if now_local >= due_at:
                        due_queue.append((due_at, p, today_local, effective_sched))

                due_queue.sort(key=lambda x: x[0])
                changed = False
                for _, p, today_local, effective_sched in due_queue:
                    run_at_utc = datetime.utcnow().isoformat() + "Z"
                    try:
                        result = push_yesterday_report_to_sheet(
                            spreadsheet_id=str(p.get("sheet_spreadsheet_id", "")),
                            sheet_name=str(p.get("sheet_tab_name", "")),
                            customer_id=str(p.get("cid", "")),
                            sections=None,
                            scan_range="A1:CF60",
                            login_customer_id=str(p.get("mcc", "")).strip() or None,
                            time_zone=str(p.get("time_zone", "Asia/Ho_Chi_Minh") or "Asia/Ho_Chi_Minh"),
                        )
                        p["last_status"] = "success"
                        p["last_error"] = ""
                        p["last_result"] = result
                        _append_project_run_log(
                            p,
                            run_at=run_at_utc,
                            status="success",
                            message="NHẬP SHEET THÀNH CÔNG",
                            report_date=str(result.get("date", "")),
                            cells=int(result.get("cells", 0) or 0),
                            keep=7,
                        )
                    except Exception as ex:
                        p["last_status"] = "error"
                        p["last_error"] = f"{effective_sched} | {ex}"
                        _append_project_run_log(
                            p,
                            run_at=run_at_utc,
                            status="error",
                            message=str(ex),
                            report_date="",
                            cells=0,
                            keep=7,
                        )
                    p["last_run_date"] = today_local
                    p["last_run_at"] = run_at_utc
                    changed = True
                    if throttle_seconds > 0:
                        time.sleep(throttle_seconds)

                if changed:
                    with _REPORT_PROJECTS_LOCK:
                        _save_report_projects(path, projects, database_url)
            except Exception:
                # Keep scheduler alive regardless of one-loop errors.
                pass
            finally:
                if leader_conn is not None:
                    try:
                        with leader_conn.cursor() as cur:
                            cur.execute("SELECT pg_advisory_unlock(%s)", (8104202601,))
                    except Exception:
                        pass
                    try:
                        leader_conn.close()
                    except Exception:
                        pass

            time.sleep(30)

    th = threading.Thread(target=_runner, daemon=True, name="report-scheduler")
    th.start()


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
    app.jinja_env.filters["vnd"] = format_vnd_thousands
    app.jinja_env.filters["cidfmt"] = _format_customer_id_display
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = os.getenv("SESSION_COOKIE_SECURE", "1").strip().lower() in (
        "1",
        "true",
        "yes",
    )

    project_root = Path(__file__).resolve().parent
    google_ads_yaml = _maybe_bootstrap_google_ads_yaml(project_root)
    report_projects_file = _report_projects_path(project_root)
    database_url = _normalize_database_url((os.getenv("DATABASE_URL") or "").strip())
    if database_url:
        try:
            _init_report_projects_table(database_url)
            _migrate_report_projects_file_to_db(report_projects_file, database_url)
        except Exception as ex:
            raise RuntimeError(f"Cannot initialize report_projects table: {ex}") from ex
    _maybe_start_report_scheduler(report_projects_file, database_url or None)

    # Single-MCC fallback config (legacy).
    mcc_login_customer_id = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or None
    configured_mcc_id = _normalize_customer_id(
        mcc_login_customer_id or _read_login_customer_id_from_yaml(google_ads_yaml)
    )
    shared_google_ads_defaults = {
        "developer_token": (os.getenv("GOOGLE_ADS_SHARED_DEVELOPER_TOKEN") or "").strip(),
        "client_id": (os.getenv("GOOGLE_ADS_SHARED_CLIENT_ID") or "").strip(),
        "client_secret": (os.getenv("GOOGLE_ADS_SHARED_CLIENT_SECRET") or "").strip(),
        "refresh_token": (os.getenv("GOOGLE_ADS_SHARED_REFRESH_TOKEN") or "").strip(),
    }
    mcc_configs = _load_mcc_configs_from_env(shared_google_ads_defaults)
    default_mcc_id = configured_mcc_id or (next(iter(mcc_configs.keys())) if mcc_configs else "")

    def _resolve_active_mcc_id() -> str:
        requested = _normalize_customer_id(request.args.get("mcc_id", ""))
        if requested:
            return requested
        return _normalize_customer_id(str(session.get("active_mcc_id", "") or "")) or default_mcc_id

    def _build_google_ads_client_for_mcc(mcc_id: str):
        mcc_id = _normalize_customer_id(mcc_id)
        cfg = mcc_configs.get(mcc_id)
        if cfg and _is_complete_mcc_config(cfg):
            conf_dict: Dict[str, Any] = {
                "developer_token": cfg["developer_token"],
                "client_id": cfg["client_id"],
                "client_secret": cfg["client_secret"],
                "refresh_token": cfg["refresh_token"],
                "login_customer_id": mcc_id,
                "use_proto_plus": True,
            }
            return load_google_ads_client_from_dict(conf_dict)
        return load_google_ads_client(google_ads_yaml, default_login_customer_id=mcc_id or mcc_login_customer_id)

    def _mcc_options_for_ui() -> list[dict]:
        if mcc_configs:
            return [
                {
                    "mcc_id": mcc_id,
                    "label": (cfg.get("label") or f"MCC { _format_customer_id_display(mcc_id) }"),
                    "source": "env_json",
                }
                for mcc_id, cfg in mcc_configs.items()
            ]
        if default_mcc_id:
            return [
                {
                    "mcc_id": default_mcc_id,
                    "label": f"MCC { _format_customer_id_display(default_mcc_id) }",
                    "source": "yaml",
                }
            ]
        return []

    # Predefined client customer IDs (comma-separated) for the dashboard.
    # Example: CLIENT_CUSTOMER_IDS=1234567890,0987654321
    dashboard_customer_ids = _env_list("CLIENT_CUSTOMER_IDS", "")
    sheet_spreadsheet_id = (os.getenv("SHEET_SPREADSHEET_ID") or "").strip()
    sheet_tab_name = (os.getenv("SHEET_TAB_NAME") or "2.2 Report Ads google").strip()
    sheet_sections_env = (os.getenv("SHEET_SECTIONS") or "").strip()
    sheet_scan_range = (os.getenv("SHEET_SCAN_RANGE") or "A1:CF60").strip()
    sheet_time_zone = (os.getenv("SHEET_TIME_ZONE") or "Asia/Ho_Chi_Minh").strip()
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
                "active_mcc_id": "",
                "mcc_options": [],
                "child_accounts": [],
                "mcc_context_error": "",
            }
        try:
            mcc_id = _resolve_active_mcc_id()
            if mcc_id:
                session["active_mcc_id"] = mcc_id
            if not mcc_id:
                return {
                    "mcc_name": "",
                    "mcc_id": "",
                    "active_mcc_id": "",
                    "mcc_options": _mcc_options_for_ui(),
                    "child_accounts": [],
                    "mcc_context_error": "Thiếu login_customer_id trong cấu hình.",
                }
            client = _build_google_ads_client_for_mcc(mcc_id)

            now = time.time()
            cached = _ACCOUNT_CACHE_BY_MCC.get(mcc_id) or {}
            if (
                (now - float(cached.get("ts", 0.0))) < 60
                and cached.get("children")
            ):
                return {
                    "mcc_name": str(cached.get("mcc_name", "")),
                    "mcc_id": _format_customer_id_display(mcc_id),
                    "active_mcc_id": mcc_id,
                    "mcc_options": _mcc_options_for_ui(),
                    "child_accounts": cached.get("children", []),
                    "mcc_context_error": "",
                }

            mcc_name = get_customer_name(client, mcc_id)
            children = list_child_accounts_under_mcc(client, mcc_id)
            _ACCOUNT_CACHE_BY_MCC[mcc_id] = {
                "ts": now,
                "mcc_name": mcc_name,
                "children": children,
            }
            return {
                "mcc_name": mcc_name,
                "mcc_id": _format_customer_id_display(mcc_id),
                "active_mcc_id": mcc_id,
                "mcc_options": _mcc_options_for_ui(),
                "child_accounts": children,
                "mcc_context_error": "",
            }
        except Exception as ex:
            # Keep MCC visible even if account-list lookup fails.
            return {
                "mcc_name": "",
                "mcc_id": _format_customer_id_display(_resolve_active_mcc_id()),
                "active_mcc_id": _resolve_active_mcc_id(),
                "mcc_options": _mcc_options_for_ui(),
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

    @app.get("/api/mcc-options")
    def api_mcc_options():
        return jsonify({"ok": True, "items": _mcc_options_for_ui(), "default_mcc_id": default_mcc_id})

    @app.get("/api/mcc-accounts")
    def api_mcc_accounts():
        """
        JSON test endpoint: child accounts under the configured MCC (GAQL customer_client).
        """
        active_mcc_id = _resolve_active_mcc_id()
        if not active_mcc_id:
            return jsonify({"ok": False, "error": "Thiếu login_customer_id trong cấu hình."}), 400
        try:
            client = _build_google_ads_client_for_mcc(active_mcc_id)
            children = list_child_accounts_under_mcc(client, active_mcc_id)
            return jsonify(
                {
                    "ok": True,
                    "mcc_customer_id": active_mcc_id,
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
            client = _build_google_ads_client_for_mcc(_resolve_active_mcc_id())
            rows = get_yesterday_campaign_performance(client, [cid])
            rdate = _local_yesterday_iso()
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
        selected_mcc_id = _resolve_active_mcc_id()
        if selected_mcc_id:
            session["active_mcc_id"] = selected_mcc_id

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
                selected_mcc_id=selected_mcc_id,
                sheet_push_enabled=bool(sheet_spreadsheet_id),
                sheet_tab_name=sheet_tab_name,
            )

        report_q: dict = {"report": "1"}
        if requested_customer_id_raw:
            report_q["customer_id"] = requested_customer_id_raw
        if selected_mcc_id:
            report_q["mcc_id"] = selected_mcc_id
        report_url = url_for("dashboard", **report_q)

        rows = []
        error = None
        report_date = None
        if want_report:
            try:
                client = _build_google_ads_client_for_mcc(selected_mcc_id)
                rows = get_yesterday_campaign_performance(client, customer_ids)
                report_date = _local_yesterday_iso()
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
            selected_mcc_id=selected_mcc_id,
            sheet_push_enabled=bool(sheet_spreadsheet_id),
            sheet_tab_name=sheet_tab_name,
        )

    @app.get("/report-projects")
    def report_projects():
        with _REPORT_PROJECTS_LOCK:
            projects = _load_report_projects(report_projects_file, database_url or None)
        projects.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return render_template("report_projects.html", projects=projects)

    @app.post("/report-projects")
    def create_report_project():
        project_name = (request.form.get("project_name") or "").strip()
        mcc = _normalize_customer_id(request.form.get("mcc", ""))
        cid = _normalize_customer_id(request.form.get("cid", ""))
        spreadsheet_id = (request.form.get("sheet_spreadsheet_id") or "").strip()
        tab_name = (request.form.get("sheet_tab_name") or "").strip()
        schedule_time = (request.form.get("schedule_time") or "06:00").strip()
        time_zone = (request.form.get("time_zone") or "Asia/Ho_Chi_Minh").strip()
        active = (request.form.get("active") or "on").strip().lower() in ("1", "true", "yes", "on")

        if not (project_name and mcc and cid and spreadsheet_id and tab_name):
            flash("Thiếu thông tin bắt buộc (Tên project, MCC, CID, Spreadsheet ID, Sheet tab).", "warning")
            return redirect(url_for("report_projects"))
        if not re.match(r"^\d{2}:\d{2}$", schedule_time):
            flash("SCHEDULE_TIME không hợp lệ. Dùng định dạng HH:MM, ví dụ 06:00.", "warning")
            return redirect(url_for("report_projects"))

        item = {
            "id": str(uuid4()),
            "project_name": project_name,
            "mcc": mcc,
            "cid": cid,
            "sheet_spreadsheet_id": spreadsheet_id,
            "sheet_tab_name": tab_name,
            "schedule_time": schedule_time,
            "time_zone": time_zone or "Asia/Ho_Chi_Minh",
            "active": active,
            "created_at": datetime.utcnow().isoformat() + "Z",
            "last_run_date": "",
            "last_run_at": "",
            "last_status": "",
            "last_error": "",
            "run_logs": [],
        }
        with _REPORT_PROJECTS_LOCK:
            projects = _load_report_projects(report_projects_file, database_url or None)
            projects.append(item)
            _save_report_projects(report_projects_file, projects, database_url or None)
        flash("Đã tạo project báo cáo tự động.", "success")
        return redirect(url_for("report_projects"))

    @app.post("/report-projects/<project_id>/edit")
    def edit_report_project(project_id: str):
        project_name = (request.form.get("project_name") or "").strip()
        mcc = _normalize_customer_id(request.form.get("mcc", ""))
        cid = _normalize_customer_id(request.form.get("cid", ""))
        spreadsheet_id = (request.form.get("sheet_spreadsheet_id") or "").strip()
        tab_name = (request.form.get("sheet_tab_name") or "").strip()
        schedule_time = (request.form.get("schedule_time") or "06:00").strip()
        time_zone = (request.form.get("time_zone") or "Asia/Ho_Chi_Minh").strip()
        active = (request.form.get("active") or "").strip().lower() in ("1", "true", "yes", "on")

        if not (project_name and mcc and cid and spreadsheet_id and tab_name):
            flash("Thiếu thông tin bắt buộc để cập nhật project.", "warning")
            return redirect(url_for("report_projects"))
        if not re.match(r"^\d{2}:\d{2}$", schedule_time):
            flash("Schedule time không hợp lệ. Dùng định dạng HH:MM, ví dụ 06:00.", "warning")
            return redirect(url_for("report_projects"))

        updated = False
        with _REPORT_PROJECTS_LOCK:
            projects = _load_report_projects(report_projects_file, database_url or None)
            for p in projects:
                if p.get("id") != project_id:
                    continue
                p["project_name"] = project_name
                p["mcc"] = mcc
                p["cid"] = cid
                p["sheet_spreadsheet_id"] = spreadsheet_id
                p["sheet_tab_name"] = tab_name
                p["schedule_time"] = schedule_time
                p["time_zone"] = time_zone or "Asia/Ho_Chi_Minh"
                p["active"] = active
                updated = True
                break
            if updated:
                _save_report_projects(report_projects_file, projects, database_url or None)

        if updated:
            flash("Đã cập nhật project.", "success")
        else:
            flash("Không tìm thấy project để cập nhật.", "warning")
        return redirect(url_for("report_projects"))

    @app.post("/report-projects/<project_id>/toggle")
    def toggle_report_project(project_id: str):
        with _REPORT_PROJECTS_LOCK:
            projects = _load_report_projects(report_projects_file, database_url or None)
            for p in projects:
                if p.get("id") == project_id:
                    p["active"] = not bool(p.get("active", True))
                    break
            _save_report_projects(report_projects_file, projects, database_url or None)
        flash("Đã cập nhật trạng thái project.", "info")
        return redirect(url_for("report_projects"))

    @app.post("/report-projects/<project_id>/delete")
    def delete_report_project(project_id: str):
        deleted = False
        with _REPORT_PROJECTS_LOCK:
            projects = _load_report_projects(report_projects_file, database_url or None)
            kept = [p for p in projects if p.get("id") != project_id]
            deleted = len(kept) != len(projects)
            if deleted:
                _save_report_projects(report_projects_file, kept, database_url or None)
        if deleted:
            flash("Đã xóa project.", "success")
        else:
            flash("Không tìm thấy project để xóa.", "warning")
        return redirect(url_for("report_projects"))

    @app.post("/report-projects/<project_id>/run-now")
    def run_report_project_now(project_id: str):
        with _REPORT_PROJECTS_LOCK:
            projects = _load_report_projects(report_projects_file, database_url or None)
            target = next((p for p in projects if p.get("id") == project_id), None)
        if not target:
            flash("Không tìm thấy project.", "warning")
            return redirect(url_for("report_projects"))
        try:
            run_at_utc = datetime.utcnow().isoformat() + "Z"
            result = push_yesterday_report_to_sheet(
                spreadsheet_id=str(target.get("sheet_spreadsheet_id", "")),
                sheet_name=str(target.get("sheet_tab_name", "")),
                customer_id=str(target.get("cid", "")),
                sections=None,
                scan_range="A1:CF60",
                login_customer_id=str(target.get("mcc", "")).strip() or None,
                time_zone=str(target.get("time_zone", "Asia/Ho_Chi_Minh") or "Asia/Ho_Chi_Minh"),
            )
            target["last_status"] = "success"
            target["last_error"] = ""
            target["last_result"] = result
            _append_project_run_log(
                target,
                run_at=run_at_utc,
                status="success",
                message="NHẬP SHEET THÀNH CÔNG",
                report_date=str(result.get("date", "")),
                cells=int(result.get("cells", 0) or 0),
                keep=7,
            )
            flash("Đã chạy nhập sheet thủ công thành công.", "success")
        except Exception as ex:
            run_at_utc = datetime.utcnow().isoformat() + "Z"
            target["last_status"] = "error"
            target["last_error"] = str(ex)
            _append_project_run_log(
                target,
                run_at=run_at_utc,
                status="error",
                message=str(ex),
                report_date="",
                cells=0,
                keep=7,
            )
            flash(f"Lỗi chạy thủ công: {ex}", "danger")
        target["last_run_date"] = datetime.utcnow().date().isoformat()
        target["last_run_at"] = run_at_utc
        with _REPORT_PROJECTS_LOCK:
            projects = _load_report_projects(report_projects_file, database_url or None)
            for idx, p in enumerate(projects):
                if p.get("id") == project_id:
                    projects[idx] = target
                    break
            _save_report_projects(report_projects_file, projects, database_url or None)
        return redirect(url_for("report_projects"))

    @app.post("/dashboard/push-sheet")
    def push_sheet():
        customer_id = _normalize_customer_id(request.form.get("customer_id", ""))
        selected_mcc_id = _normalize_customer_id(request.form.get("mcc_id", "")) or _resolve_active_mcc_id()
        if not customer_id:
            flash("Thiếu customer_id để ghi sheet.", "warning")
            return redirect(url_for("dashboard"))
        if not sheet_spreadsheet_id:
            flash("Thiếu cấu hình SHEET_SPREADSHEET_ID trên môi trường.", "warning")
            return redirect(url_for("dashboard", customer_id=customer_id, report=1, mcc_id=selected_mcc_id))

        sections = [x.strip() for x in sheet_sections_env.split(",") if x.strip()] if sheet_sections_env else None
        try:
            result = push_yesterday_report_to_sheet(
                spreadsheet_id=sheet_spreadsheet_id,
                sheet_name=sheet_tab_name,
                customer_id=customer_id,
                sections=sections,
                scan_range=sheet_scan_range,
                login_customer_id=selected_mcc_id or None,
                time_zone=sheet_time_zone or "Asia/Ho_Chi_Minh",
            )
            flash(
                f"Đã nhập sheet thành công: {result['sheet']} | ngày {result['date']} | ô cập nhật {result['cells']}.",
                "success",
            )
        except Exception as ex:
            flash(f"Lỗi nhập sheet: {ex}", "danger")
        return redirect(url_for("dashboard", customer_id=customer_id, report=1, mcc_id=selected_mcc_id))

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
            client = _build_google_ads_client_for_mcc(_resolve_active_mcc_id())
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
            selected_mcc_id = _resolve_active_mcc_id()
            client = _build_google_ads_client_for_mcc(selected_mcc_id)
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
                report_url=url_for("dashboard", customer_id=customer_id, report=1, mcc_id=selected_mcc_id),
                report_date=None,
                can_fetch_report_js=bool(_normalize_customer_id(customer_id)),
                show_report_cta=False,
                selected_mcc_id=selected_mcc_id,
                sheet_push_enabled=bool(sheet_spreadsheet_id),
                sheet_tab_name=sheet_tab_name,
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

