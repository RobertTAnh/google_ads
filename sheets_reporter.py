from __future__ import annotations

import argparse
import base64
import json
import os
import re
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from google_ads_helper import (
    CampaignPerformanceRow,
    GoogleAdsHelperError,
    get_yesterday_campaign_performance,
    load_google_ads_client,
)


MetricKey = str


def _yesterday_dmy() -> str:
    d = date.today() - timedelta(days=1)
    return f"{d.day}/{d.month}"


def _normalize_cell_text(s: Any) -> str:
    return str(s or "").strip()


def _try_parse_service_account_json(content: str) -> Dict[str, Any]:
    """
    Parse service account JSON with repair for common malformed private_key strings.
    Strategy 1: direct json.loads.
    Strategy 2: regex-based extraction of private_key PEM, replace actual newlines with \\n.
    Strategy 3: brute-force strip all literal newlines inside the PEM block.
    """
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    # Strategy 2: use regex to capture everything between BEGIN/END PRIVATE KEY markers
    # and repair actual newlines -> \n escape sequences.
    PK_RE = re.compile(
        r'("private_key"\s*:\s*")(-----BEGIN (?:RSA )?PRIVATE KEY-----.*?-----END (?:RSA )?PRIVATE KEY-----[^\n"]*)',
        re.DOTALL,
    )
    m = PK_RE.search(content)
    if m:
        prefix = m.group(1)
        pem_raw = m.group(2)
        pem_fixed = pem_raw.replace("\r\n", "\n").replace("\n", "\\n")
        repaired = content[: m.start(2)] + pem_fixed + content[m.end(2) :]
        try:
            parsed = json.loads(repaired)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    # Strategy 3: replace ALL literal newlines inside string values (nuclear option)
    # Walk the JSON char by char; when inside a string, replace \n with \\n.
    chars = list(content)
    in_str = False
    i = 0
    result: list[str] = []
    while i < len(chars):
        c = chars[i]
        if not in_str:
            if c == '"':
                in_str = True
            result.append(c)
        else:
            if c == '\\' and i + 1 < len(chars):
                result.append(c)
                result.append(chars[i + 1])
                i += 2
                continue
            elif c == '"':
                in_str = False
                result.append(c)
            elif c == '\n':
                result.append('\\n')
            elif c == '\r':
                result.append('\\r')
            else:
                result.append(c)
        i += 1

    try:
        parsed = json.loads("".join(result))
        if isinstance(parsed, dict):
            return parsed
    except Exception as ex:
        raise RuntimeError(f"Service account JSON could not be repaired: {ex}") from ex

    raise RuntimeError("Service account JSON parsed but invalid object.")


def _find_section_row(values: List[List[Any]], section_name: str, *, col_idx: int = 1) -> Optional[int]:
    target = section_name.strip().lower()
    for r, row in enumerate(values):
        if col_idx < len(row) and _normalize_cell_text(row[col_idx]).lower() == target:
            return r
    return None


def _find_date_col(values: List[List[Any]], header_row: int, dmy: str) -> Optional[int]:
    """
    Trên sheet, ngày nằm cùng hàng với title section, dạng '25/3'.
    Tìm exact match trước; fallback match theo regex d/m.
    """
    row = values[header_row] if 0 <= header_row < len(values) else []
    dmy_norm = dmy.strip()
    for c, v in enumerate(row):
        if _normalize_cell_text(v) == dmy_norm:
            return c
    # Fallback: so khớp 'dd/m' hoặc 'd/m' có thể có khoảng trắng
    pattern = re.compile(rf"^\s*{re.escape(dmy_norm)}\s*$")
    for c, v in enumerate(row):
        if pattern.match(str(v or "")):
            return c
    return None


def _find_metric_rows(
    values: List[List[Any]],
    start_row: int,
    metric_labels: Sequence[str],
    *,
    col_idx: int = 1,
    scan_limit: int = 20,
) -> Dict[str, int]:
    """
    Tìm các dòng 'Chi phí', 'Hiển thị', 'Click', 'Chuyển đổi' ngay dưới section.
    """
    wanted = {m.lower(): m for m in metric_labels}
    found: Dict[str, int] = {}
    for r in range(start_row, min(len(values), start_row + scan_limit)):
        row = values[r]
        label = _normalize_cell_text(row[col_idx] if col_idx < len(row) else "").lower()
        if label in wanted and wanted[label] not in found:
            found[wanted[label]] = r
        if len(found) == len(metric_labels):
            break
    return found


def _a1_col(c0: int) -> str:
    """0-based col -> A1 column letters."""
    n = c0 + 1
    out = ""
    while n:
        n, r = divmod(n - 1, 26)
        out = chr(ord("A") + r) + out
    return out


def _a1(row0: int, col0: int) -> str:
    return f"{_a1_col(col0)}{row0 + 1}"


def _sum_rows_for_campaign(rows: List[CampaignPerformanceRow], campaign_name: str) -> CampaignPerformanceRow:
    """
    Gộp (sum) metrics cho các dòng có campaign_name trùng.
    - clicks, impressions: int
    - cost, conversions: float
    """
    target = campaign_name.strip().lower()
    matched = [r for r in rows if (r.campaign_name or "").strip().lower() == target]
    if not matched:
        return CampaignPerformanceRow(
            customer_id="",
            customer_name="",
            campaign_id="",
            campaign_name=campaign_name,
            clicks=0,
            impressions=0,
            cost=0.0,
            conversions=0.0,
        )
    base = matched[0]
    return CampaignPerformanceRow(
        customer_id=base.customer_id,
        customer_name=base.customer_name,
        campaign_id=base.campaign_id,
        campaign_name=campaign_name,
        clicks=sum(int(r.clicks or 0) for r in matched),
        impressions=sum(int(r.impressions or 0) for r in matched),
        cost=sum(float(r.cost or 0.0) for r in matched),
        conversions=sum(float(r.conversions or 0.0) for r in matched),
    )


def build_sheets_service() -> Any:
    """
    Auth bằng Service Account.
    Railway-friendly:
    - Ưu tiên đọc JSON từ env `GOOGLE_SA_JSON_B64` / `GOOGLE_SA_JSON` (không cần file).
    - Fallback: đọc từ file theo env `GOOGLE_APPLICATION_CREDENTIALS`.
    """
    raw_b64 = (os.getenv("GOOGLE_SA_JSON_B64") or "").strip()
    raw_text = os.getenv("GOOGLE_SA_JSON")
    content = ""
    if raw_b64:
        try:
            b64 = "".join(raw_b64.split())
            missing = (-len(b64)) % 4
            if missing:
                b64 = b64 + ("=" * missing)
            content = base64.b64decode(b64).decode("utf-8")
        except Exception as ex:
            raise RuntimeError(f"Invalid GOOGLE_SA_JSON_B64: {ex}") from ex
    elif raw_text:
        content = raw_text.strip()
        if (content.startswith('"') and content.endswith('"')) or (content.startswith("'") and content.endswith("'")):
            content = content[1:-1]

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if content:
        try:
            info = _try_parse_service_account_json(content)
        except Exception as ex:
            raise RuntimeError(
                "Service account JSON is invalid. "
                "Khuyến nghị dùng GOOGLE_SA_JSON_B64 (base64 từ file .json) thay vì GOOGLE_SA_JSON raw. "
                f"Parse error: {ex}"
            ) from ex
        # Normalize private_key if it looks like it contains literal "\\n" instead of real newlines.
        # (Do not log the key; only log lengths/flags.)
        pk = info.get("private_key")
        if isinstance(pk, str):
            pk_s = pk.strip().strip('"').strip("'")
            has_begin = "BEGIN PRIVATE KEY" in pk_s or "BEGIN RSA PRIVATE KEY" in pk_s
            has_end = "END PRIVATE KEY" in pk_s or "END RSA PRIVATE KEY" in pk_s
            has_real_nl = "\n" in pk_s
            has_literal_slash_n = "\\n" in pk_s
            if has_begin and has_end and (not has_real_nl) and has_literal_slash_n:
                info["private_key"] = pk_s.replace("\\n", "\n")
            else:
                info["private_key"] = pk_s

            # #region agent log
            # PEM sanity-check & optional repair for common corruption where '+' becomes ' '.
            # Never log the PEM content; only log flags/counts.
            try:
                pk_chk = info.get("private_key") if isinstance(info.get("private_key"), str) else ""
                pem_has_begin = ("-----BEGIN PRIVATE KEY-----" in pk_chk) or ("-----BEGIN RSA PRIVATE KEY-----" in pk_chk)
                pem_has_end = ("-----END PRIVATE KEY-----" in pk_chk) or ("-----END RSA PRIVATE KEY-----" in pk_chk)
                pem_spaces = pk_chk.count(" ")
                pem_tabs = pk_chk.count("\t")
                pem_cr = pk_chk.count("\r")
                pem_lf = pk_chk.count("\n")
                pem_body_b64_ok = False
                pem_body_b64_ok_after_space_fix = False
                if pem_has_begin and pem_has_end:
                    begin = "-----BEGIN PRIVATE KEY-----" if "-----BEGIN PRIVATE KEY-----" in pk_chk else "-----BEGIN RSA PRIVATE KEY-----"
                    end = "-----END PRIVATE KEY-----" if "-----END PRIVATE KEY-----" in pk_chk else "-----END RSA PRIVATE KEY-----"
                    body = pk_chk.split(begin, 1)[1].split(end, 1)[0]
                    body_compact = "".join(body.split())
                    try:
                        base64.b64decode(body_compact, validate=True)
                        pem_body_b64_ok = True
                    except Exception:
                        if " " in body_compact:
                            try:
                                base64.b64decode(body_compact.replace(" ", "+"), validate=True)
                                pem_body_b64_ok_after_space_fix = True
                            except Exception:
                                pass
                        else:
                            try:
                                base64.b64decode(body_compact.replace(" ", "+"), validate=True)
                                pem_body_b64_ok_after_space_fix = True
                            except Exception:
                                pass

                    # Apply repair only if it makes the base64 body valid.
                    if (not pem_body_b64_ok) and pem_body_b64_ok_after_space_fix:
                        repaired_compact = body_compact.replace(" ", "+")
                        wrapped = "\n".join(repaired_compact[i : i + 64] for i in range(0, len(repaired_compact), 64))
                        info["private_key"] = f"{begin}\n{wrapped}\n{end}\n"

                # Emit a compact debug string in errors (still secret-safe).
                info["_pem_dbg"] = {
                    "pem_has_begin": bool(pem_has_begin),
                    "pem_has_end": bool(pem_has_end),
                    "pem_spaces": int(pem_spaces),
                    "pem_tabs": int(pem_tabs),
                    "pem_cr": int(pem_cr),
                    "pem_lf": int(pem_lf),
                    "pem_body_b64_ok": bool(pem_body_b64_ok),
                    "pem_body_b64_ok_after_space_fix": bool(pem_body_b64_ok_after_space_fix),
                }
            except Exception:
                pass
            # #endregion

        try:
            creds = Credentials.from_service_account_info(info, scopes=scopes)
        except Exception as ex:
            pk2 = info.get("private_key")
            pk2s = pk2 if isinstance(pk2, str) else ""
            has_begin2 = "BEGIN PRIVATE KEY" in pk2s or "BEGIN RSA PRIVATE KEY" in pk2s
            has_end2 = "END PRIVATE KEY" in pk2s or "END RSA PRIVATE KEY" in pk2s
            dbg = {
                "b64": bool(raw_b64),
                "raw": bool(raw_text),
                "clen": len(content),
                "pk_len": len(pk2s),
                "pk_has_begin": bool(has_begin2),
                "pk_has_end": bool(has_end2),
                "pk_has_real_newlines": ("\n" in pk2s),
                "pk_has_literal_slash_n": ("\\n" in pk2s),
                "pem": info.get("_pem_dbg", None),
            }
            raise RuntimeError(
                "Unable to load service account private_key PEM. "
                f"dbg={dbg} err={ex}"
            ) from ex

        return build("sheets", "v4", credentials=creds, cache_discovery=False)

    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not cred_path:
        raise RuntimeError(
            "Thiếu credential Google Sheets. "
            "Set GOOGLE_SA_JSON_B64 (khuyên dùng) hoặc GOOGLE_APPLICATION_CREDENTIALS."
        )
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def load_sheet_values(service: Any, spreadsheet_id: str, sheet_name: str, a1_range: str) -> List[List[Any]]:
    resp = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!{a1_range}")
        .execute()
    )
    return resp.get("values", []) or []


def batch_update_values(
    service: Any,
    spreadsheet_id: str,
    sheet_name: str,
    updates: List[Tuple[int, int, Any]],
) -> None:
    """
    updates: list (row0, col0, value)
    """
    data = []
    for r0, c0, v in updates:
        rng = f"{sheet_name}!{_a1(r0, c0)}"
        data.append({"range": rng, "values": [[v]]})
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def push_yesterday_report_to_sheet(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    customer_id: str,
    sections: Optional[List[str]] = None,
    scan_range: str = "A1:CF60",
    login_customer_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ghi dữ liệu chiến dịch (hôm qua) vào Google Sheet.
    Trả về metadata để hiển thị cho UI/API.
    """
    sections = [x.strip() for x in (sections or []) if x and x.strip()]

    project_root = os.path.dirname(os.path.abspath(__file__))
    yaml_path = os.path.join(project_root, "google-ads.yaml")
    client = load_google_ads_client(
        yaml_path, default_login_customer_id=login_customer_id or os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or None
    )
    campaign_rows = get_yesterday_campaign_performance(client, [customer_id])

    if not sections:
        # Auto-map theo campaign name thực có dữ liệu hôm qua.
        seen = set()
        auto_sections: List[str] = []
        for r in campaign_rows:
            name = (r.campaign_name or "").strip()
            if name and name.lower() not in seen:
                seen.add(name.lower())
                auto_sections.append(name)
        sections = auto_sections
    if not sections:
        raise RuntimeError("Không có campaign nào để điền vào sheet.")

    svc = build_sheets_service()
    grid = load_sheet_values(svc, spreadsheet_id, sheet_name, scan_range)
    if not grid:
        raise RuntimeError("Không đọc được sheet values (range trống hoặc sai sheet-name/range).")

    dmy = _yesterday_dmy()
    metric_labels = ["Chi phí", "Hiển thị", "Click", "Chuyển đổi"]
    updates: List[Tuple[int, int, Any]] = []

    for section in sections:
        header_row = _find_section_row(grid, section, col_idx=1)
        if header_row is None:
            raise RuntimeError(f"Không tìm thấy section '{section}' trong cột B (range {scan_range}).")

        date_col = _find_date_col(grid, header_row, dmy)
        if date_col is None:
            raise RuntimeError(f"Không tìm thấy cột ngày '{dmy}' tại hàng section '{section}'.")

        metric_rows = _find_metric_rows(grid, header_row + 1, metric_labels, col_idx=1, scan_limit=25)
        missing = [m for m in metric_labels if m not in metric_rows]
        if missing:
            raise RuntimeError(f"Section '{section}' thiếu các hàng metric: {', '.join(missing)}")

        summed = _sum_rows_for_campaign(campaign_rows, section)
        cost_vnd = int(round(float(summed.cost or 0.0)))
        updates.extend(
            [
                (metric_rows["Chi phí"], date_col, cost_vnd),
                (metric_rows["Hiển thị"], date_col, int(summed.impressions or 0)),
                (metric_rows["Click"], date_col, int(summed.clicks or 0)),
                (metric_rows["Chuyển đổi"], date_col, float(summed.conversions or 0.0)),
            ]
        )

    batch_update_values(svc, spreadsheet_id, sheet_name, updates)
    return {
        "spreadsheet_id": spreadsheet_id,
        "sheet": sheet_name,
        "date": dmy,
        "sections": sections,
        "cells": len(updates),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Đổ báo cáo Google Ads (yesterday) vào Google Sheet.")
    parser.add_argument("--spreadsheet-id", required=True, help="Spreadsheet ID (phần giữa /d/.../edit).")
    parser.add_argument("--sheet-name", required=True, help="Tên tab sheet, ví dụ: 2.2 Report Ads google")
    parser.add_argument("--customer-id", required=True, help="Customer ID tk con (10 digits hoặc có dấu -).")
    parser.add_argument(
        "--sections",
        default="Search - PN,Search - VLCN",
        help="Danh sách section/campaign cần điền, cách nhau bởi dấu phẩy.",
    )
    parser.add_argument(
        "--scan-range",
        default="A1:CF60",
        help="Range đủ lớn để chứa header ngày + block metrics (mặc định A1:CF60).",
    )
    args = parser.parse_args()

    spreadsheet_id = args.spreadsheet_id.strip()
    sheet_name = args.sheet_name.strip()
    customer_id = args.customer_id.strip()
    sections = [x.strip() for x in args.sections.split(",") if x.strip()]

    try:
        result = push_yesterday_report_to_sheet(
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            customer_id=customer_id,
            sections=sections,
            scan_range=args.scan_range,
        )
    except (GoogleAdsHelperError, RuntimeError) as e:
        raise SystemExit(str(e))
    print("OK. Đã ghi sheet:", result)


if __name__ == "__main__":
    main()

