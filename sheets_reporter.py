from __future__ import annotations

import argparse
import os
import re
from dataclasses import asdict
from datetime import date, timedelta
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
    Yêu cầu env GOOGLE_APPLICATION_CREDENTIALS trỏ tới file JSON service account.
    """
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not cred_path:
        raise RuntimeError("Thiếu env GOOGLE_APPLICATION_CREDENTIALS (đường dẫn JSON service account).")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
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
) -> Dict[str, Any]:
    """
    Ghi dữ liệu chiến dịch (hôm qua) vào Google Sheet.
    Trả về metadata để hiển thị cho UI/API.
    """
    sections = [x.strip() for x in (sections or []) if x and x.strip()]

    project_root = os.path.dirname(os.path.abspath(__file__))
    yaml_path = os.path.join(project_root, "google-ads.yaml")
    client = load_google_ads_client(
        yaml_path, default_login_customer_id=os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or None
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

