"""
Báo cáo hiệu suất Google Ads cho ngày hôm qua (chi phí, click, hiển thị, chuyển đổi).

- Chạy một lần:  python bao_cao.py
- Lịch 6h sáng mỗi ngày:  python bao_cao.py --schedule

Cấu hình:
  - google-ads.yaml ở thư mục gốc project (cùng cấp file này).
  - CLIENT_CUSTOMER_IDS: danh sách ID tài khoản quảng cáo (tk con), phân tách bởi dấu phẩy
    (giống app.py). Ví dụ: set biến môi trường hoặc chỉnh CUSTOMER_IDS bên dưới.
  - GOOGLE_ADS_LOGIN_CUSTOMER_ID: (tùy chọn) ghi đè MCC nếu cần.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

from google.ads.googleads.errors import GoogleAdsException

from google_ads_helper import (
    GoogleAdsHelperError,
    CampaignPerformanceRow,
    format_vnd_thousands,
    get_yesterday_campaign_performance,
    load_google_ads_client,
)

# --- Cấu hình nhanh (ưu tiên thấp hơn biến môi trường CLIENT_CUSTOMER_IDS) ---
CUSTOMER_IDS: List[str] = []


def _env_customer_ids() -> List[str]:
    raw = os.getenv("CLIENT_CUSTOMER_IDS", "")
    return [x.strip() for x in raw.split(",") if x.strip()]


def _resolve_customer_ids() -> List[str]:
    env_ids = _env_customer_ids()
    if env_ids:
        return env_ids
    return [x.strip() for x in CUSTOMER_IDS if x.strip()]


def _yesterday_label() -> str:
    """Ngày báo cáo theo lịch local (Google Ads dùng múi giờ tài khoản cho YESTERDAY)."""
    d = date.today() - timedelta(days=1)
    return d.isoformat()


def _format_table(rows: List[CampaignPerformanceRow], report_date: str) -> str:
    """Bảng ASCII: theo từng chiến dịch — chi phí (VNĐ, dấu chấm phân cách nghìn), click, hiển thị, chuyển đổi."""
    lines: List[str] = []
    lines.append("")
    lines.append(f"Báo cáo Google Ads (theo Campaign) — ngày {report_date} (YESTERDAY theo API)")
    w_acc = 22
    w_cid = 10
    w_cname = 28
    w_cost = 22
    width = w_acc + 1 + 10 + 1 + w_cname + 1 + w_cost + 1 + 8 + 1 + 10 + 1 + 12
    lines.append("=" * width)
    header = (
        f"{'Tài khoản':<{w_acc}} "
        f"{'Camp.ID':>10} "
        f"{'Chiến dịch':<{w_cname}} "
        f"{'Chi phí (VNĐ)':>{w_cost}} "
        f"{'Click':>8} "
        f"{'Hiển thị':>10} "
        f"{'Chuyển đổi':>12}"
    )
    lines.append(header)
    lines.append("-" * width)

    total_cost = 0.0
    total_clicks = 0
    total_impr = 0
    total_conv = 0.0

    for r in rows:
        acc = (r.customer_name or r.customer_id)[: w_acc - 1]
        cname = (r.campaign_name or "-")[: w_cname - 1]
        lines.append(
            f"{acc:<{w_acc}} "
            f"{r.campaign_id:>10} "
            f"{cname:<{w_cname}} "
            f"{format_vnd_thousands(r.cost):>{w_cost}} "
            f"{r.clicks:>8,} "
            f"{r.impressions:>10,} "
            f"{r.conversions:>12,.2f}"
        )
        total_cost += r.cost
        total_clicks += r.clicks
        total_impr += r.impressions
        total_conv += r.conversions

    lines.append("-" * width)
    lines.append(
        f"{'TỔNG':<{w_acc + w_cid + w_cname + 2}} "
        f"{format_vnd_thousands(total_cost):>{w_cost}} "
        f"{total_clicks:>8,} "
        f"{total_impr:>10,} "
        f"{total_conv:>12,.2f}"
    )
    lines.append("=" * width)
    lines.append("")
    return "\n".join(lines)


def run_daily_report() -> None:
    project_root = Path(__file__).resolve().parent
    yaml_path = str(project_root / "google-ads.yaml")

    customer_ids = _resolve_customer_ids()
    if not customer_ids:
        print(
            "Thiếu danh sách tài khoản: set CLIENT_CUSTOMER_IDS hoặc chỉnh CUSTOMER_IDS trong bao_cao.py.",
            file=sys.stderr,
        )
        sys.exit(1)

    login_override = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or None
    client = load_google_ads_client(yaml_path, default_login_customer_id=login_override)

    report_date = _yesterday_label()
    rows = get_yesterday_campaign_performance(client, customer_ids)
    print(_format_table(rows, report_date))


def _seconds_until_next_local_time(hour: int, minute: int = 0) -> float:
    """Số giây tới lần chạy tiếp theo tại hour:minute theo giờ hệ thống."""
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def run_scheduler() -> None:
    """Chờ đến 6:00 sáng (giờ máy) mỗi ngày rồi chạy báo cáo."""
    print("Đã bật lịch: báo cáo lúc 06:00 sáng mỗi ngày (theo giờ hệ thống). Ctrl+C để dừng.")
    while True:
        wait_sec = _seconds_until_next_local_time(6, 0)
        wait_h = wait_sec / 3600.0
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Chờ {wait_h:.2f} giờ tới lần chạy tiếp theo...")
        time.sleep(wait_sec)
        try:
            run_daily_report()
        except (GoogleAdsHelperError, GoogleAdsException) as ex:
            print(f"Lỗi báo cáo: {ex}", file=sys.stderr)
        except Exception as ex:
            print(f"Lỗi không mong đợi: {ex}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Báo cáo Google Ads hàng ngày (ngày hôm qua).")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Chạy lặp: mỗi ngày lúc 6:00 sáng (giờ máy tính).",
    )
    args = parser.parse_args()

    if args.schedule:
        run_scheduler()
    else:
        try:
            run_daily_report()
        except GoogleAdsHelperError as ex:
            print(f"Lỗi: {ex}", file=sys.stderr)
            sys.exit(1)
        except GoogleAdsException as ex:
            print(f"Lỗi API: {ex.failure.errors[0].message if ex.failure.errors else ex}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
