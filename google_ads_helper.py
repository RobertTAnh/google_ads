from __future__ import annotations

import datetime
import json
import math
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.api_core import exceptions as google_api_exceptions
from google.protobuf.field_mask_pb2 import FieldMask


@dataclass(frozen=True)
class CustomerPerformanceRow:
    customer_id: str
    customer_name: str
    clicks: int
    impressions: int
    cost: float
    conversions: float


@dataclass(frozen=True)
class CampaignPerformanceRow:
    """Chỉ số ngày hôm qua theo từng chiến dịch (GAQL `FROM campaign`)."""

    customer_id: str
    customer_name: str
    campaign_id: str
    campaign_name: str
    clicks: int
    impressions: int
    cost: float
    conversions: float


@dataclass(frozen=True)
class CampaignMetadataRow:
    """Thông tin chiến dịch (không theo ngày): id, tên, trạng thái, loại kênh."""

    customer_id: str
    customer_name: str
    campaign_id: str
    campaign_name: str
    status: str
    advertising_channel_type: str


@dataclass(frozen=True)
class CampaignBiddingRow:
    """Mục tiêu bidding cấu hình trên chiến dịch (target CPA/ROAS), không phải CPA thực tế."""

    customer_id: str
    customer_name: str
    campaign_id: str
    campaign_name: str
    status: str
    bidding_strategy_type: str
    target_cpa: Optional[float]
    target_roas: Optional[float]
    bidding_strategy_resource: str


@dataclass(frozen=True)
class KeywordPerformanceRow:
    """Chỉ số ngày hôm qua theo từng từ khóa (GAQL `FROM keyword_view`)."""

    customer_id: str
    customer_name: str
    campaign_id: str
    campaign_name: str
    ad_group_id: str
    ad_group_name: str
    criterion_id: str
    keyword_text: str
    match_type: str
    clicks: int
    impressions: int
    cost: float
    conversions: float


# GAQL predefined date ranges (segments.date DURING …). Dùng cho MCP / báo cáo kỳ.
ALLOWED_MCP_DATE_RANGES: Tuple[str, ...] = ("YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS")
MCP_CUSTOM_DATE_MAX_DAYS_DEFAULT = 90
_MCP_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class McpDateFilter:
    """Kỳ báo cáo MCP: GAQL literal DURING … hoặc BETWEEN 'YYYY-MM-DD' AND '…'."""

    label: str
    gaql_predicate: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    is_custom: bool = False


@dataclass(frozen=True)
class CustomerPeriodMetricsRow:
    """Tổng metrics cấp tài khoản trong khoảng ngày GAQL (đã gộp nhiều ngày)."""

    customer_id: str
    customer_name: str
    date_range: str
    clicks: int
    impressions: int
    cost: float
    conversions: float
    cpa: Optional[float]


@dataclass(frozen=True)
class CampaignPeriodMetricsRow:
    """Metrics theo chiến dịch trong khoảng ngày (đã gộp theo campaign)."""

    customer_id: str
    customer_name: str
    campaign_id: str
    campaign_name: str
    date_range: str
    clicks: int
    impressions: int
    cost: float
    conversions: float
    cpa: Optional[float]


@dataclass(frozen=True)
class KeywordPeriodMetricsRow:
    """Keyword (keyword_view) đã gộp theo khoảng ngày."""

    customer_id: str
    customer_name: str
    campaign_id: str
    campaign_name: str
    ad_group_id: str
    ad_group_name: str
    criterion_id: str
    keyword_text: str
    match_type: str
    date_range: str
    clicks: int
    impressions: int
    cost: float
    conversions: float
    cpa: Optional[float]


@dataclass(frozen=True)
class SearchTermPeriodMetricsRow:
    """Cụm từ tìm kiếm thực tế (search_term_view), đã gộp theo khoảng ngày."""

    customer_id: str
    customer_name: str
    campaign_id: str
    campaign_name: str
    ad_group_id: str
    ad_group_name: str
    search_term: str
    date_range: str
    clicks: int
    impressions: int
    cost: float
    conversions: float
    cpa: Optional[float]


@dataclass(frozen=True)
class CampaignBudgetPeriodRow:
    """Campaign + ngân sách ngày (amount_micros) + metrics gộp trong kỳ — phục vụ CPA / pacing."""

    customer_id: str
    customer_name: str
    campaign_id: str
    campaign_name: str
    status: str
    date_range: str
    daily_budget: float
    clicks: int
    impressions: int
    cost: float
    conversions: float
    cpa: Optional[float]


@dataclass(frozen=True)
class AdPeriodMetricsRow:
    """Quảng cáo (ad_group_ad) — metrics gộp trong kỳ."""

    customer_id: str
    customer_name: str
    campaign_id: str
    campaign_name: str
    ad_group_id: str
    ad_group_name: str
    ad_id: str
    ad_name: str
    ad_type: str
    date_range: str
    clicks: int
    impressions: int
    cost: float
    conversions: float
    cpa: Optional[float]


@dataclass(frozen=True)
class NegativeKeywordRow:
    """Từ khóa phủ định (campaign hoặc ad group). Không gắn segments.date."""

    level: str  # "campaign" | "ad_group"
    customer_id: str
    campaign_id: str
    campaign_name: str
    ad_group_id: str
    ad_group_name: str
    criterion_id: str
    keyword_text: str
    match_type: str


@dataclass(frozen=True)
class AdGroupPeriodMetricsRow:
    """Nhóm quảng cáo — metrics gộp trong kỳ."""

    customer_id: str
    customer_name: str
    campaign_id: str
    campaign_name: str
    ad_group_id: str
    ad_group_name: str
    date_range: str
    clicks: int
    impressions: int
    cost: float
    conversions: float
    cpa: Optional[float]


@dataclass(frozen=True)
class KeywordQualityPeriodRow:
    """Quality score (lịch sử) theo keyword; lấy bản ghi mới nhất theo segments.date trong kỳ."""

    customer_id: str
    campaign_id: str
    campaign_name: str
    ad_group_id: str
    ad_group_name: str
    criterion_id: str
    keyword_text: str
    match_type: str
    date_range: str
    latest_segment_date: str
    historical_quality_score: str
    historical_creative_quality_score: str
    historical_landing_page_quality_score: str


@dataclass(frozen=True)
class AudiencePeriodMetricsRow:
    """Đối tượng (ad_group_audience_view) — metrics gộp kỳ."""

    customer_id: str
    campaign_id: str
    campaign_name: str
    ad_group_id: str
    ad_group_name: str
    criterion_id: str
    audience_display_name: str
    criterion_type: str
    date_range: str
    clicks: int
    impressions: int
    cost: float
    conversions: float
    cpa: Optional[float]


@dataclass(frozen=True)
class AssetPeriodMetricsRow:
    """Asset trong asset group (PMax / asset) — metrics gộp kỳ."""

    customer_id: str
    campaign_id: str
    campaign_name: str
    asset_group_id: str
    asset_group_name: str
    asset_id: str
    asset_name: str
    asset_type: str
    date_range: str
    clicks: int
    impressions: int
    cost: float
    conversions: float
    cpa: Optional[float]


@dataclass(frozen=True)
class ChangeEventRow:
    """Sự kiện thay đổi (change_event)."""

    change_date_time: str
    change_resource_type: str
    user_email: str
    client_type: str
    resource_name: str
    changed_fields: str


class GoogleAdsHelperError(RuntimeError):
    """App-friendly wrapper for surfacing Google Ads errors."""


def format_vnd_thousands(amount: Optional[float]) -> str:
    """
    Hiển thị chi phí dạng VND: làm tròn đến đồng, phân tách hàng nghìn bằng dấu chấm
    (mỗi nhóm sau dấu chấm đúng 3 chữ số, ví dụ 545.891 ₫).

    Giả định `amount` đã là đơn vị tiền của tài khoản (API trả đúng loại tiền billing).
    """
    v = int(round(float(amount or 0)))
    neg = v < 0
    v = abs(v)
    s = f"{v:,}".replace(",", ".")
    return f"{'-' if neg else ''}{s} ₫"


@dataclass(frozen=True)
class ChildAccount:
    customer_id: str
    customer_name: str
    is_manager: bool
    level: int
    status: str


def _format_googleads_exception(ex: GoogleAdsException) -> str:
    parts = [
        f"Request ID: {getattr(ex, 'request_id', 'unknown')}",
        f"Status: {getattr(ex, 'error', None) and ex.error.code().name}",
    ]
    for err in ex.failure.errors:
        msg = err.message
        loc = ""
        if err.location and err.location.field_path_elements:
            loc = " @ " + ".".join(e.field_name for e in err.location.field_path_elements)
        parts.append(f"- {msg}{loc}")
    return "\n".join(parts)


def normalize_mcp_date_range(raw: Optional[str]) -> str:
    """
    Chuẩn hóa date_range cho GAQL `segments.date DURING …`.
    Chỉ chấp nhận literal an toàn (chống injection vào query).
    """
    s = (raw or "YESTERDAY").strip().upper()
    if s not in ALLOWED_MCP_DATE_RANGES:
        allowed = ", ".join(ALLOWED_MCP_DATE_RANGES)
        raise GoogleAdsHelperError(f"date_range không hợp lệ: {raw!r}. Chọn một trong: {allowed}")
    return s


def _parse_mcp_iso_date(raw: str) -> datetime.date:
    s = (raw or "").strip()
    if not _MCP_ISO_DATE_RE.match(s):
        raise GoogleAdsHelperError(f"Ngày không hợp lệ: {raw!r}. Dùng định dạng YYYY-MM-DD.")
    try:
        return datetime.date.fromisoformat(s)
    except ValueError as e:
        raise GoogleAdsHelperError(f"Ngày không hợp lệ: {raw!r}.") from e


def mcp_custom_date_max_days() -> int:
    raw = (os.getenv("MCP_CUSTOM_DATE_MAX_DAYS") or str(MCP_CUSTOM_DATE_MAX_DAYS_DEFAULT)).strip()
    try:
        n = int(raw)
    except ValueError:
        n = MCP_CUSTOM_DATE_MAX_DAYS_DEFAULT
    return max(1, min(365, n))


def resolve_mcp_date_filter(
    *,
    date_range: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    max_custom_days: Optional[int] = None,
) -> McpDateFilter:
    """
    Ưu tiên start_date + end_date (GAQL BETWEEN). Nếu không có thì dùng date_range (GAQL DURING).
    """
    sd_raw = (start_date or "").strip()
    ed_raw = (end_date or "").strip()
    if sd_raw or ed_raw:
        if not sd_raw or not ed_raw:
            raise GoogleAdsHelperError("Cần cả start_date và end_date (YYYY-MM-DD) cho khoảng tùy chỉnh.")
        d0 = _parse_mcp_iso_date(sd_raw)
        d1 = _parse_mcp_iso_date(ed_raw)
        if d0 > d1:
            raise GoogleAdsHelperError("start_date phải nhỏ hơn hoặc bằng end_date.")
        cap = max_custom_days if max_custom_days is not None else mcp_custom_date_max_days()
        span_days = (d1 - d0).days + 1
        if span_days > cap:
            raise GoogleAdsHelperError(
                f"Khoảng ngày tối đa {cap} ngày (đang yêu cầu {span_days} ngày). "
                f"Đặt MCP_CUSTOM_DATE_MAX_DAYS trên server nếu cần tăng (tối đa 365)."
            )
        s0, s1 = d0.isoformat(), d1.isoformat()
        return McpDateFilter(
            label=f"{s0}..{s1}",
            gaql_predicate=f"BETWEEN '{s0}' AND '{s1}'",
            start_date=s0,
            end_date=s1,
            is_custom=True,
        )
    dr = normalize_mcp_date_range(date_range)
    return McpDateFilter(label=dr, gaql_predicate=f"DURING {dr}", is_custom=False)


def _cpa_from_cost_and_conversions(cost: float, conversions: float) -> Optional[float]:
    if conversions <= 0:
        return None
    return round(float(cost) / float(conversions), 6)


def _proto_enum_name(value: Any) -> str:
    if value is None:
        return ""
    return str(getattr(value, "name", value) or "")


def load_google_ads_client(
    yaml_path: str,
    *,
    default_login_customer_id: Optional[str] = None,
    api_version: str = "v23",
) -> GoogleAdsClient:
    """
    Loads Google Ads client configuration from a `google-ads.yaml` file (optional fallback if not using env JSON).
    - Ensure `login_customer_id` is set to your MCC customer ID so calls can access
      child accounts under your manager.
    """
    try:
        client = GoogleAdsClient.load_from_storage(path=yaml_path, version=api_version)
    except FileNotFoundError as e:
        raise GoogleAdsHelperError(
            f"Missing credentials file at '{yaml_path}'. Place 'google-ads.yaml' in the project root."
        ) from e
    except Exception as e:
        raise GoogleAdsHelperError(f"Failed to load Google Ads client: {e}") from e

    # Enforce/override login_customer_id when provided.
    # The python client uses this as the manager (MCC) context for child account calls.
    if default_login_customer_id:
        # Client stores config in an internal dict; setting attribute is supported.
        client.login_customer_id = str(default_login_customer_id)

    return client


def load_google_ads_client_from_dict(
    config_data: Dict[str, Any],
    *,
    api_version: str = "v23",
) -> GoogleAdsClient:
    """
    Loads Google Ads client configuration from an in-memory dict.
    Useful for multi-MCC setups where each MCC credentials live in env JSON.
    """
    try:
        return GoogleAdsClient.load_from_dict(config_data, version=api_version)
    except Exception as e:
        raise GoogleAdsHelperError(f"Failed to load Google Ads client from env config: {e}") from e


def normalize_google_ads_customer_id(raw: str) -> str:
    """
    Google Ads customer IDs are 10 digits. Accepts "240-746-9372", "2407469372",
    or labels with a parenthetical "(3787956462)".
    """
    s = (raw or "").strip()
    if not s:
        return ""
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


def read_login_customer_id_from_yaml(yaml_path: str) -> str:
    try:
        with open(yaml_path, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if stripped.startswith("login_customer_id"):
                    _, value = stripped.split(":", 1)
                    return normalize_google_ads_customer_id(value.strip().strip("'\""))
    except OSError:
        return ""
    return ""


def google_ads_shared_oauth_defaults_from_env() -> Dict[str, str]:
    """Optional env fallbacks merged into GOOGLE_ADS_MCC_CONFIGS JSON (shared block or per-MCC gaps)."""
    return {
        "developer_token": (os.getenv("GOOGLE_ADS_SHARED_DEVELOPER_TOKEN") or "").strip(),
        "client_id": (os.getenv("GOOGLE_ADS_SHARED_CLIENT_ID") or "").strip(),
        "client_secret": (os.getenv("GOOGLE_ADS_SHARED_CLIENT_SECRET") or "").strip(),
        "refresh_token": (os.getenv("GOOGLE_ADS_SHARED_REFRESH_TOKEN") or "").strip(),
    }


def load_google_ads_mcc_configs_from_env(
    shared_defaults: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, str]]:
    """
    Parse GOOGLE_ADS_MCC_CONFIGS (JSON). Supports:
    - flat map keyed by MCC id,
    - {"shared": {...}, "mccs": {...}},
    - {"mccs": {...}} only (production: one full credential set per MCC).
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
    if shared_defaults is None:
        shared = dict(google_ads_shared_oauth_defaults_from_env())
    else:
        shared = dict(shared_defaults)
    if isinstance(parsed.get("shared"), dict):
        for key in ("developer_token", "client_id", "client_secret", "refresh_token"):
            v = str(parsed["shared"].get(key, "")).strip()
            if v:
                shared[key] = v
    mcc_items: Dict[str, Any] = parsed
    if isinstance(parsed.get("mccs"), dict):
        mcc_items = parsed["mccs"]

    out: Dict[str, Dict[str, str]] = {}
    for raw_key, cfg in mcc_items.items():
        if not isinstance(cfg, dict):
            continue
        mcc_id = normalize_google_ads_customer_id(
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


def mcc_google_ads_credentials_complete(cfg: Dict[str, str]) -> bool:
    required = ("developer_token", "client_id", "client_secret", "refresh_token", "mcc_id")
    return all(str(cfg.get(k, "")).strip() for k in required)


def build_google_ads_client_for_mcc_id(
    mcc_customer_id: str,
    mcc_configs: Dict[str, Dict[str, str]],
    *,
    yaml_path: str,
    yaml_default_login_customer_id: Optional[str] = None,
    api_version: str = "v23",
) -> GoogleAdsClient:
    """
    Prefer env `mcc_configs[mcc_id]` (full credentials per MCC).
    Else optional `google-ads.yaml` at yaml_path if the file exists (or bootstrap via GOOGLE_ADS_YAML_* on deploy).
    """
    mcc_id = normalize_google_ads_customer_id(mcc_customer_id)
    cfg = mcc_configs.get(mcc_id) if mcc_id else None
    if cfg and mcc_google_ads_credentials_complete(cfg):
        conf_dict: Dict[str, Any] = {
            "developer_token": cfg["developer_token"],
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "refresh_token": cfg["refresh_token"],
            "login_customer_id": mcc_id,
            "use_proto_plus": True,
        }
        return load_google_ads_client_from_dict(conf_dict, api_version=api_version)
    if yaml_path and os.path.isfile(yaml_path):
        return load_google_ads_client(
            yaml_path,
            default_login_customer_id=mcc_id or yaml_default_login_customer_id,
            api_version=api_version,
        )
    raise GoogleAdsHelperError(
        "Thiếu credential Google Ads: thêm MCC này vào GOOGLE_ADS_MCC_CONFIGS (đủ developer_token + OAuth), "
        "hoặc đặt file google-ads.yaml / biến GOOGLE_ADS_YAML_B64 hoặc GOOGLE_ADS_YAML_TEXT."
    )


def list_accessible_customer_ids(client: GoogleAdsClient) -> List[str]:
    """
    Google-supported pattern: `CustomerService.list_accessible_customers`.

    Returns numeric customer IDs for every Google Ads account the authenticated user can
    access. Resource names look like ``customers/1234567890``; we return only the ID part.
    """
    customer_service = client.get_service("CustomerService")
    try:
        resp = customer_service.list_accessible_customers()
    except GoogleAdsException as ex:
        raise GoogleAdsHelperError(
            f"Failed to list accessible customers:\n{_format_googleads_exception(ex)}"
        ) from ex

    out: List[str] = []
    for resource_name in resp.resource_names:
        # "customers/1234567890"
        prefix = "customers/"
        s = str(resource_name)
        if s.startswith(prefix):
            out.append(s[len(prefix) :])
        else:
            out.append(s)
    return sorted(set(out))


def get_customer_name(client: GoogleAdsClient, customer_id: str) -> str:
    """
    Fetches a single account descriptive name.

    GAQL notes:
    - `FROM customer` without date segments returns account info (non-metrics).
    """
    ga_service = client.get_service("GoogleAdsService")
    customer_id = str(customer_id).strip().replace("-", "")
    query = """
        SELECT
          customer.descriptive_name
        FROM customer
        LIMIT 1
    """.strip()
    try:
        resp = ga_service.search(customer_id=customer_id, query=query)
        for row in resp:
            return str(row.customer.descriptive_name or "")
        return ""
    except GoogleAdsException as ex:
        raise GoogleAdsHelperError(f"Failed to fetch customer name:\n{_format_googleads_exception(ex)}") from ex


def list_child_accounts_under_mcc(client: GoogleAdsClient, mcc_customer_id: str) -> List[ChildAccount]:
    """
    Lists client accounts in the hierarchy under an MCC (GAQL `customer_client`).

    GAQL notes:
    - `FROM customer_client` with `customer_id = <MCC_ID>` returns the tree under that manager.
    - `customer_client.level` is 0 for the manager row; we use `level >= 1` to skip it.
    """
    ga_service = client.get_service("GoogleAdsService")
    mcc_customer_id = str(mcc_customer_id).strip().replace("-", "")
    query = """
        SELECT
          customer_client.id,
          customer_client.descriptive_name,
          customer_client.manager,
          customer_client.status,
          customer_client.level
        FROM customer_client
        WHERE customer_client.level >= 1
    """.strip()

    out: List[ChildAccount] = []
    try:
        stream = ga_service.search_stream(customer_id=mcc_customer_id, query=query)
        for batch in stream:
            for r in batch.results:
                status_name = ""
                if r.customer_client.status:
                    status_name = r.customer_client.status.name
                out.append(
                    ChildAccount(
                        customer_id=str(r.customer_client.id),
                        customer_name=str(r.customer_client.descriptive_name or ""),
                        is_manager=bool(r.customer_client.manager),
                        level=int(r.customer_client.level or 0),
                        status=status_name,
                    )
                )
        out.sort(
            key=lambda x: (x.level, not x.is_manager, (x.customer_name or "").lower(), x.customer_id)
        )
        return out
    except GoogleAdsException as ex:
        raise GoogleAdsHelperError(f"Failed to list child accounts:\n{_format_googleads_exception(ex)}") from ex


def get_customer_metrics_for_date_range(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
    date_range: str = "YESTERDAY",
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[CustomerPeriodMetricsRow]:
    """Gộp metrics cấp tài khoản theo GAQL segments.date (DURING hoặc BETWEEN)."""
    df = resolve_mcp_date_filter(date_range=date_range, start_date=start_date, end_date=end_date)
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          customer.id,
          customer.descriptive_name,
          metrics.clicks,
          metrics.impressions,
          metrics.cost_micros,
          metrics.conversions
        FROM customer
        WHERE segments.date {df.gaql_predicate}
    """.strip()

    out: List[CustomerPeriodMetricsRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        clicks = impressions = 0
        cost_micros = 0
        conversions = 0.0
        name = ""
        found = False
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    found = True
                    name = str(r.customer.descriptive_name or "")
                    clicks += int(r.metrics.clicks or 0)
                    impressions += int(r.metrics.impressions or 0)
                    cost_micros += int(r.metrics.cost_micros or 0)
                    conversions += float(r.metrics.conversions or 0.0)
            cost = cost_micros / 1_000_000.0
            if not found:
                out.append(
                    CustomerPeriodMetricsRow(
                        customer_id=cid,
                        customer_name="",
                        date_range=df.label,
                        clicks=0,
                        impressions=0,
                        cost=0.0,
                        conversions=0.0,
                        cpa=None,
                    )
                )
            else:
                out.append(
                    CustomerPeriodMetricsRow(
                        customer_id=str(cid),
                        customer_name=name,
                        date_range=df.label,
                        clicks=clicks,
                        impressions=impressions,
                        cost=float(cost),
                        conversions=float(conversions),
                        cpa=_cpa_from_cost_and_conversions(cost, conversions),
                    )
                )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(f"Google Ads API error for customer {cid}:\n{_format_googleads_exception(ex)}") from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex
    return out


def get_campaign_metrics_for_date_range(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
    date_range: str = "YESTERDAY",
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[CampaignPeriodMetricsRow]:
    """Gộp metrics theo campaign trong kỳ (ENABLED + PAUSED)."""
    df = resolve_mcp_date_filter(date_range=date_range, start_date=start_date, end_date=end_date)
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          customer.id,
          customer.descriptive_name,
          campaign.id,
          campaign.name,
          metrics.clicks,
          metrics.impressions,
          metrics.cost_micros,
          metrics.conversions
        FROM campaign
        WHERE segments.date {df.gaql_predicate}
          AND campaign.status IN (ENABLED, PAUSED)
    """.strip()

    out: List[CampaignPeriodMetricsRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        acc: Dict[str, Dict[str, Any]] = {}
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    cap_id = str(r.campaign.id)
                    if cap_id not in acc:
                        acc[cap_id] = {
                            "customer_id": str(r.customer.id),
                            "customer_name": str(r.customer.descriptive_name or ""),
                            "campaign_name": str(r.campaign.name or ""),
                            "clicks": 0,
                            "impressions": 0,
                            "cost_micros": 0,
                            "conversions": 0.0,
                        }
                    a = acc[cap_id]
                    a["clicks"] += int(r.metrics.clicks or 0)
                    a["impressions"] += int(r.metrics.impressions or 0)
                    a["cost_micros"] += int(r.metrics.cost_micros or 0)
                    a["conversions"] += float(r.metrics.conversions or 0.0)
            for cap_id, a in acc.items():
                cost = a["cost_micros"] / 1_000_000.0
                conv = float(a["conversions"])
                out.append(
                    CampaignPeriodMetricsRow(
                        customer_id=a["customer_id"],
                        customer_name=a["customer_name"],
                        campaign_id=cap_id,
                        campaign_name=a["campaign_name"],
                        date_range=df.label,
                        clicks=int(a["clicks"]),
                        impressions=int(a["impressions"]),
                        cost=float(cost),
                        conversions=conv,
                        cpa=_cpa_from_cost_and_conversions(cost, conv),
                    )
                )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error for customer {cid}:\n{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex

    out.sort(
        key=lambda x: (
            (x.customer_name or "").lower(),
            (x.campaign_name or "").lower(),
            x.campaign_id,
        )
    )
    return out


def get_keyword_metrics_for_date_range(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
    date_range: str = "YESTERDAY",
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit_per_customer: int = 500,
) -> List[KeywordPeriodMetricsRow]:
    """Gộp keyword_view theo kỳ; trả top `limit_per_customer` theo cost sau khi gộp."""
    df = resolve_mcp_date_filter(date_range=date_range, start_date=start_date, end_date=end_date)
    if limit_per_customer < 1:
        limit_per_customer = 1
    if limit_per_customer > 5000:
        limit_per_customer = 5000

    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          customer.id,
          customer.descriptive_name,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          ad_group_criterion.criterion_id,
          ad_group_criterion.keyword.text,
          ad_group_criterion.keyword.match_type,
          metrics.clicks,
          metrics.impressions,
          metrics.cost_micros,
          metrics.conversions
        FROM keyword_view
        WHERE segments.date {df.gaql_predicate}
    """.strip()

    out: List[KeywordPeriodMetricsRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        acc: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    cap_id = str(r.campaign.id)
                    ag_id = str(r.ad_group.id)
                    crit_id = str(r.ad_group_criterion.criterion_id)
                    key = (cap_id, ag_id, crit_id)
                    mt = ""
                    if r.ad_group_criterion.keyword.match_type:
                        mt = r.ad_group_criterion.keyword.match_type.name
                    if key not in acc:
                        acc[key] = {
                            "customer_id": str(r.customer.id),
                            "customer_name": str(r.customer.descriptive_name or ""),
                            "campaign_name": str(r.campaign.name or ""),
                            "ad_group_name": str(r.ad_group.name or ""),
                            "keyword_text": str(r.ad_group_criterion.keyword.text or ""),
                            "match_type": mt,
                            "clicks": 0,
                            "impressions": 0,
                            "cost_micros": 0,
                            "conversions": 0.0,
                        }
                    a = acc[key]
                    a["clicks"] += int(r.metrics.clicks or 0)
                    a["impressions"] += int(r.metrics.impressions or 0)
                    a["cost_micros"] += int(r.metrics.cost_micros or 0)
                    a["conversions"] += float(r.metrics.conversions or 0.0)
            ranked = sorted(acc.items(), key=lambda kv: kv[1]["cost_micros"], reverse=True)[:limit_per_customer]
            for (cap_id, ag_id, crit_id), a in ranked:
                cost = a["cost_micros"] / 1_000_000.0
                conv = float(a["conversions"])
                out.append(
                    KeywordPeriodMetricsRow(
                        customer_id=a["customer_id"],
                        customer_name=a["customer_name"],
                        campaign_id=cap_id,
                        campaign_name=a["campaign_name"],
                        ad_group_id=ag_id,
                        ad_group_name=a["ad_group_name"],
                        criterion_id=crit_id,
                        keyword_text=a["keyword_text"],
                        match_type=a["match_type"],
                        date_range=df.label,
                        clicks=int(a["clicks"]),
                        impressions=int(a["impressions"]),
                        cost=float(cost),
                        conversions=conv,
                        cpa=_cpa_from_cost_and_conversions(cost, conv),
                    )
                )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error for keyword_view customer {cid}:\n{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex

    out.sort(
        key=lambda x: (
            (x.customer_name or "").lower(),
            (x.campaign_name or "").lower(),
            (x.keyword_text or "").lower(),
        )
    )
    return out


def get_search_term_metrics_for_date_range(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
    date_range: str = "YESTERDAY",
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit_per_customer: int = 400,
) -> List[SearchTermPeriodMetricsRow]:
    """Cụm từ tìm kiếm thực tế (search_term_view), gộp theo kỳ, top theo cost."""
    df = resolve_mcp_date_filter(date_range=date_range, start_date=start_date, end_date=end_date)
    if limit_per_customer < 1:
        limit_per_customer = 1
    if limit_per_customer > 5000:
        limit_per_customer = 5000

    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          customer.id,
          customer.descriptive_name,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          search_term_view.search_term,
          metrics.clicks,
          metrics.impressions,
          metrics.cost_micros,
          metrics.conversions
        FROM search_term_view
        WHERE segments.date {df.gaql_predicate}
    """.strip()

    out: List[SearchTermPeriodMetricsRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        acc: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    cap_id = str(r.campaign.id)
                    ag_id = str(r.ad_group.id)
                    term = str(r.search_term_view.search_term or "")
                    key = (cap_id, ag_id, term)
                    if key not in acc:
                        acc[key] = {
                            "customer_id": str(r.customer.id),
                            "customer_name": str(r.customer.descriptive_name or ""),
                            "campaign_name": str(r.campaign.name or ""),
                            "ad_group_name": str(r.ad_group.name or ""),
                            "clicks": 0,
                            "impressions": 0,
                            "cost_micros": 0,
                            "conversions": 0.0,
                        }
                    a = acc[key]
                    a["clicks"] += int(r.metrics.clicks or 0)
                    a["impressions"] += int(r.metrics.impressions or 0)
                    a["cost_micros"] += int(r.metrics.cost_micros or 0)
                    a["conversions"] += float(r.metrics.conversions or 0.0)
            ranked = sorted(acc.items(), key=lambda kv: kv[1]["cost_micros"], reverse=True)[:limit_per_customer]
            for (cap_id, ag_id, term), a in ranked:
                cost = a["cost_micros"] / 1_000_000.0
                conv = float(a["conversions"])
                out.append(
                    SearchTermPeriodMetricsRow(
                        customer_id=a["customer_id"],
                        customer_name=a["customer_name"],
                        campaign_id=cap_id,
                        campaign_name=a["campaign_name"],
                        ad_group_id=ag_id,
                        ad_group_name=a["ad_group_name"],
                        search_term=term,
                        date_range=df.label,
                        clicks=int(a["clicks"]),
                        impressions=int(a["impressions"]),
                        cost=float(cost),
                        conversions=conv,
                        cpa=_cpa_from_cost_and_conversions(cost, conv),
                    )
                )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error for search_term_view customer {cid}:\n{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex

    out.sort(
        key=lambda x: (
            (x.customer_name or "").lower(),
            (x.campaign_name or "").lower(),
            (x.search_term or "").lower(),
        )
    )
    return out


def get_campaign_budget_metrics_for_date_range(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
    date_range: str = "YESTERDAY",
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[CampaignBudgetPeriodRow]:
    """Campaign + ngân sách ngày (amount_micros) + metrics gộp trong kỳ (CPA = cost / conv)."""
    df = resolve_mcp_date_filter(date_range=date_range, start_date=start_date, end_date=end_date)
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          customer.id,
          customer.descriptive_name,
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.campaign_budget,
          campaign_budget.amount_micros,
          metrics.clicks,
          metrics.impressions,
          metrics.cost_micros,
          metrics.conversions
        FROM campaign
        WHERE segments.date {df.gaql_predicate}
          AND campaign.status IN (ENABLED, PAUSED)
    """.strip()

    out: List[CampaignBudgetPeriodRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        acc: Dict[str, Dict[str, Any]] = {}
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    cap_id = str(r.campaign.id)
                    st = r.campaign.status.name if r.campaign.status else ""
                    budget_micros = int(r.campaign_budget.amount_micros or 0)
                    if cap_id not in acc:
                        acc[cap_id] = {
                            "customer_id": str(r.customer.id),
                            "customer_name": str(r.customer.descriptive_name or ""),
                            "campaign_name": str(r.campaign.name or ""),
                            "status": st,
                            "budget_micros_max": budget_micros,
                            "clicks": 0,
                            "impressions": 0,
                            "cost_micros": 0,
                            "conversions": 0.0,
                        }
                    a = acc[cap_id]
                    a["clicks"] += int(r.metrics.clicks or 0)
                    a["impressions"] += int(r.metrics.impressions or 0)
                    a["cost_micros"] += int(r.metrics.cost_micros or 0)
                    a["conversions"] += float(r.metrics.conversions or 0.0)
                    if budget_micros > int(a["budget_micros_max"]):
                        a["budget_micros_max"] = budget_micros
            for cap_id, a in acc.items():
                cost = a["cost_micros"] / 1_000_000.0
                conv = float(a["conversions"])
                daily_budget = int(a["budget_micros_max"]) / 1_000_000.0
                out.append(
                    CampaignBudgetPeriodRow(
                        customer_id=a["customer_id"],
                        customer_name=a["customer_name"],
                        campaign_id=cap_id,
                        campaign_name=a["campaign_name"],
                        status=str(a["status"]),
                        date_range=df.label,
                        daily_budget=float(daily_budget),
                        clicks=int(a["clicks"]),
                        impressions=int(a["impressions"]),
                        cost=float(cost),
                        conversions=conv,
                        cpa=_cpa_from_cost_and_conversions(cost, conv),
                    )
                )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error (campaign budget metrics) for customer {cid}:\n{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex

    out.sort(
        key=lambda x: (
            (x.customer_name or "").lower(),
            (x.campaign_name or "").lower(),
            x.campaign_id,
        )
    )
    return out


def list_negative_keywords_for_customer(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
) -> List[NegativeKeywordRow]:
    """
    Danh sách từ khóa phủ định hiện tại (campaign + ad group).
    Không dùng segments.date — cấu hình hiện hành.
    """
    ga_service = client.get_service("GoogleAdsService")
    q_ag = """
        SELECT
          customer.id,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          ad_group_criterion.criterion_id,
          ad_group_criterion.keyword.text,
          ad_group_criterion.keyword.match_type
        FROM ad_group_criterion
        WHERE ad_group_criterion.type = KEYWORD
          AND ad_group_criterion.negative = TRUE
    """.strip()
    q_c = """
        SELECT
          customer.id,
          campaign.id,
          campaign.name,
          campaign_criterion.criterion_id,
          campaign_criterion.keyword.text,
          campaign_criterion.keyword.match_type
        FROM campaign_criterion
        WHERE campaign_criterion.type = KEYWORD
          AND campaign_criterion.negative = TRUE
    """.strip()

    out: List[NegativeKeywordRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        try:
            stream = ga_service.search_stream(customer_id=cid, query=q_ag)
            for batch in stream:
                for r in batch.results:
                    out.append(
                        NegativeKeywordRow(
                            level="ad_group",
                            customer_id=str(r.customer.id),
                            campaign_id=str(r.campaign.id),
                            campaign_name=str(r.campaign.name or ""),
                            ad_group_id=str(r.ad_group.id),
                            ad_group_name=str(r.ad_group.name or ""),
                            criterion_id=str(r.ad_group_criterion.criterion_id),
                            keyword_text=str(r.ad_group_criterion.keyword.text or ""),
                            match_type=_proto_enum_name(getattr(r.ad_group_criterion.keyword, "match_type", None)),
                        )
                    )
            stream2 = ga_service.search_stream(customer_id=cid, query=q_c)
            for batch in stream2:
                for r in batch.results:
                    out.append(
                        NegativeKeywordRow(
                            level="campaign",
                            customer_id=str(r.customer.id),
                            campaign_id=str(r.campaign.id),
                            campaign_name=str(r.campaign.name or ""),
                            ad_group_id="",
                            ad_group_name="",
                            criterion_id=str(r.campaign_criterion.criterion_id),
                            keyword_text=str(r.campaign_criterion.keyword.text or ""),
                            match_type=_proto_enum_name(getattr(r.campaign_criterion.keyword, "match_type", None)),
                        )
                    )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error listing negative keywords for {cid}:\n{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex

    out.sort(
        key=lambda x: (
            x.level,
            (x.campaign_name or "").lower(),
            (x.ad_group_name or "").lower(),
            (x.keyword_text or "").lower(),
        )
    )
    return out


def get_ad_group_metrics_for_date_range(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
    date_range: str = "YESTERDAY",
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[AdGroupPeriodMetricsRow]:
    df = resolve_mcp_date_filter(date_range=date_range, start_date=start_date, end_date=end_date)
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          customer.id,
          customer.descriptive_name,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          metrics.clicks,
          metrics.impressions,
          metrics.cost_micros,
          metrics.conversions
        FROM ad_group
        WHERE segments.date {df.gaql_predicate}
    """.strip()

    out: List[AdGroupPeriodMetricsRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        acc: Dict[str, Dict[str, Any]] = {}
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    ag_id = str(r.ad_group.id)
                    if ag_id not in acc:
                        acc[ag_id] = {
                            "customer_id": str(r.customer.id),
                            "customer_name": str(r.customer.descriptive_name or ""),
                            "campaign_id": str(r.campaign.id),
                            "campaign_name": str(r.campaign.name or ""),
                            "ad_group_name": str(r.ad_group.name or ""),
                            "clicks": 0,
                            "impressions": 0,
                            "cost_micros": 0,
                            "conversions": 0.0,
                        }
                    a = acc[ag_id]
                    a["clicks"] += int(r.metrics.clicks or 0)
                    a["impressions"] += int(r.metrics.impressions or 0)
                    a["cost_micros"] += int(r.metrics.cost_micros or 0)
                    a["conversions"] += float(r.metrics.conversions or 0.0)
            for ag_id, a in acc.items():
                cost = a["cost_micros"] / 1_000_000.0
                conv = float(a["conversions"])
                out.append(
                    AdGroupPeriodMetricsRow(
                        customer_id=a["customer_id"],
                        customer_name=a["customer_name"],
                        campaign_id=a["campaign_id"],
                        campaign_name=a["campaign_name"],
                        ad_group_id=ag_id,
                        ad_group_name=a["ad_group_name"],
                        date_range=df.label,
                        clicks=int(a["clicks"]),
                        impressions=int(a["impressions"]),
                        cost=float(cost),
                        conversions=conv,
                        cpa=_cpa_from_cost_and_conversions(cost, conv),
                    )
                )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error (ad_group) for customer {cid}:\n{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex

    out.sort(
        key=lambda x: (
            (x.campaign_name or "").lower(),
            (x.ad_group_name or "").lower(),
            x.ad_group_id,
        )
    )
    return out


def get_ad_performance_for_date_range(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
    date_range: str = "YESTERDAY",
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit_per_customer: int = 200,
) -> List[AdPeriodMetricsRow]:
    """Metrics theo từng ad (ad_group_ad), gộp kỳ; top theo cost."""
    df = resolve_mcp_date_filter(date_range=date_range, start_date=start_date, end_date=end_date)
    if limit_per_customer < 1:
        limit_per_customer = 1
    if limit_per_customer > 2000:
        limit_per_customer = 2000

    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          customer.id,
          customer.descriptive_name,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          ad_group_ad.ad.id,
          ad_group_ad.ad.name,
          ad_group_ad.ad.type,
          metrics.clicks,
          metrics.impressions,
          metrics.cost_micros,
          metrics.conversions
        FROM ad_group_ad
        WHERE segments.date {df.gaql_predicate}
          AND ad_group_ad.status IN (ENABLED, PAUSED)
    """.strip()

    out: List[AdPeriodMetricsRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        acc: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    cap_id = str(r.campaign.id)
                    ag_id = str(r.ad_group.id)
                    ad = r.ad_group_ad.ad
                    ad_id = str(ad.id) if getattr(ad, "id", None) else ""
                    key = (cap_id, ag_id, ad_id)
                    ad_name = str(getattr(ad, "name", "") or "")
                    ad_type = _proto_enum_name(getattr(ad, "type_", None))
                    if key not in acc:
                        acc[key] = {
                            "customer_id": str(r.customer.id),
                            "customer_name": str(r.customer.descriptive_name or ""),
                            "campaign_name": str(r.campaign.name or ""),
                            "ad_group_name": str(r.ad_group.name or ""),
                            "ad_name": ad_name,
                            "ad_type": ad_type,
                            "clicks": 0,
                            "impressions": 0,
                            "cost_micros": 0,
                            "conversions": 0.0,
                        }
                    a = acc[key]
                    a["clicks"] += int(r.metrics.clicks or 0)
                    a["impressions"] += int(r.metrics.impressions or 0)
                    a["cost_micros"] += int(r.metrics.cost_micros or 0)
                    a["conversions"] += float(r.metrics.conversions or 0.0)
            ranked = sorted(acc.items(), key=lambda kv: kv[1]["cost_micros"], reverse=True)[:limit_per_customer]
            for (cap_id, ag_id, ad_id), a in ranked:
                cost = a["cost_micros"] / 1_000_000.0
                conv = float(a["conversions"])
                out.append(
                    AdPeriodMetricsRow(
                        customer_id=a["customer_id"],
                        customer_name=a["customer_name"],
                        campaign_id=cap_id,
                        campaign_name=a["campaign_name"],
                        ad_group_id=ag_id,
                        ad_group_name=a["ad_group_name"],
                        ad_id=ad_id,
                        ad_name=a["ad_name"],
                        ad_type=a["ad_type"],
                        date_range=df.label,
                        clicks=int(a["clicks"]),
                        impressions=int(a["impressions"]),
                        cost=float(cost),
                        conversions=conv,
                        cpa=_cpa_from_cost_and_conversions(cost, conv),
                    )
                )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error (ad_group_ad) for customer {cid}:\n{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex

    out.sort(key=lambda x: ((x.campaign_name or "").lower(), (x.ad_group_name or "").lower(), x.ad_id))
    return out


def get_keyword_quality_scores_for_date_range(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
    date_range: str = "YESTERDAY",
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[KeywordQualityPeriodRow]:
    """
    Quality score lịch sử (keyword_view + segments.date).
    Với mỗi keyword lấy bản ghi có segments.date mới nhất trong kỳ.
    """
    df = resolve_mcp_date_filter(date_range=date_range, start_date=start_date, end_date=end_date)
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          segments.date,
          customer.id,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          ad_group_criterion.criterion_id,
          ad_group_criterion.keyword.text,
          ad_group_criterion.keyword.match_type,
          metrics.historical_quality_score,
          metrics.historical_creative_quality_score,
          metrics.historical_landing_page_quality_score
        FROM keyword_view
        WHERE segments.date {df.gaql_predicate}
    """.strip()

    out: List[KeywordQualityPeriodRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        best: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    cap_id = str(r.campaign.id)
                    ag_id = str(r.ad_group.id)
                    crit_id = str(r.ad_group_criterion.criterion_id)
                    key = (cap_id, ag_id, crit_id)
                    seg_date = str(r.segments.date) if r.segments.date else ""
                    prev = best.get(key)
                    if prev is None or seg_date > prev["segment_date"]:
                        best[key] = {
                            "segment_date": seg_date,
                            "customer_id": str(r.customer.id),
                            "campaign_name": str(r.campaign.name or ""),
                            "ad_group_name": str(r.ad_group.name or ""),
                            "keyword_text": str(r.ad_group_criterion.keyword.text or ""),
                            "match_type": _proto_enum_name(getattr(r.ad_group_criterion.keyword, "match_type", None)),
                            "hq": _proto_enum_name(getattr(r.metrics, "historical_quality_score", None)),
                            "hc": _proto_enum_name(getattr(r.metrics, "historical_creative_quality_score", None)),
                            "hl": _proto_enum_name(getattr(r.metrics, "historical_landing_page_quality_score", None)),
                        }
            for (cap_id, ag_id, crit_id), b in best.items():
                out.append(
                    KeywordQualityPeriodRow(
                        customer_id=b["customer_id"],
                        campaign_id=cap_id,
                        campaign_name=b["campaign_name"],
                        ad_group_id=ag_id,
                        ad_group_name=b["ad_group_name"],
                        criterion_id=crit_id,
                        keyword_text=b["keyword_text"],
                        match_type=b["match_type"],
                        date_range=df.label,
                        latest_segment_date=b["segment_date"],
                        historical_quality_score=b["hq"],
                        historical_creative_quality_score=b["hc"],
                        historical_landing_page_quality_score=b["hl"],
                    )
                )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error (keyword quality) for customer {cid}:\n{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex

    out.sort(
        key=lambda x: (
            (x.campaign_name or "").lower(),
            (x.ad_group_name or "").lower(),
            (x.keyword_text or "").lower(),
        )
    )
    return out


def get_audience_performance_for_date_range(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
    date_range: str = "YESTERDAY",
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit_per_customer: int = 300,
) -> List[AudiencePeriodMetricsRow]:
    df = resolve_mcp_date_filter(date_range=date_range, start_date=start_date, end_date=end_date)
    if limit_per_customer < 1:
        limit_per_customer = 1
    if limit_per_customer > 2000:
        limit_per_customer = 2000

    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          customer.id,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          ad_group_criterion.criterion_id,
          ad_group_criterion.display_name,
          ad_group_criterion.type,
          metrics.clicks,
          metrics.impressions,
          metrics.cost_micros,
          metrics.conversions
        FROM ad_group_audience_view
        WHERE segments.date {df.gaql_predicate}
    """.strip()

    out: List[AudiencePeriodMetricsRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        acc: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    cap_id = str(r.campaign.id)
                    ag_id = str(r.ad_group.id)
                    crit_id = str(r.ad_group_criterion.criterion_id)
                    key = (cap_id, ag_id, crit_id)
                    if key not in acc:
                        acc[key] = {
                            "customer_id": str(r.customer.id),
                            "campaign_name": str(r.campaign.name or ""),
                            "ad_group_name": str(r.ad_group.name or ""),
                            "display_name": str(r.ad_group_criterion.display_name or ""),
                            "crit_type": _proto_enum_name(getattr(r.ad_group_criterion, "type_", None)),
                            "clicks": 0,
                            "impressions": 0,
                            "cost_micros": 0,
                            "conversions": 0.0,
                        }
                    a = acc[key]
                    a["clicks"] += int(r.metrics.clicks or 0)
                    a["impressions"] += int(r.metrics.impressions or 0)
                    a["cost_micros"] += int(r.metrics.cost_micros or 0)
                    a["conversions"] += float(r.metrics.conversions or 0.0)
            ranked = sorted(acc.items(), key=lambda kv: kv[1]["cost_micros"], reverse=True)[:limit_per_customer]
            for (cap_id, ag_id, crit_id), a in ranked:
                cost = a["cost_micros"] / 1_000_000.0
                conv = float(a["conversions"])
                out.append(
                    AudiencePeriodMetricsRow(
                        customer_id=a["customer_id"],
                        campaign_id=cap_id,
                        campaign_name=a["campaign_name"],
                        ad_group_id=ag_id,
                        ad_group_name=a["ad_group_name"],
                        criterion_id=crit_id,
                        audience_display_name=a["display_name"],
                        criterion_type=a["crit_type"],
                        date_range=df.label,
                        clicks=int(a["clicks"]),
                        impressions=int(a["impressions"]),
                        cost=float(cost),
                        conversions=conv,
                        cpa=_cpa_from_cost_and_conversions(cost, conv),
                    )
                )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error (ad_group_audience_view) for customer {cid}:\n{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex

    out.sort(key=lambda x: ((x.campaign_name or "").lower(), (x.audience_display_name or "").lower()))
    return out


def get_asset_performance_for_date_range(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
    date_range: str = "YESTERDAY",
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit_per_customer: int = 300,
) -> List[AssetPeriodMetricsRow]:
    df = resolve_mcp_date_filter(date_range=date_range, start_date=start_date, end_date=end_date)
    if limit_per_customer < 1:
        limit_per_customer = 1
    if limit_per_customer > 2000:
        limit_per_customer = 2000

    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          customer.id,
          campaign.id,
          campaign.name,
          asset_group.id,
          asset_group.name,
          asset.id,
          asset.name,
          asset.type,
          metrics.clicks,
          metrics.impressions,
          metrics.cost_micros,
          metrics.conversions
        FROM asset_group_asset
        WHERE segments.date {df.gaql_predicate}
    """.strip()

    out: List[AssetPeriodMetricsRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        acc: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    cap_id = str(r.campaign.id)
                    agroup_id = str(r.asset_group.id)
                    asset_id = str(r.asset.id)
                    key = (cap_id, agroup_id, asset_id)
                    if key not in acc:
                        acc[key] = {
                            "customer_id": str(r.customer.id),
                            "campaign_name": str(r.campaign.name or ""),
                            "asset_group_name": str(r.asset_group.name or ""),
                            "asset_name": str(r.asset.name or ""),
                            "asset_type": _proto_enum_name(getattr(r.asset, "type_", None)),
                            "clicks": 0,
                            "impressions": 0,
                            "cost_micros": 0,
                            "conversions": 0.0,
                        }
                    a = acc[key]
                    a["clicks"] += int(r.metrics.clicks or 0)
                    a["impressions"] += int(r.metrics.impressions or 0)
                    a["cost_micros"] += int(r.metrics.cost_micros or 0)
                    a["conversions"] += float(r.metrics.conversions or 0.0)
            ranked = sorted(acc.items(), key=lambda kv: kv[1]["cost_micros"], reverse=True)[:limit_per_customer]
            for (cap_id, agroup_id, asset_id), a in ranked:
                cost = a["cost_micros"] / 1_000_000.0
                conv = float(a["conversions"])
                out.append(
                    AssetPeriodMetricsRow(
                        customer_id=a["customer_id"],
                        campaign_id=cap_id,
                        campaign_name=a["campaign_name"],
                        asset_group_id=agroup_id,
                        asset_group_name=a["asset_group_name"],
                        asset_id=asset_id,
                        asset_name=a["asset_name"],
                        asset_type=a["asset_type"],
                        date_range=df.label,
                        clicks=int(a["clicks"]),
                        impressions=int(a["impressions"]),
                        cost=float(cost),
                        conversions=conv,
                        cpa=_cpa_from_cost_and_conversions(cost, conv),
                    )
                )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error (asset_group_asset) for customer {cid}:\n{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex

    out.sort(key=lambda x: ((x.campaign_name or "").lower(), (x.asset_group_name or "").lower(), x.asset_id))
    return out


def get_change_events_for_date_range(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
    date_range: str = "YESTERDAY",
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 500,
) -> List[ChangeEventRow]:
    """
    Lịch sử thay đổi (change_event). Google giới hạn truy vấn ~30 ngày gần nhất.
    """
    df = resolve_mcp_date_filter(date_range=date_range, start_date=start_date, end_date=end_date)
    if limit < 1:
        limit = 1
    limit = min(limit, 10000)

    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          change_event.change_date_time,
          change_event.change_resource_type,
          change_event.user_email,
          change_event.client_type,
          change_event.resource_name,
          change_event.changed_fields
        FROM change_event
        WHERE change_event.change_date_time {df.gaql_predicate}
        ORDER BY change_event.change_date_time DESC
        LIMIT {int(limit)}
    """.strip()

    out: List[ChangeEventRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    ce = r.change_event
                    cf = getattr(ce, "changed_fields", None)
                    changed = ""
                    if cf is not None:
                        paths = getattr(cf, "paths", None)
                        if paths:
                            changed = ",".join(str(p) for p in paths)
                        else:
                            changed = str(cf)
                    out.append(
                        ChangeEventRow(
                            change_date_time=str(ce.change_date_time or ""),
                            change_resource_type=_proto_enum_name(getattr(ce, "change_resource_type", None)),
                            user_email=str(ce.user_email or ""),
                            client_type=_proto_enum_name(getattr(ce, "client_type", None)),
                            resource_name=str(ce.resource_name or ""),
                            changed_fields=changed,
                        )
                    )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error (change_event) for customer {cid}:\n{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex
    return out


def get_yesterday_customer_performance(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
) -> List[CustomerPerformanceRow]:
    """
    Uses GoogleAdsService.SearchStream to fetch yesterday's performance metrics per customer.

    GAQL notes:
    - `FROM customer` returns account-level aggregated metrics.
    - `segments.date DURING YESTERDAY` scopes the metrics to exactly yesterday.
    """
    period = get_customer_metrics_for_date_range(client, customer_ids, "YESTERDAY")
    return [
        CustomerPerformanceRow(
            customer_id=p.customer_id,
            customer_name=p.customer_name,
            clicks=p.clicks,
            impressions=p.impressions,
            cost=p.cost,
            conversions=p.conversions,
        )
        for p in period
    ]


def get_yesterday_campaign_performance(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
) -> List[CampaignPerformanceRow]:
    """
    Chỉ số ngày hôm qua theo từng chiến dịch (một dòng / campaign).

    GAQL:
    - `FROM campaign` + `segments.date DURING YESTERDAY` — metrics gắn với campaign + ngày.
    - Chỉ chiến dịch đang bật hoặc tạm dừng (bỏ REMOVED / không còn dùng).
    """
    period = get_campaign_metrics_for_date_range(client, customer_ids, "YESTERDAY")
    return [
        CampaignPerformanceRow(
            customer_id=p.customer_id,
            customer_name=p.customer_name,
            campaign_id=p.campaign_id,
            campaign_name=p.campaign_name,
            clicks=p.clicks,
            impressions=p.impressions,
            cost=p.cost,
            conversions=p.conversions,
        )
        for p in period
    ]


def list_campaigns_for_customers(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
) -> List[CampaignMetadataRow]:
    """
    Danh sách chiến dịch (ENABLED / PAUSED / …), bỏ REMOVED.
    """
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT
          customer.id,
          customer.descriptive_name,
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.advertising_channel_type
        FROM campaign
        WHERE campaign.status != REMOVED
        ORDER BY campaign.name
    """.strip()

    rows: List[CampaignMetadataRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    st = r.campaign.status.name if r.campaign.status else ""
                    ch = (
                        r.campaign.advertising_channel_type.name
                        if r.campaign.advertising_channel_type
                        else ""
                    )
                    rows.append(
                        CampaignMetadataRow(
                            customer_id=str(r.customer.id),
                            customer_name=str(r.customer.descriptive_name or ""),
                            campaign_id=str(r.campaign.id),
                            campaign_name=str(r.campaign.name or ""),
                            status=st,
                            advertising_channel_type=ch,
                        )
                    )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error listing campaigns for customer {cid}:\n{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex

    rows.sort(
        key=lambda x: (
            (x.customer_name or "").lower(),
            (x.campaign_name or "").lower(),
            x.campaign_id,
        )
    )
    return rows


def _micros_to_currency(micros: Any) -> Optional[float]:
    if micros is None:
        return None
    try:
        v = int(micros)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    return v / 1_000_000.0


def _target_cpa_from_campaign_proto(campaign: Any) -> Optional[float]:
    """Đọc target CPA từ scheme bidding gắn trực tiếp trên campaign."""
    for attr in (
        "maximize_conversions",
        "target_cpa",
    ):
        scheme = getattr(campaign, attr, None)
        if scheme is None:
            continue
        micros = getattr(scheme, "target_cpa_micros", None)
        val = _micros_to_currency(micros)
        if val is not None:
            return val
    return None


def _target_roas_from_campaign_proto(campaign: Any) -> Optional[float]:
    for attr in ("target_roas", "maximize_conversion_value"):
        scheme = getattr(campaign, attr, None)
        if scheme is None:
            continue
        roas = getattr(scheme, "target_roas", None)
        if roas is None:
            continue
        try:
            v = float(roas)
        except (TypeError, ValueError):
            continue
        if v > 0:
            return v
    return None


def _fetch_portfolio_bidding_targets(
    ga_service: Any,
    customer_id: str,
    resource_names: List[str],
) -> Dict[str, tuple[Optional[float], Optional[float]]]:
    """Tra target CPA/ROAS trên bidding_strategy (portfolio) theo resource name."""
    out: Dict[str, tuple[Optional[float], Optional[float]]] = {}
    ids: List[str] = []
    for rn in resource_names:
        s = str(rn or "").strip()
        if not s or s in out:
            continue
        prefix = "customers/" + customer_id + "/biddingStrategies/"
        if s.startswith(prefix):
            bid = s[len(prefix) :].split("/")[0]
            if bid.isdigit():
                ids.append(bid)
        elif "/biddingStrategies/" in s:
            bid = s.rsplit("/biddingStrategies/", 1)[-1].split("/")[0]
            if bid.isdigit():
                ids.append(bid)
    if not ids:
        return out
    id_list = ", ".join(sorted(set(ids)))
    query = f"""
        SELECT
          bidding_strategy.resource_name,
          bidding_strategy.target_cpa.target_cpa_micros,
          bidding_strategy.maximize_conversions.target_cpa_micros,
          bidding_strategy.target_roas.target_roas,
          bidding_strategy.maximize_conversion_value.target_roas
        FROM bidding_strategy
        WHERE bidding_strategy.id IN ({id_list})
    """.strip()
    try:
        stream = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in stream:
            for r in batch.results:
                bs = r.bidding_strategy
                rn = str(bs.resource_name or "")
                tcpa = _micros_to_currency(
                    getattr(getattr(bs, "target_cpa", None), "target_cpa_micros", None)
                ) or _micros_to_currency(
                    getattr(getattr(bs, "maximize_conversions", None), "target_cpa_micros", None)
                )
                troas = None
                for scheme_attr in ("target_roas", "maximize_conversion_value"):
                    scheme = getattr(bs, scheme_attr, None)
                    if scheme is None:
                        continue
                    raw = getattr(scheme, "target_roas", None)
                    if raw is not None:
                        try:
                            v = float(raw)
                            if v > 0:
                                troas = v
                                break
                        except (TypeError, ValueError):
                            pass
                out[rn] = (tcpa, troas)
    except GoogleAdsException:
        pass
    return out


def list_campaign_bidding_for_customers(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
) -> List[CampaignBiddingRow]:
    """
    Target CPA / ROAS **đang cấu hình** trên từng chiến dịch (không phải CPA thực tế từ metrics).
    Hỗ trợ scheme trên campaign và portfolio (bidding_strategy resource).
    """
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT
          customer.id,
          customer.descriptive_name,
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.bidding_strategy_type,
          campaign.bidding_strategy,
          campaign.maximize_conversions.target_cpa_micros,
          campaign.target_cpa.target_cpa_micros,
          campaign.target_roas.target_roas,
          campaign.maximize_conversion_value.target_roas
        FROM campaign
        WHERE campaign.status != REMOVED
        ORDER BY campaign.name
    """.strip()

    rows: List[CampaignBiddingRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        pending_portfolio: List[tuple[int, str]] = []
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    cap = r.campaign
                    st = cap.status.name if cap.status else ""
                    bst = cap.bidding_strategy_type.name if cap.bidding_strategy_type else ""
                    bs_res = str(cap.bidding_strategy or "")
                    tcpa = _target_cpa_from_campaign_proto(cap)
                    troas = _target_roas_from_campaign_proto(cap)
                    row = CampaignBiddingRow(
                        customer_id=str(r.customer.id),
                        customer_name=str(r.customer.descriptive_name or ""),
                        campaign_id=str(cap.id),
                        campaign_name=str(cap.name or ""),
                        status=st,
                        bidding_strategy_type=bst,
                        target_cpa=tcpa,
                        target_roas=troas,
                        bidding_strategy_resource=bs_res,
                    )
                    idx = len(rows)
                    rows.append(row)
                    if bs_res and (tcpa is None and troas is None):
                        pending_portfolio.append((idx, bs_res))
            if pending_portfolio:
                portfolio = _fetch_portfolio_bidding_targets(
                    ga_service,
                    cid,
                    [rn for _, rn in pending_portfolio],
                )
                for idx, rn in pending_portfolio:
                    ptcpa, ptroas = portfolio.get(rn, (None, None))
                    if ptcpa is None and ptroas is None:
                        continue
                    old = rows[idx]
                    rows[idx] = CampaignBiddingRow(
                        customer_id=old.customer_id,
                        customer_name=old.customer_name,
                        campaign_id=old.campaign_id,
                        campaign_name=old.campaign_name,
                        status=old.status,
                        bidding_strategy_type=old.bidding_strategy_type,
                        target_cpa=ptcpa if old.target_cpa is None else old.target_cpa,
                        target_roas=ptroas if old.target_roas is None else old.target_roas,
                        bidding_strategy_resource=old.bidding_strategy_resource,
                    )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error listing campaign bidding for customer {cid}:\n"
                f"{_format_googleads_exception(ex)}"
            ) from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex

    rows.sort(
        key=lambda x: (
            (x.customer_name or "").lower(),
            (x.campaign_name or "").lower(),
            x.campaign_id,
        )
    )
    return rows


def get_yesterday_keyword_performance(
    client: GoogleAdsClient,
    customer_ids: Iterable[str],
    *,
    limit_per_customer: int = 500,
) -> List[KeywordPerformanceRow]:
    """
    Chỉ số ngày hôm qua theo keyword (Search / mạng có keyword_view).
    Giới hạn số dòng / tài khoản để tránh payload quá lớn.
    """
    period = get_keyword_metrics_for_date_range(
        client, customer_ids, "YESTERDAY", limit_per_customer=limit_per_customer
    )
    return [
        KeywordPerformanceRow(
            customer_id=p.customer_id,
            customer_name=p.customer_name,
            campaign_id=p.campaign_id,
            campaign_name=p.campaign_name,
            ad_group_id=p.ad_group_id,
            ad_group_name=p.ad_group_name,
            criterion_id=p.criterion_id,
            keyword_text=p.keyword_text,
            match_type=p.match_type,
            clicks=p.clicks,
            impressions=p.impressions,
            cost=p.cost,
            conversions=p.conversions,
        )
        for p in period
    ]


def create_performance_max_campaign_for_local_leads(
    client: GoogleAdsClient,
    customer_id: str,
    *,
    campaign_name: str,
    daily_budget: float,
    target_cpa: Optional[float] = None,
    geo_target_constant_ids: Optional[List[int]] = None,
    final_url: str = "https://example.com",
    business_name: str = "Local Service Business",
) -> Dict[str, str]:
    """
    Creates a basic Performance Max campaign for local lead gen.

    Mutate operations overview (high-level):
    - Create a CampaignBudget
    - Create a Campaign (PERFORMANCE_MAX)
    - Create basic CampaignCriterion for location targeting (optional)
    - Create an AssetGroup with placeholder assets (mock creative)

    Important:
    - Performance Max requires an AssetGroup to serve. We add a minimal placeholder AssetGroup
      so the campaign exists, but you should replace assets with real creative before enabling.
    """
    customer_id = str(customer_id).strip().replace("-", "")
    if not customer_id:
        raise GoogleAdsHelperError("Customer ID is required.")
    if daily_budget <= 0:
        raise GoogleAdsHelperError("Daily budget must be > 0.")

    campaign_budget_service = client.get_service("CampaignBudgetService")
    campaign_service = client.get_service("CampaignService")
    campaign_criterion_service = client.get_service("CampaignCriterionService")
    asset_group_service = client.get_service("AssetGroupService")
    asset_group_asset_service = client.get_service("AssetGroupAssetService")
    asset_service = client.get_service("AssetService")
    mutate_operation = client.get_type("MutateOperation")

    budget_amount_micros = int(round(daily_budget * 1_000_000))
    target_cpa_micros = None if target_cpa is None else int(round(target_cpa * 1_000_000))

    # 1) Create Campaign Budget
    budget_op = mutate_operation
    budget_op.campaign_budget_operation.create.name = f"{campaign_name} Budget"
    budget_op.campaign_budget_operation.create.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD
    budget_op.campaign_budget_operation.create.amount_micros = budget_amount_micros
    # PMax typically uses non-shared budgets per campaign.
    budget_op.campaign_budget_operation.create.explicitly_shared = False

    # 2) Create Campaign
    campaign_op = client.get_type("MutateOperation")
    campaign = campaign_op.campaign_operation.create
    campaign.name = campaign_name
    campaign.status = client.enums.CampaignStatusEnum.PAUSED
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.PERFORMANCE_MAX
    campaign.campaign_budget = ""  # temp; set after budget created via resource name returned

    # Bidding: Maximize conversions (lead gen). Optionally set target CPA.
    # Note: If you set target_cpa_micros, Google will try to hit that CPA.
    # If you don't, it will just maximize conversions.
    if target_cpa_micros is not None:
        campaign.maximize_conversions.target_cpa_micros = target_cpa_micros
    else:
        campaign.maximize_conversions.CopyFrom(client.get_type("MaximizeConversions"))

    # Basic URL expansion setting (common default). You can adjust later.
    campaign.final_url_expansion_opt_out = False

    # 3) Execute budget + campaign as a batch so we can reference budget in campaign.
    try:
        # Create budget first.
        budget_response = campaign_budget_service.mutate_campaign_budgets(
            customer_id=customer_id,
            operations=[budget_op.campaign_budget_operation],
        )
        budget_resource_name = budget_response.results[0].resource_name

        # Now create campaign referencing budget.
        campaign.campaign_budget = budget_resource_name
        campaign_response = campaign_service.mutate_campaigns(
            customer_id=customer_id,
            operations=[campaign_op.campaign_operation],
        )
        campaign_resource_name = campaign_response.results[0].resource_name
    except GoogleAdsException as ex:
        raise GoogleAdsHelperError(f"Failed creating budget/campaign:\n{_format_googleads_exception(ex)}") from ex

    # 4) Optional location targeting via CampaignCriterion
    if geo_target_constant_ids:
        try:
            ops = []
            for geo_id in geo_target_constant_ids:
                op = client.get_type("CampaignCriterionOperation")
                crit = op.create
                crit.campaign = campaign_resource_name
                # Resource name format: geoTargetConstants/{geo_target_constant_id}
                crit.location.geo_target_constant = client.get_service(
                    "GeoTargetConstantService"
                ).geo_target_constant_path(int(geo_id))
                ops.append(op)
            if ops:
                campaign_criterion_service.mutate_campaign_criteria(customer_id=customer_id, operations=ops)
        except GoogleAdsException as ex:
            # Non-fatal; surface but keep the created campaign.
            raise GoogleAdsHelperError(
                f"Campaign created, but location targeting failed:\n{_format_googleads_exception(ex)}"
            ) from ex

    # 5) Create minimal AssetGroup + placeholder assets (mock creative)
    try:
        asset_group_op = client.get_type("AssetGroupOperation")
        ag = asset_group_op.create
        ag.name = f"{campaign_name} Asset Group"
        ag.campaign = campaign_resource_name
        ag.final_urls.append(final_url)
        ag.status = client.enums.AssetGroupStatusEnum.PAUSED

        ag_response = asset_group_service.mutate_asset_groups(customer_id=customer_id, operations=[asset_group_op])
        asset_group_resource_name = ag_response.results[0].resource_name

        # Create placeholder assets (text-only) to satisfy basic structure for demo.
        # Note: In real PMax you should add images, logos, and a full set of headlines/descriptions.
        created_asset_resource_names: List[str] = []

        def _create_text_asset(asset_text: str, field: str) -> str:
            op = client.get_type("AssetOperation")
            asset = op.create
            asset.text_asset.text = asset_text
            resp = asset_service.mutate_assets(customer_id=customer_id, operations=[op])
            return resp.results[0].resource_name

        headline_asset = _create_text_asset(f"{business_name} - Fast Repairs", "headline")
        long_headline_asset = _create_text_asset("Book a same-day service visit. Call now.", "long_headline")
        desc_asset = _create_text_asset("Trusted local technicians. Upfront pricing. Schedule today.", "description")
        created_asset_resource_names.extend([headline_asset, long_headline_asset, desc_asset])

        # Link assets to the AssetGroup.
        aga_ops = []
        for rn, field_type in [
            (headline_asset, client.enums.AssetFieldTypeEnum.HEADLINE),
            (long_headline_asset, client.enums.AssetFieldTypeEnum.LONG_HEADLINE),
            (desc_asset, client.enums.AssetFieldTypeEnum.DESCRIPTION),
        ]:
            op = client.get_type("AssetGroupAssetOperation")
            aga = op.create
            aga.asset_group = asset_group_resource_name
            aga.asset = rn
            aga.field_type = field_type
            aga_ops.append(op)

        asset_group_asset_service.mutate_asset_group_assets(customer_id=customer_id, operations=aga_ops)
    except GoogleAdsException as ex:
        raise GoogleAdsHelperError(
            "Campaign created, but placeholder AssetGroup/assets failed. "
            "PMax needs valid assets before it can serve.\n"
            f"{_format_googleads_exception(ex)}"
        ) from ex

    return {
        "budget_resource_name": budget_resource_name,
        "campaign_resource_name": campaign_resource_name,
        "asset_group_resource_name": asset_group_resource_name,
    }


def optimize_budgets_by_cpa(
    client: GoogleAdsClient,
    customer_id: str,
    *,
    target_cpa: float,
    date_range: str = "LAST_30_DAYS",
    increase_pct: float = 0.10,
) -> Dict[str, Any]:
    """
    Loops through enabled campaigns and increases daily budget when CPA < target.

    GAQL notes:
    - We query `campaign.campaign_budget` to know which budget resource to update.
    - We query `metrics.cost_micros` and `metrics.conversions` to compute CPA:
        CPA = (cost_micros/1e6) / conversions
    - We filter to ENABLED campaigns (active). Change if you want PAUSED included.
    """
    if target_cpa <= 0:
        raise GoogleAdsHelperError("target_cpa must be > 0")
    if increase_pct <= 0:
        raise GoogleAdsHelperError("increase_pct must be > 0")

    customer_id = str(customer_id).strip().replace("-", "")

    ga_service = client.get_service("GoogleAdsService")
    budget_service = client.get_service("CampaignBudgetService")

    query = f"""
        SELECT
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.campaign_budget,
          campaign_budget.amount_micros,
          metrics.cost_micros,
          metrics.conversions
        FROM campaign
        WHERE campaign.status = ENABLED
          AND segments.date DURING {date_range}
    """.strip()

    updated: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    try:
        stream = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in stream:
            for r in batch.results:
                conversions = float(r.metrics.conversions or 0.0)
                cost = float((r.metrics.cost_micros or 0) / 1_000_000.0)
                if conversions <= 0:
                    skipped.append(
                        {
                            "campaign_id": str(r.campaign.id),
                            "campaign_name": str(r.campaign.name),
                            "reason": "No conversions in date range",
                        }
                    )
                    continue

                cpa = cost / conversions
                current_budget_micros = int(r.campaign_budget.amount_micros or 0)
                if current_budget_micros <= 0:
                    skipped.append(
                        {
                            "campaign_id": str(r.campaign.id),
                            "campaign_name": str(r.campaign.name),
                            "reason": "Budget amount missing/zero",
                        }
                    )
                    continue

                if cpa >= target_cpa:
                    skipped.append(
                        {
                            "campaign_id": str(r.campaign.id),
                            "campaign_name": str(r.campaign.name),
                            "reason": f"CPA {cpa:.2f} >= target {target_cpa:.2f}",
                        }
                    )
                    continue

                new_budget_micros = int(math.floor(current_budget_micros * (1.0 + increase_pct)))
                op = client.get_type("CampaignBudgetOperation")
                op.update.resource_name = str(r.campaign.campaign_budget)
                op.update.amount_micros = new_budget_micros
                op.update_mask.CopyFrom(FieldMask(paths=["amount_micros"]))

                budget_service.mutate_campaign_budgets(customer_id=customer_id, operations=[op])

                updated.append(
                    {
                        "campaign_id": str(r.campaign.id),
                        "campaign_name": str(r.campaign.name),
                        "old_daily_budget": current_budget_micros / 1_000_000.0,
                        "new_daily_budget": new_budget_micros / 1_000_000.0,
                        "cpa": cpa,
                    }
                )
    except GoogleAdsException as ex:
        raise GoogleAdsHelperError(f"Budget optimization failed:\n{_format_googleads_exception(ex)}") from ex
    except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
        raise GoogleAdsHelperError(f"Transport error during optimization: {ex}") from ex

    return {"updated": updated, "skipped": skipped}

