from __future__ import annotations

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
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          customer.id,
          customer.descriptive_name,
          metrics.clicks,
          metrics.impressions,
          metrics.cost_micros,
          metrics.conversions
        FROM customer
        WHERE segments.date DURING YESTERDAY
    """.strip()

    rows: List[CustomerPerformanceRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        try:
            # SearchStream streams responses; we aggregate the single row for the account/date.
            stream = ga_service.search_stream(customer_id=cid, query=query)
            found_any = False
            for batch in stream:
                for r in batch.results:
                    found_any = True
                    cost = (r.metrics.cost_micros or 0) / 1_000_000.0
                    rows.append(
                        CustomerPerformanceRow(
                            customer_id=str(r.customer.id),
                            customer_name=str(r.customer.descriptive_name or ""),
                            clicks=int(r.metrics.clicks or 0),
                            impressions=int(r.metrics.impressions or 0),
                            cost=float(cost),
                            conversions=float(r.metrics.conversions or 0.0),
                        )
                    )
            if not found_any:
                rows.append(
                    CustomerPerformanceRow(
                        customer_id=cid,
                        customer_name="",
                        clicks=0,
                        impressions=0,
                        cost=0.0,
                        conversions=0.0,
                    )
                )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(f"Google Ads API error for customer {cid}:\n{_format_googleads_exception(ex)}") from ex
        except (google_api_exceptions.GoogleAPICallError, google_api_exceptions.RetryError) as ex:
            raise GoogleAdsHelperError(f"Transport error for customer {cid}: {ex}") from ex
    return rows


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
    ga_service = client.get_service("GoogleAdsService")
    query = """
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
        WHERE segments.date DURING YESTERDAY
          AND campaign.status IN (ENABLED, PAUSED)
    """.strip()

    rows: List[CampaignPerformanceRow] = []
    for cid in customer_ids:
        cid = str(cid).strip().replace("-", "")
        if not cid:
            continue
        try:
            stream = ga_service.search_stream(customer_id=cid, query=query)
            for batch in stream:
                for r in batch.results:
                    cost = (r.metrics.cost_micros or 0) / 1_000_000.0
                    rows.append(
                        CampaignPerformanceRow(
                            customer_id=str(r.customer.id),
                            customer_name=str(r.customer.descriptive_name or ""),
                            campaign_id=str(r.campaign.id),
                            campaign_name=str(r.campaign.name or ""),
                            clicks=int(r.metrics.clicks or 0),
                            impressions=int(r.metrics.impressions or 0),
                            cost=float(cost),
                            conversions=float(r.metrics.conversions or 0.0),
                        )
                    )
        except GoogleAdsException as ex:
            raise GoogleAdsHelperError(
                f"Google Ads API error for customer {cid}:\n{_format_googleads_exception(ex)}"
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

