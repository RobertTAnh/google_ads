"""One-off: list child accounts under a given MCC from GOOGLE_ADS_MCC_CONFIGS in .env."""
from __future__ import annotations

import os
import sys
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from google_ads_helper import (  # noqa: E402
    GoogleAdsHelperError,
    build_google_ads_client_for_mcc_id,
    list_child_accounts_under_mcc,
    load_google_ads_mcc_configs_from_env,
)


def _load_mcc_configs_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    text = env_path.read_text(encoding="utf-8")
    key = "GOOGLE_ADS_MCC_CONFIGS"
    idx = text.find(key + "=")
    if idx < 0:
        return
    rest = text[idx + len(key) + 1 :].lstrip()
    if rest.startswith("'"):
        end = rest.rfind("'")
        raw = rest[1:end] if end > 0 else rest[1:]
    elif rest.startswith('"'):
        end = rest.find('"', 1)
        raw = rest[1:end]
    else:
        raw = rest.splitlines()[0]
    os.environ["GOOGLE_ADS_MCC_CONFIGS"] = raw


def main() -> None:
    mcc_id = (sys.argv[1] if len(sys.argv) > 1 else "4626005801").strip()
    _load_mcc_configs_env()
    configs = load_google_ads_mcc_configs_from_env()
    if mcc_id not in configs:
        print(f"MCC {mcc_id} not in GOOGLE_ADS_MCC_CONFIGS")
        sys.exit(1)
    cfg = configs[mcc_id]
    print(f"MCC {mcc_id} ({cfg.get('label', '')})")
    print(f"  refresh_token: {'yes' if cfg.get('refresh_token') else 'NO'}")
    try:
        client = build_google_ads_client_for_mcc_id(
            mcc_id, configs, yaml_path=str(ROOT / "google-ads.yaml")
        )
        children = list_child_accounts_under_mcc(client, mcc_id)
    except GoogleAdsHelperError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    print(f"OK  count={len(children)}")
    for ch in children[:25]:
        d = asdict(ch)
        name = (d["customer_name"] or "")[:55]
        print(
            f"  {d['customer_id']}  L{d['level']}  {d['status']}  "
            f"mgr={d['is_manager']}  {name}"
        )
    if len(children) > 25:
        print(f"  ... +{len(children) - 25} more")


if __name__ == "__main__":
    main()
