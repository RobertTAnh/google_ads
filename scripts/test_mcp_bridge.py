"""Smoke test: local MCP package import + Railway /mcp/v1/health (+ optional auth)."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def _load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


def test_import_mcp_server() -> None:
    import mcp_server.server  # noqa: F401

    print("OK  import mcp_server")


def test_run_mcp_stdio_script() -> None:
    path = ROOT / "run_mcp_stdio.py"
    assert path.is_file()
    print(f"OK  run_mcp_stdio.py exists ({path})")


def test_http_health() -> None:
    base = (os.environ.get("GOOGLE_ADS_MCP_BASE_URL") or "").strip().rstrip("/")
    key = (os.environ.get("MCP_API_KEY") or "").strip()
    if not base:
        print("SKIP  GOOGLE_ADS_MCP_BASE_URL not set")
        return

    import httpx

    url = f"{base}/mcp/v1/health"
    r = httpx.get(url, timeout=30.0)
    r.raise_for_status()
    data = r.json()
    print(f"OK  GET {url} -> ok={data.get('ok')}, mcp_data_routes_enabled={data.get('mcp_data_routes_enabled')}")

    if not key:
        print("SKIP  MCP_API_KEY not set (auth routes not tested)")
        return

    r2 = httpx.get(
        f"{base}/mcp/v1/child_accounts",
        headers={"X-MCP-API-Key": key},
        timeout=60.0,
    )
    if r2.status_code == 401:
        raise AssertionError("MCP_API_KEY rejected (401)")
    body = r2.json()
    if not body.get("ok"):
        raise AssertionError(f"child_accounts failed: {json.dumps(body, ensure_ascii=False)[:500]}")
    n = body.get("count", len(body.get("accounts") or []))
    print(f"OK  GET /mcp/v1/child_accounts (authenticated), count={n}")


def main() -> int:
    _load_env()
    tests = [
        test_import_mcp_server,
        test_run_mcp_stdio_script,
        test_http_health,
    ]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"FAIL {t.__name__}: {e}")
            failed += 1
    if failed:
        print(f"\n{failed} test(s) failed")
        return 1
    print("\nAll checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
