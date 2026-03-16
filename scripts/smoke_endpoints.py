#!/usr/bin/env python3
"""
Smoke-test core quant-udf API endpoints.

Usage:
    python scripts/smoke_endpoints.py
    python scripts/smoke_endpoints.py --base-url http://localhost:8000 --symbol SHSE.600000
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Dict, List, Tuple

import httpx


def build_endpoints(symbol: str, query: str) -> List[Tuple[str, Dict[str, Any]]]:
    now = int(time.time())
    seven_days_ago = now - 7 * 24 * 60 * 60
    return [
        ("/config", {}),
        ("/time", {}),
        ("/search", {"query": query, "limit": 5}),
        (
            "/history",
            {
                "symbol": symbol,
                "resolution": "D",
                "from": seven_days_ago,
                "to": now,
            },
        ),
    ]


def short_json(data: Any, max_len: int = 300) -> str:
    text = json.dumps(data, ensure_ascii=False)
    return text if len(text) <= max_len else text[:max_len] + " ..."


def check_payload(path: str, payload: Any) -> Tuple[bool, str]:
    if path == "/config":
        if not isinstance(payload, dict):
            return False, "config payload is not an object"
        required = {"supported_resolutions", "supports_search", "supports_time"}
        missing = [k for k in required if k not in payload]
        if missing:
            return False, f"config missing keys: {missing}"
        return True, "ok"

    if path == "/time":
        if not isinstance(payload, dict) or "unixtime" not in payload:
            return False, "time payload missing `unixtime`"
        return True, "ok"

    if path == "/search":
        if not isinstance(payload, list):
            return False, "search payload is not a list"
        return True, "ok"

    if path == "/history":
        if not isinstance(payload, dict):
            return False, "history payload is not an object"
        status = payload.get("s")
        if status not in {"ok", "no_data", "error"}:
            return False, f"history `s` unexpected: {status!r}"
        return True, "ok"

    return True, "ok"


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test core UDF endpoints")
    parser.add_argument(
        "--base-url", default="http://localhost:8000", help="UDF service base URL"
    )
    parser.add_argument(
        "--symbol", default="SHSE.600000", help="Symbol used for /history"
    )
    parser.add_argument("--query", default="BTC", help="Search query used for /search")
    parser.add_argument(
        "--timeout", type=float, default=10.0, help="HTTP timeout in seconds"
    )
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    endpoints = build_endpoints(symbol=args.symbol, query=args.query)

    failures = 0
    print(f"== Smoke test start: {base_url} ==")

    with httpx.Client(base_url=base_url, timeout=args.timeout) as client:
        for path, params in endpoints:
            try:
                resp = client.get(path, params=params)
            except Exception as exc:
                failures += 1
                print(f"[FAIL] GET {path} params={params} -> request error: {exc}")
                continue

            prefix = "[OK]  " if resp.status_code == 200 else "[FAIL]"
            print(f"{prefix} GET {path} params={params} -> status={resp.status_code}")

            if resp.status_code != 200:
                failures += 1
                print(f"      body={resp.text[:300]}")
                continue

            try:
                payload = resp.json()
            except Exception as exc:
                failures += 1
                print(f"      [FAIL] invalid JSON: {exc}")
                print(f"      body={resp.text[:300]}")
                continue

            valid, reason = check_payload(path, payload)
            if not valid:
                failures += 1
                print(f"      [FAIL] payload check: {reason}")
                print(f"      data={short_json(payload)}")
            else:
                print(f"      [OK]  payload check: {reason}")
                print(f"      data={short_json(payload)}")

    if failures:
        print(f"== Smoke test done: FAILED ({failures} issue(s)) ==")
        return 1

    print("== Smoke test done: PASSED ==")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
