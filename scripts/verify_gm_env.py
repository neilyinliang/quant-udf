#!/usr/bin/env python3
"""
Minimal 掘金 SDK (gm) environment verification script.

Usage:
    python scripts/verify_gm_env.py

Checks:
1) gm SDK can be imported
2) GM_TOKEN can be loaded (env first, then default config files)
3) SDK can be initialized with token
4) Basic API call works: get_instruments
5) Optional history query works (symbol configurable via GM_TEST_SYMBOL)

GM_TOKEN priority:
- Environment variable: GM_TOKEN
- gm_config.json (project root)
- config/gm_config.json
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

DEFAULT_CONFIG_PATHS: Tuple[Path, ...] = (
    Path.cwd() / "gm_config.json",
    Path.cwd() / "config" / "gm_config.json",
)


def load_token_from_default_config() -> Optional[str]:
    for path in DEFAULT_CONFIG_PATHS:
        if not path.exists():
            continue

        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] Failed to parse config file {path}: {e}")
            return None

        if not isinstance(raw, dict):
            print(f"[WARN] Config file is not a JSON object: {path}")
            return None

        token = raw.get("GM_TOKEN")
        if token is None:
            print(f"[WARN] GM_TOKEN not found in config file: {path}")
            return None

        token_str = str(token).strip()
        if not token_str:
            print(f"[WARN] GM_TOKEN is empty in config file: {path}")
            return None

        return token_str

    return None


def get_gm_token() -> Optional[str]:
    token = os.getenv("GM_TOKEN")
    if token and token.strip():
        return token.strip()
    return load_token_from_default_config()


def main() -> int:
    print("== GM SDK Environment Verification ==")

    # 1) import gm
    try:
        import gm.api as gm_api
        from gm.api import query
    except Exception as e:
        print(f"[FAIL] Cannot import gm SDK: {e}")
        print("Hint: install dependencies first, e.g. pip install -r requirements.txt")
        return 1

    print("[OK] gm SDK import success")

    # 2) resolve token
    token = get_gm_token()
    if not token:
        print("[FAIL] GM_TOKEN is missing.")
        print("Provide one of:")
        print("- Environment variable: GM_TOKEN")
        print("- gm_config.json or config/gm_config.json with key GM_TOKEN")
        return 2

    # 3) init sdk
    try:
        gm_api.set_token(token)
    except Exception as e:
        print(f"[FAIL] SDK initialization failed with GM_TOKEN: {e}")
        return 3

    print("[OK] SDK initialization success")

    # 4) basic API check
    try:
        df = query.get_instruments(df=True)
        rows = 0 if getattr(df, "empty", True) else len(df)
        print(f"[OK] get_instruments success, rows={rows}")
    except Exception as e:
        print(f"[FAIL] get_instruments failed: {e}")
        return 4

    # 5) optional history check
    test_symbol = os.getenv("GM_TEST_SYMBOL", "SHSE.600000")
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=3)

    try:
        hist = query.history(
            test_symbol,
            "1d",
            start.strftime("%Y-%m-%d %H:%M:%S"),
            end.strftime("%Y-%m-%d %H:%M:%S"),
            fields="symbol,frequency,eob,open,high,low,close,volume",
            df=True,
        )
        rows = 0 if getattr(hist, "empty", True) else len(hist)
        print(f"[OK] history success, symbol={test_symbol}, rows={rows}")
    except Exception as e:
        print(f"[WARN] history check failed for symbol={test_symbol}: {e}")
        print("[INFO] This may be symbol/account/permission related.")

    print("== Verification completed ==")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
