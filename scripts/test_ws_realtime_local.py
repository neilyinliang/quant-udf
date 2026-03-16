#!/usr/bin/env python3
"""
Quick local test script for WebSocket realtime endpoint.

It validates:
1) WebSocket handshake + hello message
2) ping -> pong
3) subscribe -> ack

Run:
    python scripts/test_ws_realtime_local.py
    python scripts/test_ws_realtime_local.py --symbol DCE.l2605 --frequency tick --count 2
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Local WS realtime smoke test")
    p.add_argument("--symbol", default="DCE.l2605", help="symbol to subscribe")
    p.add_argument("--frequency", default="60s", help="30s / 60s / 1d ...")
    p.add_argument("--count", type=int, default=2, help="window count")
    p.add_argument(
        "--expect-tick-or-bar",
        action="store_true",
        help="also wait for one bar message after subscribe ack",
    )
    return p.parse_args()


def short(obj: Any, n: int = 400) -> str:
    s = json.dumps(obj, ensure_ascii=False, default=str)
    return s if len(s) <= n else s[:n] + " ..."


def main() -> int:
    args = parse_args()

    # Ensure project root is importable when running from scripts/
    project_root = Path(__file__).resolve().parents[1]
    root_str = str(project_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

    try:
        from fastapi.testclient import TestClient

        from udf_service.server import app
    except Exception as e:
        print(f"[FAIL] import failed: {e}")
        return 1

    client = TestClient(app)

    try:
        with client.websocket_connect("/ws/realtime") as ws:
            # 1) hello
            hello: Dict[str, Any] = ws.receive_json()
            print(f"[INFO] hello: {short(hello)}")
            if hello.get("type") != "hello":
                print("[FAIL] first message is not hello")
                return 2

            # 2) ping -> pong
            ws.send_json({"op": "ping"})
            pong: Dict[str, Any] = ws.receive_json()
            print(f"[INFO] pong: {short(pong)}")
            if pong.get("type") != "pong":
                print("[FAIL] ping did not return pong")
                return 3

            # 3) subscribe -> ack
            sub_req = {
                "op": "subscribe",
                "symbol": args.symbol,
                "frequency": args.frequency,
                "count": max(1, int(args.count)),
            }
            ws.send_json(sub_req)
            msg = ws.receive_json()
            print(f"[INFO] subscribe response: {short(msg)}")

            if msg.get("type") != "ack" or msg.get("op") != "subscribe":
                print("[FAIL] subscribe did not return ack")
                return 4

            if args.expect_tick_or_bar:
                event = ws.receive_json()
                print(f"[INFO] first event: {short(event)}")
                if event.get("type") != "bar":
                    print("[FAIL] expected bar event after subscribe ack")
                    return 5

            # cleanup
            ws.send_json(
                {
                    "op": "unsubscribe",
                    "symbol": args.symbol,
                    "frequency": args.frequency,
                    "count": max(1, int(args.count)),
                }
            )
            unsub = ws.receive_json()
            print(f"[INFO] unsubscribe response: {short(unsub)}")

        print("[PASS] WebSocket realtime local test passed")
        return 0

    except Exception as e:
        print(f"[FAIL] websocket test failed: {e}")
        return 10


if __name__ == "__main__":
    raise SystemExit(main())
