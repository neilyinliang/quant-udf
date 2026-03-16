#!/usr/bin/env python3
"""
Local helper to observe first realtime websocket events after subscribe.

It uses FastAPI TestClient to connect in-process (no browser required).

Usage:
    python scripts/test_ws_stream_local.py
    python scripts/test_ws_stream_local.py --symbol DCE.l --frequency tick --count 2
    python scripts/test_ws_stream_local.py --max-messages 20 --idle-timeout 15
"""

from __future__ import annotations

import argparse
import json
import queue
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Observe realtime WS events after subscribe (local TestClient)."
    )
    p.add_argument("--symbol", default="DCE.l", help="symbol to subscribe")
    p.add_argument("--frequency", default="30s", help="30s / 60s / 1d ...")
    p.add_argument("--count", type=int, default=2, help="subscription count")
    p.add_argument(
        "--max-messages",
        type=int,
        default=12,
        help="max websocket messages to print after subscribe",
    )
    p.add_argument(
        "--idle-timeout",
        type=float,
        default=12.0,
        help="stop if no new message arrives within this many seconds",
    )
    p.add_argument(
        "--hello-timeout",
        type=float,
        default=6.0,
        help="wait time for initial hello message",
    )
    return p.parse_args()


def short(data: Any, limit: int = 700) -> str:
    try:
        s = json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        s = str(data)
    return s if len(s) <= limit else s[:limit] + " ..."


class WsReader(threading.Thread):
    def __init__(self, ws, out_q: "queue.Queue[Dict[str, Any]]") -> None:
        super().__init__(daemon=True)
        self.ws = ws
        self.out_q = out_q

    def run(self) -> None:
        while True:
            try:
                msg = self.ws.receive_json()
                self.out_q.put({"kind": "msg", "data": msg, "ts": time.time()})
            except Exception as e:
                self.out_q.put({"kind": "error", "error": str(e), "ts": time.time()})
                break


def wait_one(
    q: "queue.Queue[Dict[str, Any]]",
    timeout: float,
) -> Optional[Dict[str, Any]]:
    try:
        return q.get(timeout=timeout)
    except queue.Empty:
        return None


def main() -> int:
    args = parse_args()

    # Ensure project root import when running from scripts/
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

    symbol = (args.symbol or "").strip()
    if not symbol:
        print("[FAIL] --symbol cannot be empty")
        return 2

    frequency = (args.frequency or "30s").strip() or "30s"
    count = max(1, int(args.count))
    max_messages = max(1, int(args.max_messages))
    idle_timeout = max(0.1, float(args.idle_timeout))
    hello_timeout = max(0.1, float(args.hello_timeout))

    print("== WS stream local test ==")
    print(f"symbol={symbol}")
    print(f"frequency={frequency}")
    print(f"count={count}")
    print(f"max_messages={max_messages}")
    print(f"idle_timeout={idle_timeout}")

    client = TestClient(app)
    out_q: "queue.Queue[Dict[str, Any]]" = queue.Queue()

    try:
        with client.websocket_connect("/ws/realtime") as ws:
            reader = WsReader(ws, out_q)
            reader.start()

            # 1) wait hello
            first = wait_one(out_q, timeout=hello_timeout)
            if first is None:
                print(f"[FAIL] no hello within {hello_timeout}s")
                return 3
            if first.get("kind") == "error":
                print(
                    f"[FAIL] websocket reader error before hello: {first.get('error')}"
                )
                return 4

            hello = first.get("data")
            print(f"[INFO] hello: {short(hello)}")
            if not isinstance(hello, dict) or hello.get("type") != "hello":
                print("[WARN] first message is not hello")

            # 2) subscribe
            sub_req = {
                "op": "subscribe",
                "symbol": symbol,
                "frequency": frequency,
                "count": count,
            }
            ws.send_json(sub_req)
            print(f"[SEND] {short(sub_req)}")

            # 3) observe events
            printed = 0
            while printed < max_messages:
                item = wait_one(out_q, timeout=idle_timeout)
                if item is None:
                    print(
                        f"[DONE] idle timeout reached ({idle_timeout}s), "
                        f"printed={printed}"
                    )
                    break

                if item.get("kind") == "error":
                    print(f"[ERR ] reader error: {item.get('error')}")
                    break

                msg = item.get("data")
                mtype = msg.get("type") if isinstance(msg, dict) else None
                print(f"[EVT ] type={mtype} payload={short(msg)}")
                printed += 1

            # 4) best-effort unsubscribe
            unsub_req = {
                "op": "unsubscribe",
                "symbol": symbol,
                "frequency": frequency,
                "count": count,
            }
            try:
                ws.send_json(unsub_req)
                print(f"[SEND] {short(unsub_req)}")
            except Exception as e:
                print(f"[WARN] unsubscribe send failed: {e}")

        print("[PASS] finished")
        return 0

    except Exception as e:
        print(f"[FAIL] websocket session error: {e}")
        return 10


if __name__ == "__main__":
    raise SystemExit(main())
