#!/usr/bin/env python3
"""
Dedicated GM realtime worker for WebSocket bridge.

Protocol (JSON Lines over stdio):
- Parent -> worker (stdin):
  {"op":"subscribe","symbol":"DCE.l2605","frequency":"60s","count":2}
  {"op":"unsubscribe","symbol":"DCE.l2605","frequency":"60s","count":2}
  {"op":"ping"}
  {"op":"stop"}

- Worker -> parent (stdout, each line prefixed):
  @@GMWS@@{"type":"worker_ready","ts":...}
  @@GMWS@@{"type":"worker_ack","op":"subscribe",...}
  @@GMWS@@{"type":"bar","symbol":"...","frequency":"60s","data":{...},"ts":...}
  @@GMWS@@{"type":"error","message":"...","ts":...}
  @@GMWS@@{"type":"worker_shutdown","ts":...}
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PREFIX = "@@GMWS@@"
DEFAULT_CONFIG_PATHS = (
    Path.cwd() / "gm_config.json",
    Path.cwd() / "config" / "gm_config.json",
)

CMD_Q: "queue.Queue[Dict[str, Any]]" = queue.Queue()
STOP_EVENT = threading.Event()
SUB_LOCK = threading.RLock()
ACTIVE_SUBS: set[Tuple[str, str, int]] = set()

# gm functions bound at runtime in main()
GM_SUBSCRIBE = None
GM_UNSUBSCRIBE = None

# optional startup subscription (must survive gm runtime re-import)
ENV_STARTUP_SYMBOL = "GM_WS_STARTUP_SYMBOL"
ENV_STARTUP_FREQUENCY = "GM_WS_STARTUP_FREQUENCY"
ENV_STARTUP_COUNT = "GM_WS_STARTUP_COUNT"

STARTUP_SYMBOL: str = (os.getenv(ENV_STARTUP_SYMBOL) or "").strip()
STARTUP_FREQUENCY: str = (os.getenv(ENV_STARTUP_FREQUENCY) or "60s").strip() or "60s"
try:
    STARTUP_COUNT: int = max(1, int(os.getenv(ENV_STARTUP_COUNT) or "2"))
except Exception:
    STARTUP_COUNT = 2


def emit(payload: Dict[str, Any]) -> None:
    try:
        line = PREFIX + json.dumps(payload, ensure_ascii=False, default=str)
        print(line, flush=True)
    except Exception:
        pass


def _normalize_pandas_like(value: Any) -> Any:
    """Best-effort normalize pandas DataFrame/Series into plain dict records."""
    try:
        # DataFrame-like
        if (
            hasattr(value, "to_dict")
            and hasattr(value, "iloc")
            and hasattr(value, "columns")
        ):
            if getattr(value, "empty", False):
                return None
            row = value.iloc[-1]
            if hasattr(row, "to_dict"):
                return row.to_dict()

        # Series-like
        if (
            hasattr(value, "to_dict")
            and hasattr(value, "index")
            and not isinstance(value, dict)
        ):
            data = value.to_dict()
            if isinstance(data, dict):
                return data
    except Exception:
        return None
    return None


def to_jsonable(value: Any) -> Any:
    normalized = _normalize_pandas_like(value)
    if normalized is not None:
        return to_jsonable(normalized)

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    # numpy scalar-like
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            return to_jsonable(value.item())
        except Exception:
            pass

    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(v) for v in value]
    if hasattr(value, "__dict__"):
        return to_jsonable(vars(value))
    return str(value)


def resolve_subscribe_unsubscribe() -> Tuple[Optional[Any], Optional[Any]]:
    global GM_SUBSCRIBE, GM_UNSUBSCRIBE
    if GM_SUBSCRIBE is not None and GM_UNSUBSCRIBE is not None:
        return GM_SUBSCRIBE, GM_UNSUBSCRIBE
    try:
        from gm.api import subscribe as _subscribe
        from gm.api import unsubscribe as _unsubscribe
    except Exception:
        return None, None
    GM_SUBSCRIBE = _subscribe
    GM_UNSUBSCRIBE = _unsubscribe
    return GM_SUBSCRIBE, GM_UNSUBSCRIBE


def load_token_from_file() -> Optional[str]:
    for p in DEFAULT_CONFIG_PATHS:
        if not p.exists():
            continue
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            emit(
                {
                    "type": "error",
                    "message": f"parse config failed: {p}: {e}",
                    "ts": int(time.time()),
                }
            )
            return None

        if not isinstance(raw, dict):
            emit(
                {
                    "type": "error",
                    "message": f"config is not object: {p}",
                    "ts": int(time.time()),
                }
            )
            return None

        token = raw.get("GM_TOKEN")
        if token is None:
            continue

        token = str(token).strip()
        if token:
            return token
    return None


def get_token() -> Optional[str]:
    token = os.getenv("GM_TOKEN")
    if token and token.strip():
        return token.strip()
    return load_token_from_file()


def stdin_reader() -> None:
    while not STOP_EVENT.is_set():
        line = sys.stdin.readline()
        if not line:
            # Parent may be gone; stop gracefully.
            STOP_EVENT.set()
            break
        line = line.strip()
        if not line:
            continue
        try:
            cmd = json.loads(line)
        except Exception:
            emit(
                {
                    "type": "error",
                    "message": f"invalid command json: {line[:200]}",
                    "ts": int(time.time()),
                }
            )
            continue
        if isinstance(cmd, dict):
            CMD_Q.put(cmd)


def command_processor() -> None:
    while not STOP_EVENT.is_set():
        try:
            cmd = CMD_Q.get(timeout=0.5)
        except queue.Empty:
            continue

        op = str(cmd.get("op", "")).strip().lower()
        if op == "ping":
            emit({"type": "pong", "ts": int(time.time())})
            continue
        if op == "stop":
            STOP_EVENT.set()
            emit({"type": "worker_stopping", "ts": int(time.time())})
            continue

        symbol = str(cmd.get("symbol", "")).strip()
        if not symbol:
            emit(
                {
                    "type": "error",
                    "message": f"missing symbol for op={op}",
                    "ts": int(time.time()),
                }
            )
            continue

        frequency = str(cmd.get("frequency", "60s")).strip() or "60s"
        try:
            count = max(1, int(cmd.get("count", 2)))
        except Exception:
            count = 2

        key = (symbol, frequency, count)

        try:
            subscribe_fn, unsubscribe_fn = resolve_subscribe_unsubscribe()

            if op == "subscribe":
                with SUB_LOCK:
                    # Keep behavior explicit: don't deduplicate at worker level too aggressively.
                    ACTIVE_SUBS.add(key)
                if subscribe_fn is None:
                    raise RuntimeError("GM subscribe function unavailable")
                subscribe_fn(
                    symbols=symbol,
                    frequency=frequency,
                    count=count,
                    unsubscribe_previous=False,
                )
                emit(
                    {
                        "type": "worker_ack",
                        "op": "subscribe",
                        "symbol": symbol,
                        "frequency": frequency,
                        "count": count,
                        "ts": int(time.time()),
                    }
                )
            elif op == "unsubscribe":
                with SUB_LOCK:
                    ACTIVE_SUBS.discard(key)
                if unsubscribe_fn is None:
                    raise RuntimeError("GM unsubscribe function unavailable")
                unsubscribe_fn(symbols=symbol, frequency=frequency)
                emit(
                    {
                        "type": "worker_ack",
                        "op": "unsubscribe",
                        "symbol": symbol,
                        "frequency": frequency,
                        "count": count,
                        "ts": int(time.time()),
                    }
                )
            else:
                emit(
                    {
                        "type": "error",
                        "message": f"unsupported op: {op}",
                        "ts": int(time.time()),
                    }
                )
        except Exception as e:
            emit(
                {
                    "type": "error",
                    "message": f"command {op} failed ({symbol}/{frequency}/{count}): {e}",
                    "ts": int(time.time()),
                }
            )


# ---- GM callbacks (must be module-level for many gm runtime setups) ----
def init(context: Any) -> None:
    emit({"type": "worker_ready", "ts": int(time.time())})

    # Optional direct startup subscribe for more reliable callback delivery.
    # This is executed inside gm runtime init callback.
    subscribe_fn, _ = resolve_subscribe_unsubscribe()
    if STARTUP_SYMBOL and subscribe_fn is not None:
        try:
            subscribe_fn(
                symbols=STARTUP_SYMBOL,
                frequency=STARTUP_FREQUENCY,
                count=max(1, int(STARTUP_COUNT)),
                unsubscribe_previous=False,
            )
            with SUB_LOCK:
                ACTIVE_SUBS.add(
                    (STARTUP_SYMBOL, STARTUP_FREQUENCY, max(1, int(STARTUP_COUNT)))
                )
            emit(
                {
                    "type": "worker_ack",
                    "op": "subscribe",
                    "symbol": STARTUP_SYMBOL,
                    "frequency": STARTUP_FREQUENCY,
                    "count": max(1, int(STARTUP_COUNT)),
                    "source": "startup_init",
                    "ts": int(time.time()),
                }
            )
        except Exception as e:
            emit(
                {
                    "type": "error",
                    "message": f"startup subscribe failed ({STARTUP_SYMBOL}/{STARTUP_FREQUENCY}/{STARTUP_COUNT}): {e}",
                    "ts": int(time.time()),
                }
            )


def on_bar(context: Any, bars: Any) -> None:
    def _extract_bar_dicts(value: Any) -> List[Dict[str, Any]]:
        normalized = to_jsonable(value)
        result: List[Dict[str, Any]] = []

        if isinstance(normalized, dict):
            result.append(normalized)
            return result

        if isinstance(normalized, list):
            for item in normalized:
                if isinstance(item, dict):
                    result.append(item)
                    continue
                item_norm = to_jsonable(item)
                if isinstance(item_norm, dict):
                    result.append(item_norm)
            return result

        # Some environments may pass pandas-like frames/series directly
        alt = _normalize_pandas_like(value)
        if isinstance(alt, dict):
            result.append(to_jsonable(alt))
        elif isinstance(alt, list):
            for item in alt:
                item_norm = to_jsonable(item)
                if isinstance(item_norm, dict):
                    result.append(item_norm)

        return result

    bar_list = _extract_bar_dicts(bars)

    # Normal path: emit bars directly from callback payload.
    if bar_list:
        for bar in bar_list:
            emit(
                {
                    "type": "bar",
                    "symbol": str(bar.get("symbol", "")).strip(),
                    "frequency": str(bar.get("frequency", "")).strip() or "bar",
                    "data": bar,
                    "ts": int(time.time()),
                }
            )
        return

    # Fallback path: callback payload shape is unsupported/empty.
    # Recover latest bars from context.data for active subscriptions.
    with SUB_LOCK:
        active_subs = [s for s in ACTIVE_SUBS if s[1] != "tick"]

    if not active_subs:
        return

    # Deduplicate (symbol, frequency) and keep largest requested count.
    key_to_count: Dict[Tuple[str, str], int] = {}
    for sub_symbol, sub_frequency, sub_count in active_subs:
        key = (sub_symbol, sub_frequency)
        prev = key_to_count.get(key, 0)
        key_to_count[key] = max(prev, int(sub_count))

    for (sub_symbol, sub_frequency), sub_count in key_to_count.items():
        try:
            window = context.data(
                symbol=sub_symbol,
                frequency=sub_frequency,
                count=max(1, int(sub_count)),
            )
        except Exception:
            continue

        window_norm = to_jsonable(window)
        latest_bar: Optional[Dict[str, Any]] = None

        if isinstance(window_norm, list) and window_norm:
            last = window_norm[-1]
            if isinstance(last, dict):
                latest_bar = last
        elif isinstance(window_norm, dict):
            latest_bar = window_norm

        if not isinstance(latest_bar, dict):
            continue

        emit(
            {
                "type": "bar",
                "symbol": str(latest_bar.get("symbol", sub_symbol)).strip()
                or sub_symbol,
                "frequency": str(latest_bar.get("frequency", sub_frequency)).strip()
                or sub_frequency
                or "bar",
                "data": latest_bar,
                "ts": int(time.time()),
            }
        )


def on_error(*args: Any, **kwargs: Any) -> None:
    emit(
        {
            "type": "error",
            "message": f"gm on_error args={to_jsonable(args)} kwargs={to_jsonable(kwargs)}",
            "ts": int(time.time()),
        }
    )


def shutdown(context: Any) -> None:
    try:
        if GM_UNSUBSCRIBE is not None:
            with SUB_LOCK:
                active_subs = list(ACTIVE_SUBS)
            for sub_symbol, sub_frequency, _sub_count in active_subs:
                try:
                    GM_UNSUBSCRIBE(symbols=sub_symbol, frequency=sub_frequency)
                except Exception:
                    pass
    except Exception:
        pass
    emit({"type": "worker_shutdown", "ts": int(time.time())})


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="GM WS Worker")
    p.add_argument(
        "--strategy-id",
        default="quant_udf_realtime_ws_worker",
        help="gm strategy id",
    )
    p.add_argument(
        "--startup-symbol",
        default="",
        help="optional symbol subscribed during init callback (e.g. DCE.l2605)",
    )
    p.add_argument(
        "--startup-frequency",
        default="60s",
        help="frequency for startup subscription (30s/60s/1d...)",
    )
    p.add_argument(
        "--startup-count",
        type=int,
        default=2,
        help="count/window for startup subscription",
    )
    return p.parse_args()


def main() -> int:
    global STARTUP_SYMBOL, STARTUP_FREQUENCY, STARTUP_COUNT

    args = parse_args()

    # Persist startup subscription via env vars so gm runtime re-import can see it.
    startup_symbol = (args.startup_symbol or "").strip()
    startup_frequency = (args.startup_frequency or "60s").strip() or "60s"
    startup_count = max(1, int(args.startup_count))

    os.environ[ENV_STARTUP_SYMBOL] = startup_symbol
    os.environ[ENV_STARTUP_FREQUENCY] = startup_frequency
    os.environ[ENV_STARTUP_COUNT] = str(startup_count)

    # Keep current-process globals in sync too.
    STARTUP_SYMBOL = startup_symbol
    STARTUP_FREQUENCY = startup_frequency
    STARTUP_COUNT = startup_count

    try:
        from gm.api import MODE_LIVE, run, set_token, subscribe, unsubscribe
    except Exception as e:
        emit(
            {
                "type": "error",
                "message": f"import gm failed: {e}",
                "ts": int(time.time()),
            }
        )
        return 1

    token = get_token()
    if not token:
        emit(
            {
                "type": "error",
                "message": "GM_TOKEN is not configured",
                "ts": int(time.time()),
            }
        )
        return 2

    try:
        set_token(token)
    except Exception as e:
        emit(
            {
                "type": "error",
                "message": f"set_token failed: {e}",
                "ts": int(time.time()),
            }
        )
        return 3

    global GM_SUBSCRIBE, GM_UNSUBSCRIBE
    GM_SUBSCRIBE = subscribe
    GM_UNSUBSCRIBE = unsubscribe

    # Start command I/O helpers before run()
    threading.Thread(target=stdin_reader, daemon=True).start()
    threading.Thread(target=command_processor, daemon=True).start()

    # gm.run may parse argv internally; keep argv minimal.
    sys.argv = [sys.argv[0]]

    run_candidates: List[Dict[str, Any]] = [
        {
            "strategy_id": args.strategy_id,
            "filename": Path(__file__).name,
            "mode": MODE_LIVE,
            "token": token,
        },
        {
            "strategy_id": args.strategy_id,
            "filename": str(Path(__file__).resolve()),
            "mode": MODE_LIVE,
            "token": token,
        },
    ]

    last_error: Optional[str] = None
    for kwargs in run_candidates:
        try:
            run(**kwargs)
            return 0
        except Exception as e:
            last_error = str(e)
            continue

    emit(
        {
            "type": "error",
            "message": f"worker run failed: {last_error or 'unknown'}",
            "ts": int(time.time()),
        }
    )
    return 4


if __name__ == "__main__":
    raise SystemExit(main())
