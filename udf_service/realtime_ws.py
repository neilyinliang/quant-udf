"""Subprocess-based GM realtime bridge for WebSocket push.

Design:
- Main FastAPI process does NOT run `gm.run(...)` directly (gm requires main thread behavior).
- A dedicated worker subprocess runs gm runtime and callbacks.
- Parent/worker communicate through JSON lines over stdin/stdout.

WS protocol (parent side):
- subscribe:   {"op":"subscribe","symbol":"DCE.l","frequency":"60s","count":2}
- unsubscribe: {"op":"unsubscribe","symbol":"DCE.l","frequency":"60s","count":2}
- ping:        {"op":"ping"}

Server push:
- {"type":"hello", ...}
- {"type":"ack", ...}
- {"type":"bar", ...}
- {"type":"error", ...}
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

# Lines emitted by worker will be prefixed for robust parsing.
_WORKER_PREFIX = "@@GMWS@@"

_DEFAULT_GM_CONFIG_PATHS = (
    Path.cwd() / "gm_config.json",
    Path.cwd() / "config" / "gm_config.json",
)

# -------------------- shared utils --------------------


def _load_gm_token_from_file() -> Optional[str]:
    for p in _DEFAULT_GM_CONFIG_PATHS:
        if not p.exists():
            continue
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("Failed to parse GM config file %s: %s", p, e)
            return None
        if not isinstance(raw, dict):
            logger.warning("GM config file is not a JSON object: %s", p)
            return None
        token = raw.get("GM_TOKEN")
        if token is None:
            logger.warning("GM_TOKEN missing in config file: %s", p)
            return None
        token = str(token).strip()
        if not token:
            logger.warning("GM_TOKEN is empty in config file: %s", p)
            return None
        return token
    return None


def _get_gm_token() -> Optional[str]:
    token = os.getenv("GM_TOKEN")
    if token and token.strip():
        return token.strip()
    return _load_gm_token_from_file()


def _contains_digit(symbol: str) -> bool:
    return bool(re.search(r"\d", symbol or ""))


def _to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in value]
    if hasattr(value, "__dict__"):
        return _to_jsonable(vars(value))
    return str(value)


# -------------------- worker process --------------------


def _worker_emit(payload: Dict[str, Any]) -> None:
    try:
        print(
            _WORKER_PREFIX + json.dumps(payload, ensure_ascii=False, default=str),
            flush=True,
        )
    except Exception:
        # Last resort: keep worker alive.
        pass


def _worker_start() -> int:
    """Entry for subprocess mode: run gm runtime and stream events."""
    try:
        from gm.api import MODE_LIVE, run, set_token, subscribe, unsubscribe
    except Exception as e:
        _worker_emit({"type": "error", "message": f"import gm failed: {e}"})
        return 1

    token = _get_gm_token()
    if not token:
        _worker_emit({"type": "error", "message": "GM_TOKEN is not configured"})
        return 2

    try:
        set_token(token)
    except Exception as e:
        _worker_emit({"type": "error", "message": f"set_token failed: {e}"})
        return 3

    cmd_q: "queue.Queue[Dict[str, Any]]" = queue.Queue()

    def stdin_reader() -> None:
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                cmd = json.loads(line)
            except Exception:
                continue
            cmd_q.put(cmd)

    threading.Thread(target=stdin_reader, daemon=True).start()

    def flush_commands() -> None:
        for _ in range(256):
            try:
                cmd = cmd_q.get_nowait()
            except queue.Empty:
                break

            op = str(cmd.get("op", "")).strip().lower()
            symbol = str(cmd.get("symbol", "")).strip()
            frequency = str(cmd.get("frequency", "60s")).strip() or "60s"
            count_raw = cmd.get("count", 2)
            try:
                count = max(1, int(count_raw))
            except Exception:
                count = 2

            try:
                if op == "subscribe":
                    subscribe(
                        symbols=symbol,
                        frequency=frequency,
                        count=count,
                        unsubscribe_previous=False,
                    )
                    _worker_emit(
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
                    unsubscribe(symbols=symbol, frequency=frequency)
                    _worker_emit(
                        {
                            "type": "worker_ack",
                            "op": "unsubscribe",
                            "symbol": symbol,
                            "frequency": frequency,
                            "count": count,
                            "ts": int(time.time()),
                        }
                    )
            except Exception as e:
                _worker_emit(
                    {
                        "type": "error",
                        "message": f"worker {op} failed for {symbol}/{frequency}/{count}: {e}",
                        "ts": int(time.time()),
                    }
                )

    # gm callbacks need to be module-level names in many setups; define locally but exposed in globals.
    def init(context: Any) -> None:  # noqa: ANN001
        _worker_emit({"type": "worker_ready", "ts": int(time.time())})
        flush_commands()

    def on_bar(context: Any, bars: Any) -> None:  # noqa: ANN001
        flush_commands()
        b = _to_jsonable(bars)
        bar_list: List[Dict[str, Any]]
        if isinstance(b, list):
            bar_list = [x for x in b if isinstance(x, dict)]
        elif isinstance(b, dict):
            bar_list = [b]
        else:
            bar_list = []

        for bar in bar_list:
            symbol = str(bar.get("symbol", "")).strip()
            frequency = str(bar.get("frequency", "")).strip() or "bar"
            _worker_emit(
                {
                    "type": "bar",
                    "symbol": symbol,
                    "frequency": frequency,
                    "data": bar,
                    "ts": int(time.time()),
                }
            )

    def on_error(*args: Any, **kwargs: Any) -> None:
        _worker_emit(
            {
                "type": "error",
                "message": f"gm on_error args={_to_jsonable(args)} kwargs={_to_jsonable(kwargs)}",
                "ts": int(time.time()),
            }
        )

    def shutdown(context: Any) -> None:  # noqa: ANN001
        try:
            unsubscribe(symbols="*", frequency="60s")
        except Exception:
            pass
        _worker_emit({"type": "worker_shutdown", "ts": int(time.time())})

    # expose callbacks in module globals for gm runtime lookup
    g = globals()
    g["init"] = init

    g["on_bar"] = on_bar
    g["on_error"] = on_error
    g["shutdown"] = shutdown

    # gm.run may parse argv; sanitize.
    sys.argv = [sys.argv[0]]

    # Candidate startup args (empirically robust)
    run_candidates: List[Dict[str, Any]] = [
        {
            "strategy_id": "quant_udf_realtime_ws",
            "filename": Path(__file__).name,
            "mode": MODE_LIVE,
            "token": token,
        },
        {
            "strategy_id": "quant_udf_realtime_ws",
            "filename": str(Path(__file__).resolve()),
            "mode": MODE_LIVE,
            "token": token,
        },
    ]

    last_err: Optional[str] = None
    for kwargs in run_candidates:
        try:
            run(**kwargs)
            return 0
        except Exception as e:
            last_err = str(e)
            continue

    _worker_emit(
        {
            "type": "error",
            "message": f"worker run failed: {last_err or 'unknown error'}",
            "ts": int(time.time()),
        }
    )
    return 4


# -------------------- parent hub --------------------


@dataclass(frozen=True)
class SubscriptionSpec:
    requested_symbol: str
    resolved_symbol: str
    frequency: str
    count: int


class RealtimeWsHub:
    """GM-driven realtime WebSocket subscription hub (parent process)."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        self._clients: Set[WebSocket] = set()
        self._client_subs: Dict[WebSocket, Set[SubscriptionSpec]] = {}

        # key=(resolved_symbol, frequency, count) => ref count
        self._gm_ref_counts: Dict[Tuple[str, str, int], int] = {}

        # worker subprocess resources
        self._worker_proc: Optional[subprocess.Popen[str]] = None
        self._worker_started = False
        self._worker_ready = False
        self._worker_ready_event = threading.Event()
        self._worker_stop_event = threading.Event()

        self._cmd_q: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self._pending_cmds: List[Dict[str, Any]] = []
        self._writer_thread: Optional[threading.Thread] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._worker_startup_sub: Optional[Tuple[str, str, int]] = None
        # Keys that are already subscribed by worker startup init flow.
        self._startup_seeded_subs: Set[Tuple[str, str, int]] = set()

    # ---------- loop ----------
    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    # ---------- websocket ----------
    async def websocket_handler(self, websocket: WebSocket) -> None:
        await self.connect(websocket)
        try:
            while True:
                raw = await websocket.receive_text()
                await self.handle_client_message(websocket, raw)
        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.warning("WebSocket handler error: %s", e)
        finally:
            await self.disconnect(websocket)

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._ensure_loop()
        with self._lock:
            self._clients.add(websocket)
            self._client_subs.setdefault(websocket, set())
        await websocket.send_json(
            {"type": "hello", "message": "connected", "server_time": int(time.time())}
        )

    async def disconnect(self, websocket: WebSocket) -> None:
        with self._lock:
            subs = list(self._client_subs.get(websocket, set()))
            self._clients.discard(websocket)
            self._client_subs.pop(websocket, None)

        for spec in subs:
            self._gm_unsubscribe_ref(spec.resolved_symbol, spec.frequency, spec.count)

    async def handle_client_message(
        self, websocket: WebSocket, raw_message: str
    ) -> None:
        try:
            data = json.loads(raw_message)
        except Exception:
            await websocket.send_json({"type": "error", "message": "invalid json"})
            return

        op = str(data.get("op", "")).strip().lower()
        if op == "ping":
            await websocket.send_json({"type": "pong", "ts": int(time.time())})
            return

        if op not in {"subscribe", "unsubscribe"}:
            await websocket.send_json({"type": "error", "message": "unsupported op"})
            return

        symbol = str(data.get("symbol", "")).strip()
        if not symbol:
            await websocket.send_json(
                {"type": "error", "message": "symbol is required"}
            )
            return

        frequency = str(data.get("frequency", "60s")).strip() or "60s"
        if frequency.lower() == "tick":
            await websocket.send_json(
                {
                    "type": "error",
                    "message": "tick subscription is disabled; use bar frequency like 30s/60s/1d",
                }
            )
            return

        count_raw = data.get("count", 2)
        try:
            count = max(1, int(count_raw))
        except Exception:
            count = 2

        resolved_symbol = self._resolve_symbol(symbol)
        spec = SubscriptionSpec(
            requested_symbol=symbol,
            resolved_symbol=resolved_symbol,
            frequency=frequency,
            count=count,
        )

        if op == "subscribe":
            try:
                self._ensure_worker_started(
                    startup=(spec.resolved_symbol, spec.frequency, spec.count)
                )
            except Exception as e:
                await websocket.send_json({"type": "error", "message": str(e)})
                return

            with self._lock:
                self._client_subs.setdefault(websocket, set()).add(spec)

            self._gm_subscribe_ref(spec.resolved_symbol, spec.frequency, spec.count)

            await websocket.send_json(
                {
                    "type": "ack",
                    "op": "subscribe",
                    "symbol": symbol,
                    "resolved_symbol": resolved_symbol,
                    "frequency": frequency,
                    "count": count,
                }
            )
            return

        # unsubscribe
        removed = False
        with self._lock:
            current = self._client_subs.setdefault(websocket, set())
            if spec in current:
                current.remove(spec)
                removed = True

        if removed:
            self._gm_unsubscribe_ref(spec.resolved_symbol, spec.frequency, spec.count)

        await websocket.send_json(
            {
                "type": "ack",
                "op": "unsubscribe",
                "symbol": symbol,
                "resolved_symbol": resolved_symbol,
                "frequency": frequency,
                "count": count,
                "removed": removed,
            }
        )

    # ---------- symbol resolve ----------
    def _resolve_symbol(self, symbol: str) -> str:
        if _contains_digit(symbol):
            return symbol

        try:
            import gm.api as gm_api  # lazy import in parent
        except Exception:
            return symbol

        try:
            contracts = gm_api.fut_get_continuous_contracts(csymbol=symbol)
            if contracts:
                first = contracts[0]
                resolved = (
                    first.get("symbol")
                    if isinstance(first, dict)
                    else getattr(first, "symbol", None)
                )
                if isinstance(resolved, str) and resolved.strip():
                    mapped = resolved.strip()
                    if mapped != symbol:
                        logger.info(
                            "Resolved main contract symbol: %s -> %s", symbol, mapped
                        )
                    return mapped
        except Exception as e:
            logger.warning("Resolve symbol failed for %s: %s", symbol, e)

        return symbol

    # ---------- worker lifecycle ----------
    def _ensure_worker_started(
        self,
        startup: Optional[Tuple[str, str, int]] = None,
    ) -> None:
        with self._lock:
            if self._worker_started:
                if (
                    startup is not None
                    and self._worker_startup_sub is not None
                    and startup != self._worker_startup_sub
                ):
                    logger.info(
                        "Worker already started with startup subscription %s; "
                        "new startup request %s will be sent via command queue",
                        self._worker_startup_sub,
                        startup,
                    )
                return

            token = _get_gm_token()
            if not token:
                raise RuntimeError("GM_TOKEN is not configured")

            env = os.environ.copy()
            env["GM_TOKEN"] = token

            project_root = Path(__file__).resolve().parents[1]
            worker_script = project_root / "scripts" / "gm_ws_worker.py"
            if not worker_script.exists():
                raise RuntimeError(f"worker script not found: {worker_script}")

            cmd = [sys.executable, str(worker_script)]
            if startup is not None:
                startup_symbol, startup_frequency, startup_count = startup
                startup_count = max(1, int(startup_count))
                cmd.extend(
                    [
                        "--startup-symbol",
                        startup_symbol,
                        "--startup-frequency",
                        startup_frequency,
                        "--startup-count",
                        str(startup_count),
                    ]
                )
                self._worker_startup_sub = (
                    startup_symbol,
                    startup_frequency,
                    startup_count,
                )
                self._startup_seeded_subs.add(self._worker_startup_sub)

            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
                cwd=str(project_root),
            )

            self._worker_proc = proc
            self._worker_started = True
            self._worker_stop_event.clear()
            self._worker_ready = False
            self._worker_ready_event.clear()

            self._writer_thread = threading.Thread(
                target=self._writer_loop, daemon=True
            )
            self._reader_thread = threading.Thread(
                target=self._reader_loop, daemon=True
            )
            self._writer_thread.start()
            self._reader_thread.start()

            # stderr logger thread
            threading.Thread(target=self._stderr_loop, daemon=True).start()

        # wait briefly for readiness, and fail fast if worker exits early
        ready = self._worker_ready_event.wait(timeout=8.0)
        proc = self._worker_proc
        if not ready:
            if proc is not None and proc.poll() is not None:
                raise RuntimeError(
                    f"gm worker exited before ready (exit_code={proc.returncode})"
                )
            raise RuntimeError("gm worker did not become ready in time")

    def _enqueue_worker_cmd(self, cmd: Dict[str, Any]) -> None:
        with self._lock:
            if self._worker_ready:
                self._cmd_q.put(cmd)
            else:
                self._pending_cmds.append(cmd)

    def _flush_pending_cmds(self) -> None:
        with self._lock:
            if not self._pending_cmds:
                return
            pending = list(self._pending_cmds)
            self._pending_cmds.clear()

        for cmd in pending:
            self._cmd_q.put(cmd)

    def _writer_loop(self) -> None:
        while not self._worker_stop_event.is_set():
            try:
                cmd = self._cmd_q.get(timeout=0.5)
            except queue.Empty:
                continue

            proc = self._worker_proc
            if proc is None or proc.stdin is None:
                continue

            try:
                proc.stdin.write(json.dumps(cmd, ensure_ascii=False) + "\n")
                proc.stdin.flush()
            except Exception:
                self._publish_system_error("gm worker stdin write failed")
                return

    def _stderr_loop(self) -> None:
        proc = self._worker_proc
        if proc is None or proc.stderr is None:
            return
        for line in proc.stderr:
            line = line.rstrip("\n")
            if line:
                logger.warning("[gm-worker-stderr] %s", line)

    def _reader_loop(self) -> None:
        proc = self._worker_proc
        if proc is None or proc.stdout is None:
            self._publish_system_error("gm worker stdout unavailable")
            return

        for line in proc.stdout:
            if self._worker_stop_event.is_set():
                break

            line = line.rstrip("\n")
            if not line:
                continue
            if not line.startswith(_WORKER_PREFIX):
                # non-protocol output, ignore
                continue

            payload_str = line[len(_WORKER_PREFIX) :]
            try:
                msg = json.loads(payload_str)
            except Exception:
                continue

            mtype = str(msg.get("type", "")).strip()

            if mtype == "worker_ready":
                self._worker_ready = True
                self._worker_ready_event.set()
                self._flush_pending_cmds()
                self._broadcast(
                    {
                        "type": "worker_ready",
                        "ts": int(msg.get("ts", time.time())),
                    }
                )
                continue
            if mtype == "worker_ack":
                self._broadcast(
                    {
                        "type": "worker_ack",
                        "op": msg.get("op"),
                        "symbol": msg.get("symbol"),
                        "frequency": msg.get("frequency"),
                        "count": msg.get("count"),
                        "ts": int(msg.get("ts", time.time())),
                    }
                )
                continue
            if mtype == "error":
                self._publish_system_error(str(msg.get("message", "worker error")))
                continue
            if mtype == "tick":
                # Tick events are intentionally ignored to reduce overhead.
                continue
            if mtype == "bar":
                self._deliver_bar_message(msg)
                continue

        self._publish_system_error("gm runtime stopped")

    # ---------- gm ref count ----------
    def _gm_subscribe_ref(self, symbol: str, frequency: str, count: int) -> None:
        key = (symbol, frequency, count)
        with self._lock:
            prev = self._gm_ref_counts.get(key, 0)
            self._gm_ref_counts[key] = prev + 1
            first = prev == 0

        if first:
            if key in self._startup_seeded_subs:
                logger.info(
                    "Skip duplicate subscribe command for startup-seeded subscription %s",
                    key,
                )
                return
            self._enqueue_worker_cmd(
                {
                    "op": "subscribe",
                    "symbol": symbol,
                    "frequency": frequency,
                    "count": count,
                }
            )

    def _gm_unsubscribe_ref(self, symbol: str, frequency: str, count: int) -> None:
        key = (symbol, frequency, count)
        with self._lock:
            prev = self._gm_ref_counts.get(key, 0)
            if prev <= 1:
                self._gm_ref_counts.pop(key, None)
                last = prev > 0
            else:
                self._gm_ref_counts[key] = prev - 1
                last = False

        if last:
            self._startup_seeded_subs.discard(key)
            self._enqueue_worker_cmd(
                {
                    "op": "unsubscribe",
                    "symbol": symbol,
                    "frequency": frequency,
                    "count": count,
                }
            )

    # ---------- broadcasting ----------
    def _broadcast(
        self,
        payload: Dict[str, Any],
        predicate: Optional[Callable[[WebSocket], bool]] = None,
    ) -> None:
        loop = self._loop
        if loop is None:
            return

        with self._lock:
            clients = list(self._clients)

        for ws in clients:
            if predicate is not None and not predicate(ws):
                continue
            fut = asyncio.run_coroutine_threadsafe(ws.send_json(payload), loop)

            def _done_cb(f: "asyncio.Future[Any]") -> None:
                try:
                    f.result()
                except Exception:
                    pass

            fut.add_done_callback(_done_cb)

    def _publish_system_error(self, message: str) -> None:
        self._broadcast({"type": "error", "message": message, "ts": int(time.time())})

    def _deliver_bar_message(self, msg: Dict[str, Any]) -> None:
        symbol = str(msg.get("symbol", "")).strip()
        raw_frequency = str(msg.get("frequency", "")).strip()
        data = msg.get("data")
        if not symbol:
            return

        def _normalize_bar_frequency(freq: str) -> str:
            f = (freq or "").strip().lower()
            if not f:
                return ""
            aliases = {
                "1m": "60s",
                "1min": "60s",
                "60sec": "60s",
                "60secs": "60s",
                "60second": "60s",
                "60seconds": "60s",
                "minute": "60s",
                "1minute": "60s",
            }
            return aliases.get(f, f)

        # Priority: top-level message frequency, then bar payload frequency.
        data_frequency = ""
        if isinstance(data, dict):
            data_frequency = str(data.get("frequency", "")).strip()

        freq_candidates_raw: Set[str] = {
            f for f in [raw_frequency, data_frequency] if f
        }
        freq_candidates_normalized: Set[str] = {
            _normalize_bar_frequency(f) for f in freq_candidates_raw if f
        }

        with self._lock:
            targets: List[Tuple[WebSocket, SubscriptionSpec]] = []
            for ws, subs in self._client_subs.items():
                for sub in subs:
                    if sub.resolved_symbol != symbol:
                        continue
                    if sub.frequency == "tick":
                        continue

                    # If worker bar frequency is missing, fan out to all non-tick subscribers for this symbol.
                    if not freq_candidates_raw:
                        targets.append((ws, sub))
                        continue

                    sub_raw = (sub.frequency or "").strip()
                    sub_norm = _normalize_bar_frequency(sub_raw)
                    if (
                        sub_raw in freq_candidates_raw
                        or sub_norm in freq_candidates_normalized
                    ):
                        targets.append((ws, sub))

        loop = self._loop
        if loop is None:
            return

        for ws, sub in targets:
            payload = {
                "type": "bar",
                "symbol": sub.requested_symbol,
                "resolved_symbol": sub.resolved_symbol,
                "frequency": sub.frequency,
                "source_frequency": raw_frequency or data_frequency or "bar",
                "data": data,
                "ts": int(time.time()),
            }
            fut = asyncio.run_coroutine_threadsafe(ws.send_json(payload), loop)
            fut.add_done_callback(lambda f: f.exception())


realtime_hub = RealtimeWsHub()


if __name__ == "__main__":
    # Worker mode has moved to scripts/gm_ws_worker.py.
    raise SystemExit(0)
