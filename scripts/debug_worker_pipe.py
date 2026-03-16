#!/usr/bin/env python3
"""
Debug helper: validate gm_ws_worker command pipeline end-to-end.

This script launches `scripts/gm_ws_worker.py` as a subprocess, then:
1) waits for worker_ready
2) sends ping and expects pong
3) sends subscribe
4) listens for worker_ack / bar / error for a period
5) sends unsubscribe + stop

Usage examples:
    python scripts/debug_worker_pipe.py
    python scripts/debug_worker_pipe.py --symbol DCE.l2605 --frequency tick --count 2
    python scripts/debug_worker_pipe.py --symbol DCE.l --frequency tick --count 2 --run-seconds 30
    python scripts/debug_worker_pipe.py --token YOUR_GM_TOKEN
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

PREFIX = "@@GMWS@@"


@dataclass
class WorkerEvent:
    ts: float
    source: str  # stdout/stderr
    raw: str
    parsed: Optional[Dict[str, Any]] = None


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Debug gm ws worker command pipeline")
    p.add_argument("--symbol", default="DCE.l2605", help="symbol to subscribe")
    p.add_argument("--frequency", default="60s", help="30s / 60s / 1d ...")
    p.add_argument("--count", type=int, default=2, help="window count for subscribe")
    p.add_argument(
        "--ready-timeout",
        type=float,
        default=10.0,
        help="seconds to wait for worker_ready",
    )
    p.add_argument(
        "--run-seconds",
        type=float,
        default=20.0,
        help="seconds to wait for tick/bar after subscribe",
    )
    p.add_argument(
        "--worker-path",
        default="scripts/gm_ws_worker.py",
        help="relative worker script path from project root",
    )
    p.add_argument(
        "--token",
        default="",
        help="optional GM_TOKEN override (otherwise env/config file is used by worker)",
    )
    p.add_argument(
        "--no-subscribe",
        action="store_true",
        help="only test worker startup + ping, skip subscribe flow",
    )
    return p.parse_args()


def reader_stdout(pipe, out_q: "queue.Queue[WorkerEvent]") -> None:
    for line in pipe:
        line = line.rstrip("\n")
        evt = WorkerEvent(ts=time.time(), source="stdout", raw=line, parsed=None)
        if line.startswith(PREFIX):
            payload_str = line[len(PREFIX) :]
            try:
                evt.parsed = json.loads(payload_str)
            except Exception:
                pass
        out_q.put(evt)


def reader_stderr(pipe, out_q: "queue.Queue[WorkerEvent]") -> None:
    for line in pipe:
        line = line.rstrip("\n")
        out_q.put(WorkerEvent(ts=time.time(), source="stderr", raw=line, parsed=None))


def send_cmd(proc: subprocess.Popen, cmd: Dict[str, Any]) -> bool:
    if proc.stdin is None:
        return False
    try:
        proc.stdin.write(json.dumps(cmd, ensure_ascii=False) + "\n")
        proc.stdin.flush()
        print(f"[{now_str()}] -> CMD {cmd}")
        return True
    except Exception as e:
        print(f"[{now_str()}] !! send cmd failed: {e}")
        return False


def wait_for_event(
    out_q: "queue.Queue[WorkerEvent]",
    predicate,
    timeout: float,
    echo: bool = True,
) -> Optional[WorkerEvent]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        remain = max(0.0, deadline - time.time())
        try:
            evt = out_q.get(timeout=min(0.5, remain))
        except queue.Empty:
            continue

        if echo:
            if evt.parsed is not None:
                print(f"[{now_str()}] <- EVT {evt.parsed}")
            elif evt.source == "stderr":
                print(f"[{now_str()}] <- STDERR {evt.raw}")
            else:
                print(f"[{now_str()}] <- RAW {evt.raw}")

        try:
            if predicate(evt):
                return evt
        except Exception:
            pass
    return None


def drain_for_duration(
    out_q: "queue.Queue[WorkerEvent]",
    duration: float,
) -> Dict[str, int]:
    counts = {
        "worker_ack": 0,
        "tick": 0,
        "bar": 0,
        "error": 0,
        "other_json": 0,
        "stderr": 0,
        "raw": 0,
    }
    end_at = time.time() + duration
    while time.time() < end_at:
        timeout = max(0.0, min(0.5, end_at - time.time()))
        try:
            evt = out_q.get(timeout=timeout)
        except queue.Empty:
            continue

        if evt.parsed is not None:
            t = str(evt.parsed.get("type", ""))
            if t in counts:
                counts[t] += 1
            else:
                counts["other_json"] += 1
            print(f"[{now_str()}] <- EVT {evt.parsed}")
        else:
            if evt.source == "stderr":
                counts["stderr"] += 1
                print(f"[{now_str()}] <- STDERR {evt.raw}")
            else:
                counts["raw"] += 1
                print(f"[{now_str()}] <- RAW {evt.raw}")
    return counts


def main() -> int:
    args = parse_args()

    project_root = Path(__file__).resolve().parents[1]
    worker_path = (project_root / args.worker_path).resolve()
    if not worker_path.exists():
        print(f"[{now_str()}] [FAIL] worker script not found: {worker_path}")
        return 2

    env = os.environ.copy()
    if args.token.strip():
        env["GM_TOKEN"] = args.token.strip()

    cmd = [sys.executable, str(worker_path)]
    print(f"[{now_str()}] Starting worker: {cmd}")
    print(f"[{now_str()}] CWD: {project_root}")

    proc = subprocess.Popen(
        cmd,
        cwd=str(project_root),
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    out_q: "queue.Queue[WorkerEvent]" = queue.Queue()

    t_out = threading.Thread(
        target=reader_stdout, args=(proc.stdout, out_q), daemon=True
    )
    t_err = threading.Thread(
        target=reader_stderr, args=(proc.stderr, out_q), daemon=True
    )
    t_out.start()
    t_err.start()

    try:
        # 1) wait ready
        evt_ready = wait_for_event(
            out_q,
            predicate=lambda e: (
                e.parsed is not None and e.parsed.get("type") == "worker_ready"
            ),
            timeout=args.ready_timeout,
        )
        if evt_ready is None:
            rc = proc.poll()
            print(
                f"[{now_str()}] [FAIL] no worker_ready within {args.ready_timeout}s (proc rc={rc})"
            )
            send_cmd(proc, {"op": "stop"})
            return 3

        # 2) ping/pong
        if not send_cmd(proc, {"op": "ping"}):
            return 4

        evt_pong = wait_for_event(
            out_q,
            predicate=lambda e: e.parsed is not None and e.parsed.get("type") == "pong",
            timeout=5.0,
        )
        if evt_pong is None:
            print(f"[{now_str()}] [WARN] no pong received in 5s")

        if args.no_subscribe:
            print(f"[{now_str()}] [PASS] startup + ping flow completed")
            send_cmd(proc, {"op": "stop"})
            return 0

        # 3) subscribe
        sub_cmd = {
            "op": "subscribe",
            "symbol": args.symbol,
            "frequency": args.frequency,
            "count": max(1, int(args.count)),
        }
        if not send_cmd(proc, sub_cmd):
            return 5

        evt_sub_ack = wait_for_event(
            out_q,
            predicate=lambda e: (
                e.parsed is not None
                and e.parsed.get("type") == "worker_ack"
                and e.parsed.get("op") == "subscribe"
            ),
            timeout=8.0,
        )
        if evt_sub_ack is None:
            print(f"[{now_str()}] [WARN] no worker subscribe ack in 8s")

        # 4) receive market events
        print(
            f"[{now_str()}] Listening for events {args.run_seconds}s "
            f"(symbol={args.symbol}, frequency={args.frequency})..."
        )
        counts = drain_for_duration(out_q, args.run_seconds)

        print(f"[{now_str()}] Event summary: {counts}")
        if counts["bar"] == 0:
            print(
                f"[{now_str()}] [WARN] no bar received. "
                "Possible causes: market closed, symbol/frequency unsupported, or subscribe not effective."
            )

        # 5) unsubscribe + stop
        unsub_cmd = {
            "op": "unsubscribe",
            "symbol": args.symbol,
            "frequency": args.frequency,
            "count": max(1, int(args.count)),
        }
        send_cmd(proc, unsub_cmd)
        wait_for_event(
            out_q,
            predicate=lambda e: (
                e.parsed is not None
                and e.parsed.get("type") == "worker_ack"
                and e.parsed.get("op") == "unsubscribe"
            ),
            timeout=5.0,
        )

        send_cmd(proc, {"op": "stop"})
        wait_for_event(
            out_q,
            predicate=lambda e: (
                e.parsed is not None
                and e.parsed.get("type") in {"worker_stopping", "worker_shutdown"}
            ),
            timeout=5.0,
        )

        print(f"[{now_str()}] [DONE] worker pipeline debug completed")
        return 0

    finally:
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
