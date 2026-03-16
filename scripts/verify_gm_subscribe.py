#!/usr/bin/env python3
"""
验证掘金 GM 实时订阅回调脚本（独立测试）。

目标：
1) 读取 GM_TOKEN（环境变量优先，支持 gm_config.json / config/gm_config.json 兜底）
2) 订阅同一标的的 tick 与 bar（默认 60s）
3) 打印 on_tick / on_bar 回调和滑窗数据
4) 对 gm.run 在不同版本的参数差异做兼容尝试

用法示例：
    python scripts/verify_gm_subscribe.py
    python scripts/verify_gm_subscribe.py --symbol SHSE.600519 --bar-frequency 60s --count 2
    python scripts/verify_gm_subscribe.py --symbol DCE.l2605 --bar-frequency 60s --count 3 --unsubscribe-previous
"""

from __future__ import annotations

import argparse
import inspect
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# 官方示例风格：从 gm.api 导入运行时回调相关函数
from gm.api import *  # noqa: F403,F401

# -----------------------------
# 配置读取
# -----------------------------
DEFAULT_CONFIG_PATHS = (
    Path.cwd() / "gm_config.json",
    Path.cwd() / "config" / "gm_config.json",
)


def _load_token_from_file() -> Optional[str]:
    for p in DEFAULT_CONFIG_PATHS:
        if not p.exists():
            continue
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] 配置文件解析失败 {p}: {e}")
            return None

        if not isinstance(raw, dict):
            print(f"[WARN] 配置文件不是 JSON 对象: {p}")
            return None

        token = raw.get("GM_TOKEN")
        if token is None:
            print(f"[WARN] 配置文件缺少 GM_TOKEN: {p}")
            return None

        token = str(token).strip()
        if not token:
            print(f"[WARN] 配置文件中的 GM_TOKEN 为空: {p}")
            return None
        return token

    return None


def resolve_token() -> Optional[str]:
    token = os.getenv("GM_TOKEN")
    if token and token.strip():
        return token.strip()
    return _load_token_from_file()


# -----------------------------
# 日志辅助
# -----------------------------
def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _as_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return str(value)


# -----------------------------
# 运行参数（通过环境变量在 gm.run 可能的脚本重载间传递）
# -----------------------------
ENV_SYMBOL = "GM_SUB_TEST_SYMBOL"
ENV_BAR_FREQUENCY = "GM_SUB_TEST_BAR_FREQUENCY"
ENV_COUNT = "GM_SUB_TEST_COUNT"
ENV_UNSUB_PREV = "GM_SUB_TEST_UNSUB_PREV"

DEFAULT_SYMBOL = "SHSE.600519"
DEFAULT_BAR_FREQUENCY = "60s"
DEFAULT_COUNT = 2
DEFAULT_UNSUB_PREV = False


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _runtime_params() -> Dict[str, Any]:
    symbol = (os.getenv(ENV_SYMBOL) or DEFAULT_SYMBOL).strip() or DEFAULT_SYMBOL
    bar_frequency = (
        os.getenv(ENV_BAR_FREQUENCY) or DEFAULT_BAR_FREQUENCY
    ).strip() or DEFAULT_BAR_FREQUENCY

    count_raw = os.getenv(ENV_COUNT)
    try:
        count = max(1, int(count_raw)) if count_raw is not None else DEFAULT_COUNT
    except Exception:
        count = DEFAULT_COUNT

    unsubscribe_previous = _env_bool(ENV_UNSUB_PREV, DEFAULT_UNSUB_PREV)

    return {
        "symbol": symbol,
        "bar_frequency": bar_frequency,
        "count": count,
        "unsubscribe_previous": unsubscribe_previous,
    }


# -----------------------------
# GM 回调
# -----------------------------
def init(context):
    p = _runtime_params()
    symbol = p["symbol"]
    bar_frequency = p["bar_frequency"]
    count = p["count"]
    unsubscribe_previous = p["unsubscribe_previous"]

    print(f"[{_ts()}] init()")
    print(
        f"[{_ts()}] subscribe bar: symbol={symbol}, frequency={bar_frequency}, "
        f"count={count}, unsubscribe_previous={unsubscribe_previous}"
    )
    subscribe(  # noqa: F405
        symbols=symbol,
        frequency=bar_frequency,
        count=count,
        unsubscribe_previous=unsubscribe_previous,
    )


def on_bar(context, bars):
    p = _runtime_params()
    print(f"[{_ts()}] on_bar  -> {_as_json(bars)}")
    try:
        data = context.data(
            symbol=p["symbol"],
            frequency=p["bar_frequency"],
            count=p["count"],
        )
        print(f"[{_ts()}] window  -> {_as_json(data)}")
    except Exception as e:
        print(f"[{_ts()}] [WARN] context.data 读取滑窗失败: {e}")


def on_error(*args, **kwargs):
    # 不同版本 SDK 的 on_error 参数可能略有差异，用通配参数兜底
    print(f"[{_ts()}] on_error -> args={_as_json(args)}, kwargs={_as_json(kwargs)}")


def shutdown(context):
    p = _runtime_params()
    try:
        unsubscribe(symbols="*", frequency=p["bar_frequency"])  # noqa: F405
        print(f"[{_ts()}] unsubscribe done")
    except Exception as e:
        print(f"[{_ts()}] [WARN] unsubscribe failed: {e}")


# -----------------------------
# run 兼容层
# -----------------------------
def _supported_params() -> set[str]:
    try:
        sig = inspect.signature(run)  # noqa: F405
        return set(sig.parameters.keys())
    except Exception:
        return set()


def _filter_kwargs(kwargs: Dict[str, Any], supported: set[str]) -> Dict[str, Any]:
    if not supported:
        return kwargs
    return {k: v for k, v in kwargs.items() if k in supported}


def _build_run_candidates(token: str) -> List[Dict[str, Any]]:
    supported = _supported_params()
    file_abs = str(Path(__file__).resolve())
    file_name = Path(__file__).name

    # mode 常量可能不存在（极少数情况），做保护
    try:
        live_mode = MODE_LIVE  # noqa: F405
    except Exception:
        live_mode = None

    base: Dict[str, Any] = {
        "strategy_id": "verify_gm_subscribe",
        "token": token,
    }
    if live_mode is not None:
        base["mode"] = live_mode

    filename_keys = ("filename", "file", "script", "strategy_file")
    package_keys = ("package", "module", "strategy_module")

    # 组合多种候选，按“最常见 -> 兜底”顺序尝试
    candidates: List[Dict[str, Any]] = []

    # 1) 仅基础参数
    candidates.append(dict(base))

    # 2) 加绝对路径 filename
    for fk in filename_keys:
        kw = dict(base)
        kw[fk] = file_abs
        candidates.append(kw)

    # 3) 加文件名 filename
    for fk in filename_keys:
        kw = dict(base)
        kw[fk] = file_name
        candidates.append(kw)

    # 4) filename + package 兜底
    package_values = (None, "", "__main__")
    for fk in filename_keys:
        for pk in package_keys:
            for pv in package_values:
                kw = dict(base)
                kw[fk] = file_abs
                kw[pk] = pv
                candidates.append(kw)

    # 5) 再尝试 file_name + package
    for fk in filename_keys:
        for pk in package_keys:
            for pv in package_values:
                kw = dict(base)
                kw[fk] = file_name
                kw[pk] = pv
                candidates.append(kw)

    # 按签名过滤 + 去重
    seen = set()
    filtered: List[Dict[str, Any]] = []
    for cand in candidates:
        c = _filter_kwargs(cand, supported)
        key = tuple(sorted(c.items(), key=lambda x: x[0]))
        if key in seen:
            continue
        seen.add(key)
        filtered.append(c)

    return filtered


def run_with_fallbacks(token: str) -> None:
    errors: List[str] = []
    candidates = _build_run_candidates(token)

    for idx, kwargs in enumerate(candidates, start=1):
        try:
            print(f"[{_ts()}] try run candidate #{idx}: {kwargs}")
            run(**kwargs)  # noqa: F405
            return
        except KeyboardInterrupt:
            raise
        except Exception as e:
            errors.append(f"candidate#{idx} kwargs={kwargs} -> {e}")

    joined = "\n".join(errors[-10:])
    raise RuntimeError(f"所有 run 候选都失败，最近错误如下：\n{joined}")


# -----------------------------
# CLI
# -----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="验证 GM 实时订阅回调")
    p.add_argument("--symbol", default="SHSE.600519", help="测试标的")
    p.add_argument("--bar-frequency", default="60s", help="bar 频率，例如 60s / 1d")
    p.add_argument("--count", type=int, default=2, help="滑窗大小")
    p.add_argument(
        "--unsubscribe-previous",
        action="store_true",
        help="订阅时是否取消此前订阅",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    symbol = (args.symbol or DEFAULT_SYMBOL).strip() or DEFAULT_SYMBOL
    bar_frequency = (
        args.bar_frequency or DEFAULT_BAR_FREQUENCY
    ).strip() or DEFAULT_BAR_FREQUENCY
    count = max(1, int(args.count))
    unsubscribe_previous = bool(args.unsubscribe_previous)

    # 关键：写入环境变量，确保 gm.run 若重载脚本时仍能拿到 CLI 参数
    os.environ[ENV_SYMBOL] = symbol
    os.environ[ENV_BAR_FREQUENCY] = bar_frequency
    os.environ[ENV_COUNT] = str(count)
    os.environ[ENV_UNSUB_PREV] = "1" if unsubscribe_previous else "0"

    token = resolve_token()
    if not token:
        print(
            "[FAIL] GM_TOKEN 未设置。请配置环境变量 GM_TOKEN，"
            "或在 gm_config.json/config/gm_config.json 中设置 GM_TOKEN。"
        )
        return 1

    try:
        set_token(token)  # noqa: F405
    except Exception as e:
        print(f"[FAIL] set_token 失败: {e}")
        return 2

    print("== GM Subscribe Verify ==")
    print(f"symbol={symbol}")
    print(f"bar_frequency={bar_frequency}")
    print(f"count={count}")
    print(f"unsubscribe_previous={unsubscribe_previous}")
    print("等待回调中，按 Ctrl+C 结束...")

    # gm.run 可能自行解析 sys.argv（与我们自定义参数冲突），先清空额外参数
    sys.argv = [sys.argv[0]]

    try:
        run_with_fallbacks(token)
    except KeyboardInterrupt:
        print(f"\n[{_ts()}] 收到 Ctrl+C，退出。")
    except Exception as e:
        print(f"[FAIL] run 执行失败: {e}")
        return 3

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
