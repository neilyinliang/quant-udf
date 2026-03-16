"""A thin adapter around 掘金 SDK (`gm`) to provide market data for the UDF server.

GM token resolution priority:
1) Environment variable: GM_TOKEN
2) Default config file (JSON): ./gm_config.json
3) Default config file (JSON): ./config/gm_config.json

If no token is configured, the adapter falls back to stub responses so the UDF service stays responsive.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import gm.api as gm_api
from gm.api import query
from gm.api._errors import GmError

from .models import HistoryResponse, SymbolInfo

logger = logging.getLogger(__name__)

_DEFAULT_GM_CONFIG_PATHS = (
    Path.cwd() / "gm_config.json",
    Path.cwd() / "config" / "gm_config.json",
)


def _load_gm_token_from_default_config() -> Optional[str]:
    """Load GM_TOKEN from default config files if present."""
    for path in _DEFAULT_GM_CONFIG_PATHS:
        if not path.exists():
            continue

        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("Failed to parse GM config file %s: %s", path, e)
            return None

        if not isinstance(raw, dict):
            logger.warning("GM config file is not a JSON object: %s", path)
            return None

        token = raw.get("GM_TOKEN")
        if token is None:
            logger.warning("GM_TOKEN not found in config file: %s", path)
            return None

        token_str = str(token).strip()
        if not token_str:
            logger.warning("GM_TOKEN is empty in config file: %s", path)
            return None

        return token_str

    return None


def _get_gm_token() -> Optional[str]:
    """Get GM_TOKEN from env first, then default config files."""
    token = os.getenv("GM_TOKEN")
    if token and token.strip():
        return token.strip()
    return _load_gm_token_from_default_config()


def _resolution_to_gm_frequency(resolution: str) -> Optional[str]:
    """Convert TradingView resolution (e.g. 1, 5, D, W) into gm frequency."""
    if resolution.isdigit():
        return f"{int(resolution)}m"
    if resolution == "D":
        return "1d"
    if resolution == "W":
        return "1w"
    return None


def _resolution_to_seconds(resolution: str) -> Optional[int]:
    """Convert TradingView resolution strings to seconds."""
    if resolution.isdigit():
        return int(resolution) * 60
    if resolution == "D":
        return 24 * 60 * 60
    if resolution == "W":
        return 7 * 24 * 60 * 60
    return None


def _series_to_unix_seconds(series) -> List[int]:
    """Convert numeric/datetime pandas series to UNIX seconds."""
    dtype_str = str(series.dtype)
    if dtype_str.startswith("datetime64"):
        return (series.astype("int64") // 1_000_000_000).astype(int).tolist()
    return series.astype(int).tolist()


def _symbol_has_digit(symbol: str) -> bool:
    """Check if symbol contains any digit."""
    return bool(re.search(r"\d", symbol or ""))


class JuejinClient:
    """Client that talks to 掘金 SDK and returns data in TradingView UDF format."""

    def __init__(self) -> None:
        self._configured = False
        self._init_gm_sdk()

    def _init_gm_sdk(self) -> None:
        token = _get_gm_token()
        if not token:
            logger.warning(
                "GM_TOKEN is not set in env or default config files; "
                "UDF service will return stub data."
            )
            return

        try:
            gm_api.set_token(token)
            self._configured = True
        except Exception as e:
            logger.warning("Failed to configure gm SDK with GM_TOKEN: %s", e)

    def _resolve_main_contract_symbol(self, symbol: str) -> str:
        """Resolve non-numeric futures symbol to current main contract symbol."""
        if _symbol_has_digit(symbol):
            return symbol

        try:
            contracts = gm_api.fut_get_continuous_contracts(csymbol=symbol)
        except Exception as e:
            logger.warning(
                "Failed to resolve continuous contract for %s, fallback to original symbol: %s",
                symbol,
                e,
            )
            return symbol

        if not contracts:
            logger.warning(
                "No continuous contract returned for %s, fallback to original symbol",
                symbol,
            )
            return symbol

        first = contracts[0]
        resolved_symbol = (
            first.get("symbol")
            if isinstance(first, dict)
            else getattr(first, "symbol", None)
        )
        if isinstance(resolved_symbol, str) and resolved_symbol.strip():
            mapped = resolved_symbol.strip()
            if mapped != symbol:
                logger.info(
                    "Resolved main contract symbol: %s -> %s",
                    symbol,
                    mapped,
                )
            return mapped

        logger.warning(
            "Continuous contract payload missing `symbol` for %s, fallback to original symbol",
            symbol,
        )
        return symbol

    def symbols(self) -> List[SymbolInfo]:
        """Return available symbols."""
        if not self._configured:
            # Return stub symbol so TradingView can render something.
            return [
                SymbolInfo(
                    name="STUB.JQBTC",
                    full_name="STUB.JQBTC",
                    ticker="STUB.JQBTC",
                    description="[STUB] 示例符号 (请设置 GM_TOKEN 并配置掘金 SDK)",
                    type="crypto",
                    exchange="STUB",
                    session="24x7",
                    timezone="UTC",
                )
            ]

        try:
            df = query.get_instruments(df=True)
            if getattr(df, "empty", True):
                return []

            result: List[SymbolInfo] = []
            for row in df.to_dict(orient="records"):
                symbol = row.get("symbol")
                if not symbol:
                    continue

                name = row.get("name") or symbol
                sec_type = row.get("sec_type") or "stock"
                exchange = str(row.get("exchange", "")).strip()
                session = (
                    str(row.get("trade_time", "")).strip()
                    or str(row.get("session", "")).strip()
                    or "0900-1700"
                )
                description = str(row.get("name", "")).strip()
                if exchange:
                    description = f"[{exchange}] {description}".strip()

                result.append(
                    SymbolInfo(
                        name=symbol,
                        full_name=name,
                        ticker=symbol,
                        description=description,
                        type=str(sec_type),
                        exchange=exchange,
                        session=session,
                        timezone="Asia/Shanghai",
                    )
                )

            return result
        except Exception as e:
            logger.warning("Failed to fetch symbols from gm SDK: %s", e)
            return []

    def get_history(
        self,
        symbol: str,
        resolution: str,
        from_ts: int,
        to_ts: int,
    ) -> HistoryResponse:
        """Fetch historical bars for the given symbol.

        `from_ts` and `to_ts` are UNIX timestamps (seconds since epoch).
        """
        if not self._configured:
            return self._stub_history(resolution)

        frequency = _resolution_to_gm_frequency(resolution)
        if not frequency:
            return HistoryResponse(
                s="error", t=None, o=None, h=None, l=None, c=None, v=None
            )

        start_time = datetime.fromtimestamp(from_ts, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_time = datetime.fromtimestamp(to_ts, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        resolved_symbol = self._resolve_main_contract_symbol(symbol)

        try:
            df = query.history(
                resolved_symbol,
                frequency,
                start_time,
                end_time,
                fields="symbol,frequency,eob,open,high,low,close,volume",
                df=True,
            )
        except GmError as e:
            logger.warning("gm SDK history query failed: %s", e)
            return HistoryResponse(
                s="error", t=None, o=None, h=None, l=None, c=None, v=None
            )
        except Exception as e:
            logger.warning("Unexpected error querying gm history: %s", e)
            return HistoryResponse(
                s="error", t=None, o=None, h=None, l=None, c=None, v=None
            )

        if getattr(df, "empty", True):
            return HistoryResponse(
                s="no_data", t=None, o=None, h=None, l=None, c=None, v=None
            )

        # Ensure output is sorted by time
        if "eob" in df.columns:
            df = df.sort_values(by="eob")
            t_values = _series_to_unix_seconds(df["eob"])
        elif "timestamp" in df.columns:
            df = df.sort_values(by="timestamp")
            t_values = _series_to_unix_seconds(df["timestamp"])
        else:
            # Fallback: use row indices
            t_values = list(range(len(df)))

        return HistoryResponse(
            s="ok",
            t=t_values,
            o=df.get("open").astype(float).tolist() if "open" in df.columns else None,
            h=df.get("high").astype(float).tolist() if "high" in df.columns else None,
            l=df.get("low").astype(float).tolist() if "low" in df.columns else None,
            c=df.get("close").astype(float).tolist() if "close" in df.columns else None,
            v=df.get("volume").astype(float).tolist()
            if "volume" in df.columns
            else None,
        )

    def _stub_history(self, resolution: str) -> HistoryResponse:
        """Return deterministic stub data when GM is not configured."""
        now = datetime.now(timezone.utc).timestamp()
        interval_sec = _resolution_to_seconds(resolution)
        if interval_sec is None:
            return HistoryResponse(
                s="error", t=None, o=None, h=None, l=None, c=None, v=None
            )

        t, o, h, low_values, c, v = [], [], [], [], [], []
        for i in range(10):
            ts = int(now - (9 - i) * interval_sec)
            t.append(ts)
            o.append(100 + i)
            h.append(100 + i + 2)
            low_values.append(100 + i - 2)
            c.append(100 + i + 1)
            v.append(1000 + i * 10)

        return HistoryResponse(s="ok", t=t, o=o, h=h, l=low_values, c=c, v=v)
