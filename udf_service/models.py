from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class Resolution(str, Enum):
    """Common TradingView resolutions."""

    MINUTE = "1"
    FIVE = "5"
    FIFTEEN = "15"
    THIRTY = "30"
    HOUR = "60"
    DAY = "D"
    WEEK = "W"


class SymbolInfo(BaseModel):
    """TradingView symbol metadata."""

    name: str
    full_name: str
    ticker: str
    description: Optional[str] = None
    type: str = "stock"
    session: str = "0900-1700"
    timezone: str = "Asia/Shanghai"
    supported_resolutions: List[str] = [Resolution.MINUTE, Resolution.FIVE, Resolution.FIFTEEN, Resolution.THIRTY, Resolution.HOUR, Resolution.DAY, Resolution.WEEK]
    has_intraday: bool = True
    has_daily: bool = True
    has_no_volume: bool = False


class HistoryResponse(BaseModel):
    s: str
    t: Optional[List[int]]
    o: Optional[List[float]]
    h: Optional[List[float]]
    l: Optional[List[float]]
    c: Optional[List[float]]
    v: Optional[List[float]]
    next_time: Optional[int] = None


class SearchResult(BaseModel):
    symbol: str
    full_name: str
    description: str
    exchange: str
    ticker: str
    type: str
