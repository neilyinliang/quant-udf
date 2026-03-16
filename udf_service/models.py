from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


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
    exchange: str = ""
    session: str = "0900-1700"
    timezone: str = "Asia/Shanghai"
    supported_resolutions: List[str] = Field(
        default_factory=lambda: [
            Resolution.MINUTE.value,
            Resolution.FIVE.value,
            Resolution.FIFTEEN.value,
            Resolution.THIRTY.value,
            Resolution.HOUR.value,
            Resolution.DAY.value,
            Resolution.WEEK.value,
        ]
    )
    has_intraday: bool = True
    has_daily: bool = True
    has_no_volume: bool = False


class HistoryResponse(BaseModel):
    s: str
    t: Optional[List[int]]
    o: Optional[List[float]]
    h: Optional[List[float]]
    l: Optional[List[float]]  # noqa: E741 - UDF protocol field name
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
