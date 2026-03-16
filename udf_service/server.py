"""TradingView UDF server implementation."""

from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .juejin_client import JuejinClient
from .models import HistoryResponse, SearchResult, SymbolInfo

app = FastAPI(title="quant-udf")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
_client = JuejinClient()


@app.get("/config")
def config() -> dict:
    """Return TradingView UDF config."""
    return {
        "supports_search": True,
        "supports_group_request": False,
        "supported_resolutions": [
            "1",
            "5",
            "15",
            "30",
            "60",
            "D",
            "W",
        ],
        "supports_marks": False,
        "supports_time": True,
    }


@app.get("/symbols")
def symbols(
    symbol: str = Query(..., description="Symbol identifier, e.g. BTC"),
) -> SymbolInfo:
    """Return symbol information for TradingView."""
    candidates = [
        s
        for s in _client.symbols()
        if s.ticker.upper() == symbol.upper() or s.name == symbol
    ]
    if not candidates:
        raise HTTPException(status_code=404, detail=f"Symbol not found: {symbol}")
    return candidates[0]


@app.get("/search")
def search(
    query: str = Query("", description="Search term"),
    limit: int = Query(30, ge=1, le=100),
    exchange: Optional[str] = None,
    symbol: Optional[str] = None,
) -> List[SearchResult]:
    """Search symbols."""
    symbols = _client.symbols()
    results: List[SearchResult] = []

    query_lower = query.strip().lower()
    for s in symbols:
        if (
            query_lower in s.name.lower()
            or query_lower in s.full_name.lower()
            or query_lower in (s.ticker or "").lower()
        ):
            results.append(
                SearchResult(
                    symbol=s.name,
                    full_name=s.full_name,
                    description=s.description or "",
                    exchange=s.session,
                    ticker=s.ticker,
                    type=s.type,
                )
            )
            if len(results) >= limit:
                break

    return results


@app.get("/history")
def history(
    symbol: str = Query(..., description="Symbol identifier"),
    resolution: str = Query(
        "D", description="Resolution (e.g. 1, 5, 15, 30, 60, D, W)"
    ),
    _from: int = Query(..., alias="from", description="From timestamp (seconds)"),
    to: int = Query(..., description="To timestamp (seconds)"),
    count_back: Optional[int] = Query(
        None, alias="countback", description="Number of bars to return"
    ),
) -> JSONResponse:
    """Fetch historical bars."""
    data: HistoryResponse = _client.get_history(symbol, resolution, _from, to)
    if data.s != "ok":
        raise HTTPException(status_code=502, detail="Failed to fetch history data")

    # TradingView expects `s=ok|no_data|error` and all arrays of same length or null.
    return JSONResponse(content=data.dict())


@app.get("/time")
def time() -> dict:
    """Return server time (ms)."""
    return {"unixtime": int(__import__("time").time())}
