from udf_service.models import SymbolInfo


def test_search_matches_name_fullname_and_ticker_case_insensitive(client, set_symbols):
    symbols = [
        SymbolInfo(
            name="SHSE.600000",
            full_name="浦发银行",
            ticker="PFYH",
            description="bank",
            type="stock",
            session="SSE",
        ),
        SymbolInfo(
            name="SZSE.000001",
            full_name="平安银行",
            ticker="PAYH",
            description="bank",
            type="stock",
            session="SZSE",
        ),
        SymbolInfo(
            name="JQBTC",
            full_name="掘金-示例BTC",
            ticker="BTC",
            description="crypto",
            type="crypto",
            session="24x7",
        ),
    ]
    set_symbols(symbols)

    # match by ticker (case-insensitive)
    r1 = client.get("/search", params={"query": "btc"})
    assert r1.status_code == 200
    data1 = r1.json()
    assert len(data1) == 1
    assert data1[0]["symbol"] == "JQBTC"

    # match by full_name substring
    r2 = client.get("/search", params={"query": "平安"})
    assert r2.status_code == 200
    data2 = r2.json()
    assert len(data2) == 1
    assert data2[0]["symbol"] == "SZSE.000001"

    # match by name
    r3 = client.get("/search", params={"query": "600000"})
    assert r3.status_code == 200
    data3 = r3.json()
    assert len(data3) == 1
    assert data3[0]["symbol"] == "SHSE.600000"


def test_search_respects_limit(client, set_symbols):
    symbols = [
        SymbolInfo(name=f"SYM{i}", full_name=f"Symbol {i}", ticker=f"T{i}")
        for i in range(10)
    ]
    set_symbols(symbols)

    resp = client.get("/search", params={"query": "sym", "limit": 3})
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 3
    assert [item["symbol"] for item in data] == ["SYM0", "SYM1", "SYM2"]


def test_search_empty_query_returns_first_n_by_limit(client, set_symbols):
    symbols = [
        SymbolInfo(name=f"AAA{i}", full_name=f"Name {i}", ticker=f"TK{i}")
        for i in range(5)
    ]
    set_symbols(symbols)

    resp = client.get("/search", params={"query": "", "limit": 2})
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert [item["symbol"] for item in data] == ["AAA0", "AAA1"]


def test_search_returns_empty_when_no_match(client, set_symbols):
    symbols = [
        SymbolInfo(name="SHSE.600000", full_name="浦发银行", ticker="PFYH"),
        SymbolInfo(name="SZSE.000001", full_name="平安银行", ticker="PAYH"),
    ]
    set_symbols(symbols)

    resp = client.get("/search", params={"query": "not-found"})
    assert resp.status_code == 200
    assert resp.json() == []


def test_search_response_field_mapping(client, set_symbols):
    symbols = [
        SymbolInfo(
            name="JQBTC",
            full_name="掘金-示例BTC",
            ticker="BTC",
            description=None,  # should become empty string in response
            type="crypto",
            session="24x7",  # mapped to exchange
        )
    ]
    set_symbols(symbols)

    resp = client.get("/search", params={"query": "btc"})
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    item = data[0]
    assert item["symbol"] == "JQBTC"
    assert item["full_name"] == "掘金-示例BTC"
    assert item["ticker"] == "BTC"
    assert item["type"] == "crypto"
    assert item["exchange"] == "24x7"
    assert item["description"] == ""
