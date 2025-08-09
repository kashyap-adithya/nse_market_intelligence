def fetch_price_history(tickers, **kwargs):
    results = []

    import yfinance as yf
    for symbol in tickers:
        try:
            data = yf.Ticker(symbol).history(period="500d", interval="1d")
            for index, row in data.iterrows():
                results.append({
                    "symbol": symbol,
                    "date": index.strftime('%Y-%m-%d'),
                    "open": row["Open"],
                    "close": row["Close"],
                    "high": row["High"],
                    "low": row["Low"],
                    "volume": row["Volume"]
                })
        except Exception as e:
            print(f"Failed to fetch price history for {symbol}: {e}")

    print(f"Fetched price history: {results[:3]}...")  # log a few rows
    return results  # <-- return to push to XCom
