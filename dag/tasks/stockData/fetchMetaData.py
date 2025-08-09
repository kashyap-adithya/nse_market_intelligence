import yfinance as yf

def fetch_stock_metadata(tickers, **kwargs):
    metadata = []

    for symbol in tickers:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            metadata.append({
                'symbol': symbol,
                'name': info.get('longName', ''),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'isin': info.get('isin', ''),
                'exchange': info.get('exchange', ''),
                'pe_ratio': info.get('trailingPE',0),
                'pb_ratio': info.get('priceToBook',0),
                'dividend_yield': info.get('dividendYield',0),
                'roe': info.get('returnOnEquity',0),
                'debt_to_equity': info.get('debtToEquity',0),
                'earnings_growth': info.get('earningsGrowth',0)
            })
        except Exception as e:
            print(f"Error fetching metadata for {symbol}: {e}")

    # Push to XCom
    kwargs['ti'].xcom_push(key='metadata', value=metadata)
    print(f"Metadata pushed to XCom: {metadata}")
