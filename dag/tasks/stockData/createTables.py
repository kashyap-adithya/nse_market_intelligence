# dags/stock_data/create_tables.py

CREATE_STOCKS_SQL = """
CREATE TABLE IF NOT EXISTS stocks (
    stock_id SERIAL PRIMARY KEY,
    symbol TEXT UNIQUE NOT NULL,
    name TEXT,
    sector TEXT,
    industry TEXT,
    isin TEXT,
    exchange TEXT,
    market_cap BIGINT,
    pe_ratio NUMERIC,
    pb_ratio NUMERIC,
    dividend_yield NUMERIC,
    roe NUMERIC,
    debt_to_equity NUMERIC,
    earnings_growth NUMERIC
);
"""

CREATE_PRICE_SQL = """
CREATE TABLE IF NOT EXISTS price_history (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    open NUMERIC,
    close NUMERIC,
    high NUMERIC,
    low NUMERIC,
    volume BIGINT,
    PRIMARY KEY (symbol, date)
);
"""

CREATE_FORECAST_SQL = """
CREATE TABLE IF NOT EXISTS stock_forecasts (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    predicted_close FLOAT,
    PRIMARY KEY (symbol, date) 
);
"""

CREATE_ENRICHED_DATA = """
CREATE TABLE IF NOT EXISTS enriched_price_history (
    symbol TEXT,
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    SMA_20 FLOAT,
    EMA_12 FLOAT,
    EMA_26 FLOAT,
    MACD_12_26_9 FLOAT,
    MACDh_12_26_9 FLOAT,
    MACDs_12_26_9 FLOAT,
    RSI_14 FLOAT,
    BBL_20_2_0 FLOAT,
    BBM_20_2_0 FLOAT,
    BBU_20_2_0 FLOAT,
    ATRr_14 FLOAT,
    OBV FLOAT,
    MFI_14 FLOAT,
    PRIMARY KEY (symbol, date)
);
"""

CREATE_SUGGESTION_DATA = """
CREATE TABLE IF NOT EXISTS stock_suggestions (
    symbol TEXT,
    date DATE,
    suggestion TEXT,
    reason TEXT,
    sector TEXT,
    related_stocks TEXT,
    PRIMARY KEY (symbol, date)
);
"""