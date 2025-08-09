import pandas as pd
import pandas_ta as ta

def enrich_price_data(**kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    ti = kwargs['ti']
    
    price_data = ti.xcom_pull(task_ids='fetch_price_history')  # key is 'return_value' by default

    df = pd.DataFrame(price_data)
    df['date'] = pd.to_datetime(df['date'])

    enriched_all = []

    for symbol, group in df.groupby("symbol"):
        group = group.sort_values("date").reset_index(drop=True)

        # Calculate technical indicators
        group.ta.sma(length=20, append=True)
        group.ta.ema(length=12, append=True)
        group.ta.ema(length=26, append=True)
        group.ta.macd(append=True)
        group.ta.rsi(length=14, append=True)
        group.ta.bbands(length=20, append=True)
        group.ta.atr(length=14, append=True)
        group.ta.obv(append=True)
        group.ta.mfi(length=14, append=True)

        # Drop rows where major indicators are NaN
        group.dropna(subset=[
            "SMA_20", "EMA_12", "EMA_26", "MACD_12_26_9", "RSI_14",
            "BBL_20_2.0", "ATRr_14", "OBV", "MFI_14"
        ], inplace=True)

        enriched_all.append(group)

    enriched_df = pd.concat(enriched_all).reset_index(drop=True)

    # Replace NaN with None for PostgreSQL compatibility
    enriched_df = enriched_df.where(pd.notnull(enriched_df), None)

    enriched_df['date'] = enriched_df['date'].dt.strftime('%Y-%m-%d')

    # Push to XCom
    enriched_records = enriched_df.to_dict(orient="records")
    ti.xcom_push(key="enriched_price_data", value=enriched_records)
