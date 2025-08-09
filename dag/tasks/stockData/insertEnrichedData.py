import psycopg2
import pandas as pd
from airflow.hooks.base import BaseHook
from datetime import datetime
import math

def clean_row(row):
    """Sanitize row for SQL insert: convert NaN to None, timestamps to date."""
    cleaned = {}
    for k, v in row.items():
        if isinstance(v, float) and (math.isnan(v) or v in (float("inf"), float("-inf"))):
            cleaned[k] = None
        elif isinstance(v, (datetime, pd.Timestamp)):
            cleaned[k] = v.date()
        else:
            cleaned[k] = v
    return cleaned

def convert_keys_dot_to_underscore(data):
    """Replace dot in keys with underscore."""
    if isinstance(data, dict):
        return {
            k.replace('.', '_'): convert_keys_dot_to_underscore(v)
            for k, v in data.items()
        }
    elif isinstance(data, list):
        return [convert_keys_dot_to_underscore(item) for item in data]
    else:
        return data

def insert_enriched_price_data(**kwargs):
    ti = kwargs['ti']
    enriched_data = ti.xcom_pull(task_ids='enrich_data', key='enriched_price_data')

    if not enriched_data:
        print("[ERROR] No enriched data found.")
        return

    # Replace dots in column names with underscores
    enriched_data = convert_keys_dot_to_underscore(enriched_data)

    conn_id = 'stock_postgres_connection'
    connection = BaseHook.get_connection(conn_id)

    conn = psycopg2.connect(
        host=connection.host,
        port=connection.port,
        user=connection.login,
        password=connection.password,
        dbname=connection.schema
    )
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO enriched_price_history (
            symbol, date, open, high, low, close, volume,
            SMA_20, EMA_12, EMA_26,
            MACD_12_26_9, MACDh_12_26_9, MACDs_12_26_9,
            RSI_14, BBL_20_2_0, BBM_20_2_0, BBU_20_2_0,
            ATRr_14, OBV, MFI_14
        )
        VALUES (
            %(symbol)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s,
            %(SMA_20)s, %(EMA_12)s, %(EMA_26)s,
            %(MACD_12_26_9)s, %(MACDh_12_26_9)s, %(MACDs_12_26_9)s,
            %(RSI_14)s, %(BBL_20_2_0)s, %(BBM_20_2_0)s, %(BBU_20_2_0)s,
            %(ATRr_14)s, %(OBV)s, %(MFI_14)s
        )
        ON CONFLICT (symbol, date) DO UPDATE SET
            SMA_20 = EXCLUDED.SMA_20,
            EMA_12 = EXCLUDED.EMA_12,
            EMA_26 = EXCLUDED.EMA_26,
            MACD_12_26_9 = EXCLUDED.MACD_12_26_9,
            MACDh_12_26_9 = EXCLUDED.MACDh_12_26_9,
            MACDs_12_26_9 = EXCLUDED.MACDs_12_26_9,
            RSI_14 = EXCLUDED.RSI_14,
            BBL_20_2_0 = EXCLUDED.BBL_20_2_0,
            BBM_20_2_0 = EXCLUDED.BBM_20_2_0,
            BBU_20_2_0 = EXCLUDED.BBU_20_2_0,
            ATRr_14 = EXCLUDED.ATRr_14,
            OBV = EXCLUDED.OBV,
            MFI_14 = EXCLUDED.MFI_14;
    """

    for row in enriched_data:
        try:
            clean = clean_row(row)
            cursor.execute(insert_sql, clean)
        except Exception as e:
            print(f"[ERROR] Failed to insert row for {row.get('symbol')} on {row.get('date')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("[SUCCESS] Enriched data inserted successfully.")
