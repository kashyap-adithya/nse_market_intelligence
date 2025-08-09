# dags/tasks/stockData/insertPriceData.py

import psycopg2
from airflow.hooks.base import BaseHook

def insert_price_data(**kwargs):
    ti = kwargs['ti']
    price_data = ti.xcom_pull(task_ids='fetch_price_history')

    if not price_data:
        print("[ERROR] No price data found in XCom. Exiting insert.")
        return

    print(f"[DEBUG] Inserting {len(price_data)} rows into price_history...")

    conn_id = 'stock_postgres_connection'
    connection = BaseHook.get_connection(conn_id)

    try:
        conn = psycopg2.connect(
            host=connection.host,
            port=connection.port,
            user=connection.login,
            password=connection.password,
            dbname=connection.schema
        )
        cursor = conn.cursor()

        for row in price_data:
            cursor.execute("""
                INSERT INTO price_history (symbol, date, open, close, high, low, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO UPDATE
                SET open = EXCLUDED.open,
                    close = EXCLUDED.close,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    volume = EXCLUDED.volume;
            """, (
                row['symbol'],
                row['date'],
                row['open'],
                row['close'],
                row['high'],
                row['low'],
                row['volume']
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print("[INFO] Successfully inserted price history data into DB.")

    except Exception as e:
        print(f"[ERROR] Failed to insert price data: {e}")
