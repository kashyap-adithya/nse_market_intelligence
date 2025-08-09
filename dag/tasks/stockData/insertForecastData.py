from airflow.providers.postgres.hooks.postgres import PostgresHook

def insert_forecasts(**kwargs):
    """
    Inserts forecasted stock prices into the 'stock_forecasts' table.
    Uses explicit ON CONFLICT clause for upserts.
    """
    ti = kwargs['ti']
    forecasts = ti.xcom_pull(task_ids='forecast_prices', key='forecasts')

    if not forecasts:
        print("No forecast data found in XCom.")
        return

    # Prepare data rows
    records = [
        (f['symbol'], f['date'], f['predicted_close'])
        for f in forecasts
    ]

    insert_sql = """
        INSERT INTO stock_forecasts (symbol, date, predicted_close)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET
            predicted_close = EXCLUDED.predicted_close;
    """

    hook = PostgresHook(postgres_conn_id='stock_postgres_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.executemany(insert_sql, records)
        conn.commit()
        print(f"Inserted/updated {len(records)} forecast records into stock_forecasts.")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting forecast records: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
