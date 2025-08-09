import psycopg2
from airflow.hooks.base import BaseHook
import json

def insert_stock_suggestions(**kwargs):
    ti = kwargs["ti"]
    suggestions = ti.xcom_pull(task_ids='suggestion_gemini', key='stock_suggestions')

    if not suggestions:
        print("[ERROR] No Gemini suggestions found to insert.")
        return

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
        INSERT INTO stock_suggestions (symbol, date, suggestion, reason, sector ,related_stocks)
        VALUES (%(symbol)s, %(date)s, %(suggestion)s, %(reason)s, %(sector)s, %(related_stocks)s)
        ON CONFLICT (symbol, date) DO UPDATE SET
            suggestion = EXCLUDED.suggestion,
            reason = EXCLUDED.reason,
            related_stocks = EXCLUDED.related_stocks;
    """

    for suggestion in suggestions:
        try:
            # Convert related_stocks to comma-separated string if it's a list
            if isinstance(suggestion.get("related_stocks"), list):
                suggestion["related_stocks"] = ", ".join(suggestion["related_stocks"])

            cursor.execute(insert_sql, suggestion)
        except Exception as e:
            print(f"[ERROR] Failed to insert suggestion for {suggestion.get('symbol')} on {suggestion.get('date')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("[SUCCESS] Gemini stock suggestions inserted into database.")
