# dags/tasks/stockData/insertMetaData.py

import psycopg2
from airflow.hooks.base import BaseHook

def insert_metadata(**kwargs):
    ti = kwargs['ti']
    fetched_metadata = ti.xcom_pull(task_ids='fetch_metadata', key='metadata')

    if not fetched_metadata:
        print("No metadata found from XCom.")
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
    for record in fetched_metadata:
        cursor.execute("""
            INSERT INTO stocks (symbol, name, sector, industry, isin, exchange, market_cap, pe_ratio, pb_ratio, dividend_yield, roe, debt_to_equity, earnings_growth)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE
            SET name = EXCLUDED.name,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                isin = EXCLUDED.isin,
                exchange = EXCLUDED.exchange,
                market_cap = EXCLUDED.market_cap, 
                pe_ratio = EXCLUDED.pe_ratio, 
                pb_ratio = EXCLUDED.pb_ratio, 
                dividend_yield = EXCLUDED.dividend_yield, 
                roe = EXCLUDED.roe, 
                debt_to_equity = EXCLUDED.debt_to_equity, 
                earnings_growth = EXCLUDED.earnings_growth;
        """, (
            record.get('symbol'),
            record.get('name'),
            record.get('sector'),
            record.get('industry'),
            record.get('isin'),
            record.get('exchange'),
            record.get('market_cap'),
            record.get('pe_ratio'),
            record.get('pb_ratio'),
            record.get('dividend_yield'),
            record.get('roe'),
            record.get('debt_to_equity'),
            record.get('earnings_growth')
        ))
    conn.commit()
    cursor.close()
    conn.close()
