# stock_data_dag.py
import json

from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.param import Param

# Import your task functions and table DDLs
from tasks.stockData.fetchMetaData import fetch_stock_metadata
from tasks.stockData.insertMetaData import insert_metadata
from tasks.stockData.fetchPriceHistory import fetch_price_history
from tasks.stockData.insertPriceData import insert_price_data
from tasks.stockData.forecastPrices import forecast_prices
from tasks.stockData.insertForecastData import insert_forecasts
from tasks.stockData.calculateIndicators import enrich_price_data
from tasks.stockData.insertEnrichedData import insert_enriched_price_data
from tasks.stockData.geminiSuggestion import get_stock_suggestion_gemini
from tasks.stockData.insertSuggestion import insert_stock_suggestions
from tasks.stockData.createTables import CREATE_STOCKS_SQL, CREATE_PRICE_SQL, CREATE_FORECAST_SQL, CREATE_ENRICHED_DATA, CREATE_SUGGESTION_DATA


def load_tickers():
    config_path = Path(__file__).parent / "config" / "tickers.json"
    with open(config_path) as f:
        data = json.load(f)
    return data.get("tickers", [])

tickers = load_tickers()

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    'stock_etl_pipeline',
    default_args=default_args,
    description='Fetch and store Indian stock data in Postgres',
    schedule_interval='@daily',
    catchup=False,
)

# Task: Create Table
create_stocks_table = PostgresOperator(
    task_id='create_stocks_table',
    postgres_conn_id='stock_postgres_connection',
    sql=CREATE_STOCKS_SQL,
    dag=dag,
)

create_price_table = PostgresOperator(
    task_id='create_price_table',
    postgres_conn_id='stock_postgres_connection',
    sql=CREATE_PRICE_SQL,
    dag=dag,
)

create_forecast_table = PostgresOperator(
    task_id='create_forecast_table',
    postgres_conn_id='stock_postgres_connection',
    sql=CREATE_FORECAST_SQL,
    dag=dag,
)

create_enriched_table = PostgresOperator(
    task_id='create_enriched_table',
    postgres_conn_id='stock_postgres_connection',
    sql=CREATE_ENRICHED_DATA,
    dag=dag,
)

create_suggestion_table = PostgresOperator(
    task_id='create_suggestion_table',
    postgres_conn_id='stock_postgres_connection',
    sql=CREATE_SUGGESTION_DATA,
    dag=dag,
)

# Task: Fetch stock metadata
fetch_metadata_task = PythonOperator(
    task_id='fetch_metadata',
    python_callable=fetch_stock_metadata,
    op_args=[tickers],
    provide_context=True,
    dag=dag,
)

# Task: Insert metadata into DB
insert_metadata_task = PythonOperator(
    task_id='insert_metadata',
    python_callable=insert_metadata,
    provide_context=True,
    dag=dag,
)

# Task: Fetch price history
fetch_price_history_task = PythonOperator(
    task_id='fetch_price_history',
    python_callable=fetch_price_history,
    op_args=[tickers],
    provide_context=True,
    dag=dag,
)

# Task: Insert price data
insert_price_data_task = PythonOperator(
    task_id='insert_price_data',
    python_callable=insert_price_data,
    provide_context=True,
    dag=dag,
)

forecast_prices_task = PythonOperator(
    task_id='forecast_prices',
    python_callable=forecast_prices,
    provide_context=True,
    dag=dag,
)

insert_forecasts_task = PythonOperator(
    task_id='insert_forecasts',
    python_callable=insert_forecasts,
    provide_context=True,
    dag=dag,
)

enriched_price_data_task = PythonOperator(
    task_id = 'enrich_data',
    python_callable=enrich_price_data,
    provide_context=True,
    dag=dag,
)

insert_enriched_price_data_task = PythonOperator(
    task_id='insert_enriched_data',
    python_callable=insert_enriched_price_data,
    provide_context=True,
    dag=dag
)

suggestion_gemini_task = PythonOperator(
    task_id = 'suggestion_gemini',
    python_callable=get_stock_suggestion_gemini,
    provide_context=True,
    dag=dag,
)

insert_gemini_suggestion_task = PythonOperator(
    task_id='insert_gemini_suggestion',
    python_callable=insert_stock_suggestions,
    provide_context=True,
    dag=dag
)

# Define DAG dependencies
(
    create_stocks_table >> create_price_table >>  create_forecast_table >> create_enriched_table >> create_suggestion_table
    >> fetch_metadata_task >> insert_metadata_task >> fetch_price_history_task >> insert_price_data_task  >> forecast_prices_task >> insert_forecasts_task 
    >> enriched_price_data_task >> insert_enriched_price_data_task >> suggestion_gemini_task >> insert_gemini_suggestion_task
)
