from prophet import Prophet
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def forecast_prices(**kwargs):
    ti = kwargs['ti']
    price_data = ti.xcom_pull(task_ids='fetch_price_history')

    forecasts = []
    for symbol in set(d['symbol'] for d in price_data):
        df = pd.DataFrame([d for d in price_data if d['symbol'] == symbol])
        df['ds'] = pd.to_datetime(df['date'])
        df['y'] = df['close']
        model = Prophet()
        model.fit(df[['ds', 'y']])
        future = model.make_future_dataframe(periods=45)
        forecast = model.predict(future)
        for _, row in forecast.tail(45).iterrows():
            forecasts.append({
                "symbol": symbol,
                "date": row['ds'].date().isoformat(),
                "predicted_close": row['yhat']
            })

    ti.xcom_push(key='forecasts', value=forecasts)
