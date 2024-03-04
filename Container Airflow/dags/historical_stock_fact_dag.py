from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import os
import logging
import json
import pandas as pd
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
import time
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 16, 1, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'historical_stock_fact_dag',
    default_args=default_args,
    description='DAG for fetching and processing historical stock data',
    # No schedule interval
)


def generate_date_ranges(start_year=2024, end_year=2000):
    start_date = date(start_year, 1, 1)
    for year in range(start_year, end_year - 1, -1):
        end_date = start_date.replace(year=year - 1)
        yield start_date, end_date

def fetch_historical_data(stock_symbols=["AAPL"], extra_params=None, http_conn_id='http_default'):
    http_hook = HttpHook(method='GET', http_conn_id=http_conn_id)
    api_key = Variable.get("API_KEY")
    fetched_data_prices = []
    fetched_data_market_cap = []

    for symbol in stock_symbols:
        for start_date, end_date in generate_date_ranges():
            price_endpoint = f"historical-price-full/{symbol}"
            market_cap_endpoint = f"historical-market-capitalization/{symbol}"

            params = {'limit': '1000', 'from': start_date.strftime('%Y-%m-%d'), 'to': end_date.strftime('%Y-%m-%d'), 'apikey': api_key}
            if extra_params:
                params.update(extra_params)
                
            response = http_hook.run(price_endpoint, data=params)
            if response.status_code == 200:
                fetched_data_prices.append(response.json())
                logging.info(f"Price data fetched for {symbol}")
            else:
                logging.error(f"Failed to fetch price data for {symbol}: HTTP {response.status_code}")

            response = http_hook.run(market_cap_endpoint, data=params)
            if response.status_code == 200:
                fetched_data_market_cap.append(response.json())
                logging.info(f"Market cap data fetched for {symbol}")
            else:
                logging.error(f"Failed to fetch market cap data for {symbol}: HTTP {response.status_code}")

    with open('historical_stock_price_data.json', 'w') as file:
        json.dump(fetched_data_prices, file)
    logging.info("Historical stock price data saved.")

    with open('historical_market_cap_data.json', 'w') as file:
        json.dump(fetched_data_market_cap, file)
    logging.info("Historical market cap data saved.")


def preprocess_data():
    start_time = time.time()
    json_file_path = '/Users/michaelb/Project 01/Historical Stock Fact Load/historical_stock_price_data.json'

    fields_to_extract = ['symbol', 'date', 'high', 'low', 'close', 'volume', 'change', 'changePercent']

    with open(json_file_path, 'r') as file:
        data = json.load(file)
        normalised_df = pd.json_normalize(data, record_path='historical', meta=['symbol'])

    normalised_df = normalised_df[fields_to_extract]

    normalised_df['date'] = pd.to_datetime(normalised_df['date'])

    column_mapping = {
        'changePercent': 'changesPercentage',
        'close': 'price',
        'date': 'timestamp',
        'high': 'dayHigh',
        'low': 'dayLow',
    }

    normalised_df = normalised_df.rename(columns=column_mapping)
    normalised_df['composite_key'] = normalised_df['symbol'] + '_' + normalised_df['timestamp'].astype(str)


    json_file_path2 = '/Users/michaelb/Project 01/Historical Stock Fact Load/historical_market_cap_data.json'
    fields_to_extract2 = ['symbol', 'date', 'marketCap']


    with open(json_file_path2, 'r') as file:
        data = json.load(file)
    data = [item for sublist in data for item in sublist]
    normalised_df2 = pd.json_normalize(data)
    normalised_df2 = normalised_df2[fields_to_extract2]
    column_mapping = {
        'date': 'timestamp',
    }

    normalised_df2['date'] = pd.to_datetime(normalised_df2['date'])

    normalised_df2['composite_key'] = normalised_df2['symbol'] + '_' + normalised_df2['date'].astype(str)
    normalised_df2 = normalised_df2.rename(columns=column_mapping)
    normalised_df2 = normalised_df2.drop(columns=['symbol', 'timestamp'])
    merged_df = normalised_df.merge(normalised_df2, on='composite_key', how='inner')

    print(merged_df.columns)

    missing_columns = ['eps', 'name', 'priceAvg200', 'priceAvg50', 'yearHigh', 'yearLow']
    for column in missing_columns:
        merged_df[column] = None
    merged_df['timestamp'] = merged_df['timestamp'].dt.tz_localize('UTC')

    merged_df = merged_df.rename(columns=column_mapping)
    merged_df = merged_df.drop(columns=['composite_key'])
    columns_to_convert = ['yearHigh', 'yearLow', 'priceAvg50', 'priceAvg200', 'eps']
    for column in columns_to_convert:
        merged_df[column] = merged_df[column].astype('float64')

    merged_df.to_parquet('/opt/airflow/outputs/historical_stock_fact.parquet', index=False)

    end_time = time.time()

    print('Time elapsed:', end_time - start_time, 'seconds')

def import_data():
    password = Variable.get('password')

    try:
        start_time = time.time()
        logging.info("Starting script...")

        logging.info("Loading Parquet file...")
        df = pd.read_parquet('/opt/airflow/outputs/historical_stock_fact.parquet')
        logging.info("Parquet file loaded successfully.")

        logging.info("Creating database engine...")
        engine = create_engine(f'postgresql://michaelb:{password}@host.docker.internal:5432/Project01')
        logging.info("Database engine created.")

        logging.info("Inserting data into database...")
        df.to_sql('Project01', engine, if_exists='append', index=False)
        logging.info("Data inserted into database.")

        end_time = time.time()

        duration = end_time - start_time
        logging.info(f"Operation took {duration} seconds")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Script finished.")

fetch_historical_data = PythonOperator(
    task_id='fetch_stock_data',
    python_callable=fetch_historical_data,
    op_kwargs={'stock_symbols': ["LULU", "CROX", "SHAK", "COST", "WBD", "NVDA", "LMT", "PM", "DJIA"], 'extra_params': None},
    dag=dag,
)

preprocess_data = PythonOperator(
    task_id='process_data',
    python_callable=preprocess_data,
    dag=dag,
)

import_data = PythonOperator(
    task_id='load_data_to_database',
    python_callable=import_data,
    dag=dag,
)

fetch_historical_data >> preprocess_data >> import_data




