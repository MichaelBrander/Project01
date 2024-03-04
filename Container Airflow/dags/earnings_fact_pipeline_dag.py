import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import logging
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_earnings(symbol, http_conn_id='http_default', extra_params=None):
    http = HttpHook(method='GET', http_conn_id=http_conn_id)
    api_key = Variable.get('API_KEY')  
    endpoint = f"/api/v3/key-metrics/{symbol}?period=annual&apikey={api_key}"
    if extra_params:
        endpoint += f"&{extra_params}"
    
    try:
        response = http.run(endpoint)
        data = response.json()
        if data:
            logging.info(f"Data fetched successfully for {symbol}: {data}")
            return data
        else:
            logging.error(f"No data returned for {symbol}")
            return None
    except Exception as e:
        logging.error(f"An error occurred while fetching data for {symbol}: {e}")
        return None

def fetch_and_save_data(**kwargs):
    stock_symbols = kwargs.get('stock_symbols', ["AAPL"])
    extra_params = kwargs.get('extra_params', None)
    fetched_data = []
    
    for symbol in stock_symbols:
        stock_data = fetch_earnings(symbol=symbol, extra_params=extra_params)
        if stock_data:
            fetched_data.append(stock_data)
        else:
            logging.info(f"Failed to fetch data for {symbol}")

    file_path = 'earnings_data.json'
    with open(file_path, 'w') as file:
        json.dump(fetched_data, file)
    logging.info(f"Data for symbols {stock_symbols} saved to {file_path}")
def process_and_save_data(**kwargs):
    import pandas as pd
    import json

    with open('C:\Projects\Project01\earnings_data.json') as f:
        data = json.load(f)

    df = pd.json_normalize([item for sublist in data for item in sublist])

    df.to_parquet('C:\Projects\Project01\earnings_data.parquet')

def load_data_to_database(**kwargs):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    password = Variable.get('password')

    try:
        start_time = time.time()
        logging.info("Starting script..")

        logging.info("Loading Parquet file...")
        df = pd.read_parquet('C:\Projects\Project01\earnings_data.parquet')
        logging.info("Parquet file loaded successfully/")

        logging.info("Creating database engine...")
        engine = create_engine(f'postgresql://postgres:{password}@host.docker.internal:5432/postgres')
        connection = engine.raw_connection()
        logging.info("Database engine created.")

        logging.info("Inserting data into database..")
        df.to_sql('Earnings_Fact', engine, if_exists='append', index=False)
        logging.info("Data inserted into databse")

        end_time = time.time()

        duration = end_time = start_time 
        logging.info(f"Operation took {duration} seconds")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Script finsihed.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG('earnings_fact_pipeline_dag', default_args=default_args, schedule_interval='@yearly') as dag:
    fetch_and_save_task = PythonOperator(
        task_id='fetch_and_save_data',
        python_callable=fetch_and_save_data,
        op_kwargs={'stock_symbols': ["LULU", "CROX", "SHAK", "COST", "WBD", "NVDA", "LMT", "PM"]}
    )

    process_and_save_task = PythonOperator(
        task_id='process_and_save_data',
        python_callable=process_and_save_data
    )

    load_data_to_db_task = PythonOperator(
        task_id='load_data_to_database',
        python_callable=load_data_to_database
    )

    fetch_and_save_task >> process_and_save_task >> load_data_to_db_task
