import requests
import json
from dotenv import load_dotenv
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_companyinfo(symbol, http_conn_id='http_default', extra_params=None):
    api_key = Variable.get('API_KEY') 
    http = HttpHook(method='GET', http_conn_id=http_conn_id)
    endpoint = f"/profile/{symbol}?apikey={api_key}"
    
    if extra_params:
        endpoint += f"&{extra_params}"
    
    try:
        response = http.run(endpoint)
        if response.status_code == 200:
            data = response.json()
            logging.info(f"Data fetched successfully for {symbol}: {data}")
            return data
        else:
            logging.error(f"HTTP Error for {symbol}: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"An error occurred while fetching data for {symbol}: {e}")
        return None

def main(stock_symbols=["AAPL"], extra_params=None):
    fetched_data = []
    for symbol in stock_symbols:
        stock_data = fetch_companyinfo(symbol=symbol, extra_params=extra_params)
        if stock_data:
            fetched_data.append(stock_data)
        else:
            logging.info(f"Failed to fetch data for {symbol}")

    file_path = "/opt/airflow/outputs/companyinfo_data.json"
    with open(file_path, "w") as file:
        json.dump(fetched_data, file)
    logging.info("Company info data fetch completed.")

def process_data():

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    json_file_path = '/opt/airflow/outputs/companyinfo_data.json'

    try:
        with open(json_file_path, 'r') as file:
            data = json.load(file)
            if not isinstance(data, list) or not all(isinstance(item, list) and isinstance(item[0], dict) for item in data):
                logging.error("Invalid JSON structure: Expected a list of lists of dictionaries")
                raise ValueError("Invalid JSON structure")

        flattened_data = [item[0] for item in data]
        df = pd.DataFrame(flattened_data)
        df['fullTimeEmployees'] = pd.to_numeric(df['fullTimeEmployees'], errors='coerce').astype('Int64')
        print(df.dtypes)


        df.to_parquet('/opt/airflow/outputs/companyinfo_data.parquet')
        logging.info("Data validation and conversion to Parquet completed successfully")

    except FileNotFoundError:
        logging.error(f"File not found: {json_file_path}")
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e}")
    except ValueError as e:
        logging.error(f"Value error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

def load_data():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    password = Variable.get('password')

    try:
        start_time = time.time()
        logging.info("Starting script..")
        
        logging.info("Loading Parquet file...")
        df = pd.read_parquet('/opt/airflow/outputs/companyinfo_data.parquet')
        logging.info("Parquet file loaded successfully/")
        
        logging.info("Creating database engine...")
        engine = create_engine(f'postgresql://postgres:{password}@host.docker.internal:5432/postgres')
        logging.info("Database engine created.")
        
        logging.info("Inserting data into database..")
        df.to_sql('CompanyInfo_Dim', engine, if_exists='append', index=False)
        logging.info("Data inserted into databse")

        end_time = time.time()

        duration = end_time - start_time 
        logging.info(f"Operation took {duration} seconds")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Script finsihed.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('company_info_dimension_dag', default_args=default_args, schedule_interval='@yearly') as dag:
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=main,
        op_kwargs={'stock_symbols': ["LULU", "CROX", "SHAK", "COST", "WBD", "NVDA", "LMT", "PM", "DJIA"]}
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    fetch_data_task >> process_data_task >> load_data_task
