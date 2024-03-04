from datetime import datetime, timedelta, time as dt_time
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import pytz
import os
import logging
import json
import pandas as pd
import plotly.graph_objs as go
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from sqlalchemy import create_engine
import time 
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

password=os.getenv('password')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 16, 1, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_stock_data():
    stock_data = []
    start_time = time.time()
    api_key = Variable.get("API_KEY")
    http = HttpHook(method="GET", http_conn_id="http_default")

    stock_symbols = ["LULU", "CROX", "SHAK", "COST", "WBD", "NVDA", "LMT", "PM", "DJIA"]

    end_time = start_time + 6.5 * 60 * 60

    while time.time() < end_time:
        for symbol in stock_symbols:
            endpoint = f"quote/{symbol}?apikey={api_key}"
            response = http.run(endpoint)
            try:
                response.raise_for_status()
                data = response.json()
                if "Error Message" in data:
                    raise Exception(f"Endpoint returned an error message in a 200 response for {symbol}")
            except Exception as err:
                logging.error(f"{type(err)} - {err} for {symbol}")
                logging.error(f"Request failed for {symbol} - {response.status_code} - {response.text}")
                continue  # Skip to the next symbol in case of an error
            stock_data.append(data[0])
            logging.info(f"Data fetched for {symbol} - {len(stock_data)} records fetched.")

    file_path = "/opt/airflow/outputs/stock_data.json"
    with open(file_path, "w") as file:
        json.dump(stock_data, file)


def preprocessing():
    
    json_file_path = '/opt/airflow/outputs/stock_data.json'

    fields_to_extract = ['symbol', 'name', 'price', 'changesPercentage', 'change', 
                        'dayLow', 'dayHigh', 'yearHigh', 'yearLow', 'marketCap',
                        'priceAvg50', 'priceAvg200', 'volume', 'eps', 'timestamp']
                

    with open(json_file_path, 'r') as file:
        try:
            data = json.load(file)
        except FileNotFoundError:
            logging.error(f"FIle not found: {json_file_path}")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON: {e}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    if not all(isinstance(sublist, list) and all(isinstance(item, dict) for item in sublist) for sublist in data):
        logging.error("Invalid JSON structure")


    try:
        df = pd.DataFrame(data)
    except Exception as e:
        logging.error(f"Error occurred while creating DataFrame: {e}")


    selected_df = df[fields_to_extract]

    try:
        selected_df['symbol'] = selected_df['symbol'].astype(str)
        selected_df['name'] = selected_df['name'].astype(str)
        selected_df['price'] = selected_df['price'].astype(float)
        selected_df['changesPercentage'] = selected_df['changesPercentage'].astype(float)
        selected_df['change'] = selected_df['change'].astype(float)
        selected_df['dayLow'] = selected_df['dayLow'].astype(float)
        selected_df['dayHigh'] = selected_df['dayHigh'].astype(float)
        selected_df['yearHigh'] = selected_df['yearHigh'].astype(float)
        selected_df['yearLow'] = selected_df['yearLow'].astype(float)
        selected_df['priceAvg50'] = selected_df['priceAvg50'].astype(float)
        selected_df['priceAvg200'] = selected_df['priceAvg200'].astype(float)
        selected_df['volume'] = selected_df['volume'].astype(int)
        selected_df['eps'] = selected_df['eps'].astype(float)
        selected_df['timestamp'] = pd.to_datetime(selected_df['timestamp'], unit='s', utc=True)
    except KeyError as e:
        logging.error(f"Key error during DataFrame conversion: {e}")
    except ValueError as e:
        logging.error(f"Value error during DataFrame convesion: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during DataFrame conversion: {e}")


    if selected_df['price'].min() < 0:
        logging.error("Invalid data: Negative values found in 'price'")
    if selected_df['changesPercentage'].min() < 0:
        logging.error("Invalid data: Negative values found in 'changesPercentage'")
    if selected_df['change'].min() < 0:
        logging.error("Invalid data: Negative values found in 'change'")
    if selected_df['dayLow'].min() < 0:
        logging.error("Invalid data: Negative values found in 'dayLow'")
    if selected_df['dayHigh'].min() < 0:
        logging.error("Invalid data: Negative values found in 'dayHigh'")
    if selected_df['yearLow'].min() < 0:
        logging.error("Invalid data: Negative values found in 'yearLow'")
    if selected_df['marketCap'].min() < 0:
        logging.error("Invalid data: Negative values found in 'marketCap'")
    if selected_df['priceAvg50'].min() < 0:
        logging.error("Invalid data: Negative values found in 'priceAvg50'")
    if selected_df['priceAvg200'].min() < 0:
        logging.error("Invalid data: Negative values found in 'priceAvg200'")
    if selected_df['volume'].min() < 0:
        logging.error("Invalid data: Negative values found in 'volume'")
    if selected_df['eps'].min() < 0:
        logging.error("Invalid data: Negative values found in 'eps'")


    if not (0 <= selected_df['changesPercentage'].max() <=100):
        logging.error("Invalid data: 'changesPercentage' out of expected range" )


    if selected_df.isnull().any().any():
        logging.error("Missing values found in the DataFrame")

    try:
        selected_df.to_parquet('/Users/michaelb/Project 01/stock_data.parquet')
    except IOError as e:
        logging.error(f"IOError occurred: {e}") 
    except ValueError as e:
        logging.error(f"ValueError occurred: {e}")
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")

    logging.info("Data validation passed successfully")

def data_analysis():
                pdf_path = "stock_chart.pdf"
                c = canvas.Canvas(pdf_path, pagesize=letter)
                width, height = letter

                parquet_file_path = '/opt/airflow/outputs/stock_data.parquet'

                try:
                            df = pd.read_parquet(parquet_file_path)
                except Exception as e:
                            logging.error(f"Error occurred while loading Parquet file: {e}")


                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                hourly_volume = df['volume'].resample('H').sum()

                fig = go.Figure()

                fig.add_trace(go.Scatter(x=df.index, y=df['price'], name='Price'))

                fig.add_trace(go.Bar(x=hourly_volume.index, y=hourly_volume, name='Volume', yaxis='y2'))

                fig.update_layout(
                        yaxis=dict(title='Price'),
                        yaxis2=dict(title='Volume', side='right', overlaying='y', showgrid=False),
                        barmode='overlay',
                    )

                fig.write_image("stock_chart.png")

                image_path = "stock_chart.png"
                image = ImageReader(image_path)
                c.drawImage(image, 50, height - 400, width=500, height=300) 

                c.save()

def import_to_database():

    
    try:
        start_time = time.time()

        df = pd.read_parquet('/opt/airflow/outputs/stock_data.parquet')
        logging.info("Parquet file loaded successfully.")

        engine = create_engine('postgresql://postgres:{password}@host.docker.internal:5432/postgres')
        logging.info("Database engine created.")

        df.to_sql('Project01', engine, if_exists='append', index=False)
        logging.info("Data inserted into database.")

        end_time = time.time()

        duration = end_time - start_time
        logging.info(f"Operation took {duration} seconds")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

with DAG('Stock_Price_Daily_Pipeline',
         default_args=default_args,
         description='A data pipeline',
         schedule='30 14 * * 1-5',  
         catchup=False) as dag:


    task_fetch_stock_data = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=fetch_stock_data,
        dag=dag,
    )
    task_preprocessing = PythonOperator(
        task_id='preprocessing',
        python_callable=preprocessing,
        dag=dag,
    )
    task_data_analysis = PythonOperator(
        task_id='data_analysis',
        python_callable=data_analysis,
        dag=dag,
    )
    task_import_to_database = PythonOperator(
        task_id='import_to_database',
        python_callable=import_to_database,
        dag=dag,
    )

task_fetch_stock_data >> task_preprocessing >> task_data_analysis >> task_import_to_database
