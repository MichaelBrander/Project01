from datetime import datetime, timedelta
import datetime as dt 
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import aiohttp
from dotenv import load_dotenv
import asyncio
import pytz
import os
import logging
import json
import pandas as pd
import pyarrow.parquet as pq
import plotly.graph_objs as go
import pdfkit
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from sqlalchemy import create_engine
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv('/Users/michaelb/Project 01/apikey.env')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 16, 1, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def preprocessing():
    
    fields_to_extract = ['symbol', 'name', 'price', 'changesPercentage', 'change', 
                         'dayLow', 'dayHigh', 'yearHigh', 'yearLow', 'marketCap',
                         'priceAvg50', 'priceAvg200', 'volume', 'eps', 'timestamp']

    json_file_path = '/Users/michaelb/Project 01/stock_data.json'

    with open(json_file_path, 'r') as file:
        try:
            data = json.load(file)
        except FileNotFoundError:
            logging.error(f"File not found: {json_file_path}")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON: {e}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    if not all(isinstance(sublist, list) and all(isinstance(item, dict) for item in sublist) for sublist in data):
        logging.error("Invalid JSON structure")

    flattened_data = [item for sublist in data for item in sublist]

    try:
        df = pd.DataFrame(flattened_data)
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
        selected_df['marketCap'] = selected_df['marketCap'].astype(int)  
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

                parquet_file_path = '/Users/michaelb/Project 01/stock_data.parquet'

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

        df = pd.read_parquet('/Users/michaelb/Project 01/stock_data.parquet')
        logging.info("Parquet file loaded successfully.")

        engine = create_engine('postgresql://michaelb:{password}localhost:5432/Project01')
        logging.info("Database engine created.")

        df.to_sql('Project01', engine, if_exists='append', index=False)
        logging.info("Data inserted into database.")

        end_time = time.time()

        duration = end_time - start_time
        logging.info(f"Operation took {duration} seconds")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

with DAG('my_data_pipeline',
         default_args=default_args,
         description='A simple data pipeline',
         schedule='2 21 * * *',  
         catchup=False) as dag:



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

task_preprocessing >> task_data_analysis >> task_import_to_database
