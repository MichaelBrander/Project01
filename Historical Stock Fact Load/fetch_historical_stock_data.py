from datetime import date, time as dt_time
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
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv('/Users/michaelb/Project 01/apikey.env')


def fetch_historical_data(api_url):
        try:
            response = requests.get(api_url)
            if response.status_code == 200:
                data = response.json()
                logging.info(f"Data fetched successfully: {data}")
                return data
            else:
                logging.error(f"HTTP Error: {response.status_code}")
                return None
        except requests.ConnectionError:
            logging.error("Connection Error")
        except requests.Timeout:
            logging.error("Request timed out")
        except Exception as e:
            logging.error(f"An error occurred: {e}")


def generate_date_ranges(start_year=2024, end_year=2000):
    start_date = date(start_year, 1, 16)
    for year in range(start_year, end_year - 1, -1):
        end_date = start_date.replace(year= year - 1)
        yield start_date, end_date


def main(stock_symbols=["AAPL"], extra_params=None):
        api_key=os.getenv('API_KEY')
        
        fetched_data1 = []
        for symbol in stock_symbols:
            for start_date, end_date in generate_date_ranges():    
                api_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?limit=100&from={start_date}&to={end_date}&apikey={api_key}"
                if extra_params:
                    api_url += f"&{extra_params}"

                stock_data = fetch_historical_data(api_url)
                if stock_data:
                    fetched_data1.append(stock_data)
                else:
                    logging.info(f"Failed to fetch data for {symbol}")
                time.sleep(1.0)
        
        with open('historical_stock_price_data.json', 'w') as file:
            json.dump(fetched_data1, file)
                
        fetched_data2 = []
        api_key=os.getenv('API_KEY')
        for symbol in stock_symbols:
            for start_date, end_date in generate_date_ranges():
                api_url = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{symbol}?limit=100&from={start_date}&to={end_date}&apikey={api_key}"
                if extra_params:
                    api_url += f"&{extra_params}"

                marketcap_data = fetch_historical_data(api_url)
                if marketcap_data:
                    fetched_data2.append(marketcap_data)
                else:
                    logging.info(f"Failed to fetch data for {symbol}")
                time.sleep(1.0)        
        
        with open('historical_market_cap_data.json', 'w') as file:
            json.dump(fetched_data2, file)

if __name__ == "__main__":
        main(stock_symbols=["LULU", "CROX", "SHAK", "COST", "WBD", "NVDA", "LMT", "PM", "DJIA"])
