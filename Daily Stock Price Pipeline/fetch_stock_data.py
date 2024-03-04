import requests
from dotenv import load_dotenv
import pytz
from datetime import datetime, time as dt_time
import time
import os
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv('C:\\Projects\\Project01\\Variables\\apikey.env')

def fetch_stocks(api_url):
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

def is_within_trading_hours():
    ny_tz = pytz.timezone('America/New_York')
    ny_time = datetime.now(ny_tz).time()
    start_time = dt_time(7, 35)
    end_time = dt_time(7, 36)
    return start_time <= ny_time <= end_time

def main(stock_symbols=["AAPL"], extra_params=None):
    api_key = os.getenv('API_KEY')
    fetched_data = []

    while not is_within_trading_hours():
        time.sleep(0.0)

    while is_within_trading_hours():
        for symbol in stock_symbols:
            api_url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={api_key}"
            if extra_params:
                api_url += f"&{extra_params}"

            stock_data = fetch_stocks(api_url)
            if stock_data:
                fetched_data.append(stock_data)
            else:
                logging.info(f"Failed to fetch data for {symbol}")
            time.sleep(2.5)
    
    with open('stock_data.json', 'w') as file:
        json.dump(fetched_data, file)

if __name__ == "__main__":
    main(stock_symbols=["LULU", "CROX", "SHAK", "COST", "WBD", "NVDA", "LMT", "PM", "DJIA"])
