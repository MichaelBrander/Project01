import json
import os
from dotenv import load_dotenv
import requests
from datetime import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv('/Users/michaelb/Project 01/Env_Variables/apikey.env')

def fetch_earnings(api_url):
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
         
def main(stock_symbols=["AAPL"], extra_params=None):
    api_key = os.getenv('API_KEY')
    fetched_data = []
    for symbol in stock_symbols:
        api_url = f"https://financialmodelingprep.com/api/v3/key-metrics/{symbol}?period=annual&apikey={api_key}"
        if extra_params:
            api_url += f"&{extra_params}"
        stock_data = fetch_earnings(api_url)
        if stock_data:
            fetched_data.append(stock_data)
        else:
            logging.info(f"Failed to fetch data for {symbol}")

    with open('earnings_data.json', 'w') as file:
        json.dump(fetched_data, file)
    
if __name__ == "__main__":
    main(stock_symbols=["LULU", "CROX", "SHAK", "COST", "WBD", "NVDA", "LMT", "PM"])
    