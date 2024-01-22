import aiohttp
from dotenv import load_dotenv
import asyncio
import pytz
from datetime import datetime, time
import os
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv('/Users/michaelb/Project 01/apikey.env')

async def fetch_stocks(session, api_url):
    try:
        async with session.get(api_url) as response:
            if response.status == 200:
                data = await response.json()
                logging.info(f"Data fetched successfully: {data}")
                return data
            else:
                logging.error(f"HTTP Error: {response.status}")
                return None
    except aiohttp.ClientConnectionError:
        logging.error("Connection Error")
    except asyncio.TimeoutError:
        logging.error("Request timed out")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def is_within_trading_hours():
    ny_tz = pytz.timezone('America/New_York')
    ny_time = datetime.now(ny_tz).time()
    start_time = time(9, 30)
    end_time = time(16, 0)
    return start_time <= ny_time <= end_time

async def main(stock_symbols=["AAPL"], extra_params=None):
    api_key = os.getenv('API_KEY')
    fetched_data = []

    async with aiohttp.ClientSession() as session:
        while not is_within_trading_hours():
            await asyncio.sleep(1)

        while is_within_trading_hours():
            for symbol in stock_symbols:
                api_url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={api_key}"
                if extra_params:
                    api_url += f"&{extra_params}"

                stock_data = await fetch_stocks(session, api_url)
                if stock_data:
                    fetched_data.append(stock_data)
                else:
                    logging.info(f"Failed to fetch data for {symbol}")
                await asyncio.sleep(0.1)

    with open('stock_data.json', 'w') as file:
        json.dump(fetched_data, file)

asyncio.run(main(stock_symbols=["LULU", "CROX", "SHAK", "COST", "WBD", "NVDA", "LMT", "PM", "DJIA"]))