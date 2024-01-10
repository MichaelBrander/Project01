import aiohttp
from dotenv import load_dotenv
import asyncio
import time
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

async def main(stock_symbol="AAPL", extra_params=None):
    api_key = os.getenv('API_KEY') 
    api_url = f"https://financialmodelingprep.com/api/v3/quote/{stock_symbol}?apikey={api_key}"
    if extra_params:
        api_url += f"&{extra_params}"

    requests_per_second = 2
    duration_seconds = 60
    total_requests = requests_per_second * duration_seconds

    fetched_data = []

    async with aiohttp.ClientSession() as session:
        for _ in range(total_requests):
            stock_data = await fetch_stocks(session, api_url)
            if stock_data:
                fetched_data.append(stock_data)
            else:
                logging.info("Failed to fetch data")
    
    await asyncio.sleep(0.25)

    with open('stock_data.json', 'w') as file:
        json.dump(fetched_data, file)

asyncio.run(main())