import aiohttp
from dotenv import load_dotenv
import asyncio
import time
import os

load_dotenv('/Users/michaelb/Project 01/apikey.env')

async def fetch_stocks(session, api_url):
    async with session.get(api_url) as response:
        if response.status == 200:
            return await response.json()
        else:
            return None
    
    
async def main():
    api_key = os.getenv('API_KEY') 
    api_url = f"https://financialmodelingprep.com/api/v3/quote/SHAK?apikey={api_key}"
    requests_per_second = 2
    duration_seconds = 60
    total_requests = requests_per_second * duration_seconds
    

    async with aiohttp.ClientSession() as session:
        for _ in range(total_requests):
            stock_data = await fetch_stocks(session, api_url)
            if stock_data:
                print(stock_data)
            else:
                print("Failed to fetch data")

    await asyncio.sleep(0.25)

asyncio.run(main())              