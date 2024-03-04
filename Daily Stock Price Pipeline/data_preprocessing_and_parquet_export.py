import pandas as pd
import logging
import os
import json
import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

json_file_path = 'C:\Projects\Project01\stock_data.json'

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
    selected_df.to_parquet('C:\Projects\Project01\stock_data.parquet')
except IOError as e:
    logging.error(f"IOError occurred: {e}") 
except ValueError as e:
    logging.error(f"ValueError occurred: {e}")
except Exception as e:
    logging.error(f"Unexpected error occurred: {e}")

logging.info("Data validation passed successfully")
