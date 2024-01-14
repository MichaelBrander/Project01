import pandas as pd
import pyarrow as py
import pyarrow.parquet as pq
import logging
import os
import json
import datetime

json_file_path = '/Users/michaelb/Project 01/stock_data.json'

fields_to_extract = ['symbol', 'name', 'price', 'changesPercentage', 'change', 
                     'dayLow', 'dayHigh', 'yearHigh', 'yearLow', 'marketCap',
                     'priceAvg50', 'priceAvg200', 'volume', 'eps', 'timestamp']


pd.set_option('display.max_rows', None)  
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None) 

with open(json_file_path, 'r') as file:
    data = json.load(file)

flattened_data = [item for sublist in data for item in sublist]

df = pd.DataFrame(flattened_data)

selected_df = df[fields_to_extract]



