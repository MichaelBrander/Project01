import pandas as pd
import json
import time

start_time = time.time()

json_file_path = 'C:\Projects\Project01\historical_stock_price_data.json'

fields_to_extract = ['symbol', 'date', 'high', 'low', 'close', 'volume', 
                     'change', 'changePercent']

with open(json_file_path, 'r') as file:
    data = json.load(file)
    normalised_df = pd.json_normalize(data, record_path='historical', meta=['symbol'])
    
# Only keep the columns you need
normalised_df = normalised_df[fields_to_extract]

# Convert 'date' column to datetime
normalised_df['date'] = pd.to_datetime(normalised_df['date'])


column_mapping = {
    'changePercent': 'changesPercentage',
    'close': 'price',
    'date': 'timestamp',
    'high': 'dayHigh',
    'low': 'dayLow',
}

normalised_df = normalised_df.rename(columns=column_mapping)

normalised_df['composite_key'] = normalised_df['symbol'] + '_' + normalised_df['timestamp'].astype(str)


#Specify the path to the second JSON file
json_file_path2 = 'C:\Projects\Project01\historical_market_cap_data.json'

# Define the fields to extract from the second JSON file
fields_to_extract2 = ['symbol', 'date', 'marketCap']

# Load the second JSON file
# Load the JSON data
with open(json_file_path2, 'r') as file:
    data = json.load(file)

# Extract the inner list
data = [item for sublist in data for item in sublist]

# Normalize the JSON data into a DataFrame
normalised_df2 = pd.json_normalize(data)

# Now, df should be a DataFrame with 'symbol', 'date', and 'marketCap' columns

# Only keep the columns you need
normalised_df2 = normalised_df2[fields_to_extract2]

# Convert 'date' column to datetime
column_mapping = {
    'date': 'timestamp',
}

normalised_df2['date'] = pd.to_datetime(normalised_df2['date'])

# Create a composite key
normalised_df2['composite_key'] = normalised_df2['symbol'] + '_' + normalised_df2['date'].astype(str)
normalised_df2 = normalised_df2.rename(columns=column_mapping)
normalised_df2 = normalised_df2.drop(columns=['symbol', 'timestamp'])
merged_df = normalised_df.merge(normalised_df2, on='composite_key', how='inner')


print(merged_df.columns)

missing_columns = ['eps', 'name', 'priceAvg200', 'priceAvg50', 'yearHigh', 'yearLow']
for column in missing_columns:
    merged_df[column] = None
merged_df['timestamp'] = merged_df['timestamp'].dt.tz_localize('UTC')

merged_df = merged_df.rename(columns=column_mapping)
merged_df = merged_df.drop(columns=['composite_key'])
columns_to_convert = ['yearHigh', 'yearLow', 'priceAvg50', 'priceAvg200', 'eps']
for column in columns_to_convert:
    merged_df[column] = merged_df[column].astype('float64')

merged_df.to_parquet('C:\Projects\Project01\historical_stock_fact.parquet', index=False)


print(merged_df.head(5))
print(merged_df.dtypes)

end_time = time.time()

print('Time elapsed:', end_time - start_time, 'seconds')