import pandas as pd
import json

with open('C:\Projects\Project01\earnings_data.json') as f:
    data = json.load(f)

df = pd.json_normalize([item for sublist in data for item in sublist])


df.to_parquet('C:\Projects\Project01\earnings_data.parquet')
