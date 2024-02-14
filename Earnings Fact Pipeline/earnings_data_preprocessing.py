import pandas as pd
import json

with open('/Users/michaelb/Project 01/earnings_data.json') as f:
    data = json.load(f)

df = pd.json_normalize([item for sublist in data for item in sublist])


df.to_parquet('/Users/michaelb/Project 01/Earnings Fact Pipeline/earnings_data.parquet')
