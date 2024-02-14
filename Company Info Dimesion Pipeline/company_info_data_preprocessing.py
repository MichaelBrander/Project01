import pandas as pd
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

json_file_path = '/Users/michaelb/Project 01/companyinfo_data.json'

try:
    with open(json_file_path, 'r') as file:
        data = json.load(file)
        if not isinstance(data, list) or not all(isinstance(item, list) and isinstance(item[0], dict) for item in data):
            logging.error("Invalid JSON structure: Expected a list of lists of dictionaries")
            raise ValueError("Invalid JSON structure")
        
    # Flatten the lists and dictionaries
    flattened_data = [item[0] for item in data]
    df = pd.DataFrame(flattened_data)
    
    df['fullTimeEmployees'] = pd.to_numeric(df['fullTimeEmployees'], errors='coerce').astype('Int64')
    
    print(df.dtypes)
    
    
    df.to_parquet('/Users/michaelb/Project 01/Company Info Dimesion Pipeline/companyinfo_data.parquet')
    logging.info("Data validation and conversion to Parquet completed successfully")
    
except FileNotFoundError:
    logging.error(f"File not found: {json_file_path}")
except json.JSONDecodeError as e:
    logging.error(f"Error decoding JSON: {e}")
except ValueError as e:
    logging.error(f"Value error: {e}")
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")