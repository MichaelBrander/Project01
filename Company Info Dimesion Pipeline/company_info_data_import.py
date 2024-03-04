from sqlalchemy import create_engine
import os
import logging
import pandas as pd
import time
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

password=os.getenv('password')

try:
    start_time = time.time()
    logging.info("Starting script..")
    
    logging.info("Loading Parquet file...")
    df = pd.read_parquet('C:\Projects\Project01\Container Airflow\outputs\companyinfo_data.parquet')
    logging.info("Parquet file loaded successfully/")
    
    logging.info("Creating database engine...")
    engine = create_engine(f'postgresql://postgres:{password}@localhost:5432/postgres')
    logging.info("Database engine created.")
    
    logging.info("Inserting data into database..")
    df.to_sql('CompanyInfo_Dim', engine, if_exists='append', index=False)
    logging.info("Data inserted into databse")
    
    end_time = time.time()
    
    duration = end_time - start_time 
    logging.info(f"Operation took {duration} seconds")
    
except Exception as e:
    logging.error(f"An error occurred: {e}")
finally:
    logging.info("Script finsihed.")
    
    
    
    

