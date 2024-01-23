import psycopg2
import csv
import os
from dotenv import load_dotenv

load_dotenv('/Users/michaelb/Project 01/apikey.env')

password = os.getenv('password')

dbname = "Project01"
user = "michaelb"
password = {password}
host = "localhost"
port = "5432"

query = 'COPY (SELECT * FROM "Project01") TO STDOUT WITH CSV HEADER'

csv_file_path = "/Users/michaelb/Project 01/stock_data_postgres_db.csv"

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
cur = conn.cursor()

with open(csv_file_path, 'w') as csv_file:
    cur.copy_expert(query, csv_file)

cur.close()
conn.close()
