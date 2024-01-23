import psycopg2
import csv

# Replace these variables with your database connection info
dbname = "Project01"
user = "michaelb"
password = "password"
host = "localhost"
port = "5432"

# SQL query to export data
query = 'COPY (SELECT * FROM "Project01") TO STDOUT WITH CSV HEADER'

# Path to save the CSV file
csv_file_path = "/Users/michaelb/Project 01/stock_data_postgres_db.csv"

# Connect to the PostgreSQL database
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
cur = conn.cursor()

# Execute the COPY command and save the output to a file
with open(csv_file_path, 'w') as csv_file:
    cur.copy_expert(query, csv_file)

# Close the cursor and the connection
cur.close()
conn.close()
