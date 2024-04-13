import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2  # or import mysql.connector for MySQL

# Load environment variables
load_dotenv()

# Database credentials
host = os.getenv('DB_HOST')
user = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
dbname = os.getenv('DB_NAME')

# Establishing the database connection
conn = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    dbname=dbname
)

# SQL query to fetch the data
sql_query = 'SELECT * FROM tech_layoffs'

# Load data into a DataFrame
df = pd.read_sql_query(sql_query, conn)

# Close the database connection
conn.close()

# Export DataFrame to CSV
df.to_csv('data/tech_layoffs_db.csv', index=False)
