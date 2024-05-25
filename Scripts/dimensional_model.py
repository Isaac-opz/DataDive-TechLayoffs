import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

data = pd.read_sql_table('tech_layoffs', engine)

print("Columns in the DataFrame before renaming: ", data.columns)

# If 'date_layoffs' exists, rename it to 'date'
if 'date_layoffs' in data.columns:
    data.rename(columns={'date_layoffs': 'date'}, inplace=True)
    print("Columns in the DataFrame after renaming: ", data.columns)
else:
    print("Column 'date_layoffs' not found in DataFrame. Check the table schema or CSV column names.")


time_data = data[['date', 'year']].drop_duplicates().reset_index(drop=True)
time_data['date_id'] = time_data.index + 1

location_data = data[['location_hq', 'country', 'continent', 'latitude', 'longitude']].drop_duplicates().reset_index(drop=True)
location_data['location_id'] = location_data.index + 1

company_data = data[['company', 'industry', 'stage']].drop_duplicates().reset_index(drop=True)
company_data['company_id'] = company_data.index + 1

data = data.merge(time_data, on='date', how='left')  # 'left' parameter to ensure all rows in 'data' are kept
data = data.merge(location_data, on=['location_hq', 'country', 'continent', 'latitude', 'longitude'], how='left')
data = data.merge(company_data, on=['company', 'industry', 'stage'], how='left')

fact_data = data[['date_id', 'location_id', 'company_id', 'laid_off', 'percentage', 'company_size_before_layoffs', 'company_size_after_layoffs', 'money_raised_in_mil']]

time_data[['date_id', 'date', 'year']].to_sql('dim_time', engine, if_exists='append', index=False)
location_data[['location_id', 'location_hq', 'country', 'continent', 'latitude', 'longitude']].to_sql('dim_location', engine, if_exists='append', index=False)
company_data[['company_id', 'company', 'industry', 'stage']].to_sql('dim_company', engine, if_exists='append', index=False)
fact_data.to_sql('fact_layoffs', engine, if_exists='append', index=False)

print("ETL Process completed successfully.")



