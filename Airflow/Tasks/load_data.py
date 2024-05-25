import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
import logging
import os

# Configurando el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(merged_data):
    # Load environment variables
    load_dotenv()

    # Database connection parameters from .env
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_NAME')

    # Create database connection
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

    # Create tables if they do not exist
    create_tables_sql = """
    -- Create the time dimension table
    CREATE TABLE IF NOT EXISTS dim_time (
        date_id SERIAL PRIMARY KEY,
        date DATE,
        year INT
    );

    -- Create the location dimension table
    CREATE TABLE IF NOT EXISTS dim_location (
        location_id SERIAL PRIMARY KEY,
        location_hq VARCHAR(255),
        country VARCHAR(100),
        continent VARCHAR(100),
        latitude DECIMAL(9,6),
        longitude DECIMAL(9,6)
    );

    -- Create the company dimension table
    CREATE TABLE IF NOT EXISTS dim_company (
        company_id SERIAL PRIMARY KEY,
        company VARCHAR(255),
        industry VARCHAR(100),
        stage VARCHAR(100),
        mapped_industry VARCHAR(100),
        profit_margins DECIMAL(5,2),
        pe_ratio DECIMAL(10,2)
    );

    -- Create the fact table
    CREATE TABLE IF NOT EXISTS fact_layoffs (
        fact_id SERIAL PRIMARY KEY,
        date_id INT,
        location_id INT,
        company_id INT,
        laid_off INT,
        percentage DECIMAL(5,2),
        company_size_before_layoffs INT,
        company_size_after_layoffs INT,
        money_raised_in_mil DECIMAL(12,2),
        FOREIGN KEY (date_id) REFERENCES dim_time (date_id),
        FOREIGN KEY (location_id) REFERENCES dim_location (location_id),
        FOREIGN KEY (company_id) REFERENCES dim_company (company_id)
    );
    """

    # Execute the table creation SQL
    with engine.connect() as connection:
        connection.execute(text(create_tables_sql))

    # Verify that 'date_layoffs' column is present
    if 'date_layoffs' not in merged_data.columns:
        raise KeyError("The 'date_layoffs' column is missing from the data")

    # Transform Data
    # Ensure the 'date_layoffs' column is in datetime format
    merged_data['date_layoffs'] = pd.to_datetime(merged_data['date_layoffs'], errors='coerce')

    # Time dimension
    time_data = merged_data[['date_layoffs', 'year']].drop_duplicates().reset_index(drop=True)
    time_data.rename(columns={'date_layoffs': 'date'}, inplace=True)
    time_data['date_id'] = time_data.index + 1

    # Location dimension
    location_data = merged_data[['location_hq', 'country', 'continent', 'latitude', 'longitude']].drop_duplicates().reset_index(drop=True)
    location_data['location_id'] = location_data.index + 1

    # Company dimension
    company_data = merged_data[['company', 'industry', 'stage', 'mapped_industry', 'profit_margins', 'pe_ratio']].drop_duplicates().reset_index(drop=True)
    company_data['company_id'] = company_data.index + 1

    # Map IDs for fact table
    merged_data = merged_data.merge(time_data, left_on=['date_layoffs', 'year'], right_on=['date', 'year'])
    merged_data = merged_data.merge(location_data, on=['location_hq', 'country', 'continent', 'latitude', 'longitude'])
    merged_data = merged_data.merge(company_data, on=['company', 'industry', 'stage', 'mapped_industry', 'profit_margins', 'pe_ratio'])

    # Fact table
    fact_data = merged_data[['date_id', 'location_id', 'company_id', 'laid_off', 'percentage', 'company_size_before_layoffs', 'company_size_after_layoffs', 'money_raised_in_mil']]

    # Insert data into dimension tables
    time_data[['date_id', 'date', 'year']].to_sql('dim_time', engine, if_exists='append', index=False)
    location_data[['location_id', 'location_hq', 'country', 'continent', 'latitude', 'longitude']].to_sql('dim_location', engine, if_exists='append', index=False)
    company_data[['company_id', 'company', 'industry', 'stage', 'mapped_industry', 'profit_margins', 'pe_ratio']].to_sql('dim_company', engine, if_exists='append', index=False)
    fact_data.to_sql('fact_layoffs', engine, if_exists='append', index=False)

    logging.info("Load Process completed successfully.")
