import pandas as pd
import os
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data_api():
    """Extract data from API source."""
    load_dotenv()
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")
    data_from_db_api = pd.read_sql_table('data_api', engine)
    logging.info("API data extracted successfully.")
    return data_from_db_api

def clean_data_api(**kwargs):
    """Clean and preprocess API data."""
    ti = kwargs['ti']
    data_from_db_api = ti.xcom_pull(task_ids='extract_api_data')
    if data_from_db_api is not None:
        data_from_db_api = data_from_db_api.applymap(lambda x: x.lower() if isinstance(x, str) else x)
        data_from_db_api.replace(to_replace=r'.*no\s+disponible.*', value=np.nan, regex=True, inplace=True)
        data_from_db_api.dropna(inplace=True)
        data_from_db_api.reset_index(drop=True, inplace=True)
        data_from_db_api.drop_duplicates(inplace=True)
        logging.info("API data cleaned and preprocessed.")
        return data_from_db_api
    else:
        logging.error("No data received from extract_api_data task.")

