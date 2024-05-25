import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data_dataset():
    """Extract data from dataset."""
    load_dotenv()
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")
    data_from_db_layoffs = pd.read_sql_query("SELECT * FROM tech_layoffs;", engine)
    logging.info("Dataset data extracted successfully.")
    return data_from_db_layoffs

def clean_data_dataset(**kwargs):
    """Clean and preprocess dataset data."""
    ti = kwargs['ti']
    data_from_db_layoffs = ti.xcom_pull(task_ids='extract_dataset_data')
    if data_from_db_layoffs is not None:
        data_from_db_layoffs = data_from_db_layoffs.rename(columns={"#": "id", "Company": "company"})
        data_from_db_layoffs['money_raised_in_mil'] = data_from_db_layoffs['money_raised_in_mil'].replace('[\$,]', '', regex=True).astype(float)
        data_from_db_layoffs = data_from_db_layoffs[data_from_db_layoffs['stage'] != 'Unknown']
        data_from_db_layoffs = data_from_db_layoffs[data_from_db_layoffs['industry'] != 'Other']
        logging.info("Dataset data cleaned and preprocessed.")
        return data_from_db_layoffs
    else:
        logging.error("No data received from extract_dataset_data task.")