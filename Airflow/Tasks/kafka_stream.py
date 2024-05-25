import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from kafka import KafkaProducer
from json import dumps
import time
import logging

# Configurando el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def produce_to_kafka():
    load_dotenv()

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    conn = psycopg2.connect(
        dbname=DB_NAME, 
        user=DB_USER, 
        password=DB_PASSWORD, 
        host=DB_HOST, 
        port=DB_PORT
    )

    def execute_query(query):
        df = pd.read_sql(query, conn)
        return df

    # Datos los cuales queremos mostrar en nuestro dashboard en tiempo real
    custom_query = {
        "query": """
            SELECT 
                c.company_id, 
                c.company, 
                c.industry, 
                f.laid_off, 
                f.company_size_before_layoffs, 
                f.company_size_after_layoffs, 
                l.country, 
                t.year
            FROM 
                dim_company c
            JOIN 
                fact_layoffs f ON c.company_id = f.company_id
            JOIN 
                dim_location l ON f.location_id = l.location_id
            JOIN 
                dim_time t ON f.date_id = t.date_id;
        """
    }

    custom_df = execute_query(custom_query["query"])
    custom_dict = custom_df.to_dict(orient="records")
    conn.close()

    # --- Lógica Kafka Producer ---
    producer = KafkaProducer(
        value_serializer=lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers=['localhost:9092']
    )

    for row in custom_dict:
        producer.send('kafka_lab2', value=row)
        time.sleep(1)
    producer.close()
    
    logging.info("Data sent to Kafka successfully.")
