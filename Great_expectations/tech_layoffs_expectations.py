import json
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import great_expectations as ge
import numpy as np

def extract_and_clean_data():
    # Cargar las variables de entorno
    load_dotenv()

    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    print("Estableciendo conexión a la base de datos...")
    # Crear conexión a la base de datos
    connection_string = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)

    print("Extrayendo datos de la tabla 'tech_layoffs'...")
    # Extraer los datos de la tabla
    table_name = 'tech_layoffs'
    data_from_db = pd.read_sql_table(table_name, engine)

    # Limpieza de los datos
    # Cambio de tipo de datos
    data_from_db['money_raised_in_mil'] = pd.to_numeric(data_from_db['money_raised_in_mil'], errors='coerce')  

    # Normalización de texto en campos y registros
    data_from_db['industry'] = data_from_db['industry'].str.lower()
    data_from_db['stage'] = data_from_db['stage'].str.lower()

    # Eliminación de registros duplicados
    data_from_db.drop_duplicates(inplace=True)

    return data_from_db

def create_expectation_suite(data):
    # Creamos un dataset de GX a partir del DataFrame
    data_gx = ge.from_pandas(data)

    data_gx.expect_column_values_to_not_be_null("company")
    data_gx.expect_column_values_to_not_be_null("country")
    data_gx.expect_column_values_to_be_in_set("stage", ["seed", "series a", "series b", "series c", "series d", "acquired", "post-ipo", "private equity", "series i", "series j", "series h","subsidi"])
    data_gx.expect_column_values_to_be_in_set("industry", data["industry"].unique())
    data_gx.expect_column_mean_to_be_between("money_raised_in_mil", 0, 894.09)

    validation_result = data_gx.validate(result_format="SUMMARY")
    
    print(validation_result)

    # Obtener la suite de expectativas
    suite = data_gx.get_expectation_suite()

    return suite


cleaned_data = extract_and_clean_data()
suite = create_expectation_suite(cleaned_data)

# Imprimimos las expectativas
print("Expectativas:")
print(suite)

# Guardamos las expectativas en un archivo JSON
suite_json_dict = suite.to_json_dict()
with open('Great_expectations/tech_layoffs_expectations.json', 'w') as file:
    json.dump(suite_json_dict, file, indent=2)
