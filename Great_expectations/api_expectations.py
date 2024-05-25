import json
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import great_expectations as ge
import numpy as np

def extract_and_clean_data():
    load_dotenv()

    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    print("Estableciendo conexión a la base de datos...")
    connection_string = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)

    print("Extrayendo datos de la tabla 'data_api'...")
    table_name = 'data_api'  
    data_from_db = pd.read_sql_table(table_name, engine)


    data_from_db = data_from_db.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    data_from_db.replace(to_replace=r'.*no\s+disponible.*', value=np.nan, regex=True, inplace=True)
    data_from_db.dropna(inplace=True)
    data_from_db.reset_index(drop=True, inplace=True)
    

    data_from_db['Profit Margins'] = pd.to_numeric(data_from_db['Profit Margins'], errors='coerce')  
    data_from_db['PE Ratio'] = pd.to_numeric(data_from_db['PE Ratio'], errors='coerce')
    data_from_db['Full Time Employees'] = pd.to_numeric(data_from_db['Full Time Employees'], errors='coerce')
    

    data_from_db.rename(columns=lambda x: x.lower(), inplace=True)
    data_from_db['industry'] = data_from_db['industry'].str.lower()
    data_from_db['country'] = data_from_db['country'].str.lower()
    data_from_db['city'] = data_from_db['city'].str.lower()
    

    data_from_db.drop_duplicates(inplace=True)

    return data_from_db

def create_expectation_suite(data):
    # Creamos un dataset de GX a partir del DataFrame
    data_gx = ge.from_pandas(data)
    print(data_gx.info())
    
    # Definimos las expectativas para que no haya valores nulos en todas las columnas
    for col in data.columns:
        data_gx.expect_column_values_to_not_be_null(column=col)

    # Definimos las expectativas para que no haya valores duplicados en ninguna columna
    for col in data.columns:
        data_gx.expect_column_values_to_not_be_null(column=col)
        data_gx.expect_column_values_to_be_unique(column=col)
    

    if 'industry' in data.columns:
        data_gx.expect_column_values_to_not_be_null('industry')
    
    data_gx.expect_column_values_to_be_of_type('price', 'float')
    data_gx.expect_column_values_to_be_between('price', min_value=0, max_value=None)
    
    data_gx.expect_column_values_to_be_between('profit margins', min_value=0, max_value=1)
    
    data_gx.expect_column_values_to_be_of_type('pe ratio', 'float')
    data_gx.expect_column_values_to_be_between('pe ratio', min_value=0, max_value=None)
    
    data_gx.expect_column_values_to_be_of_type('full time employees', 'int')
    data_gx.expect_column_values_to_be_between('full time employees', min_value=0, max_value=None)
    
    data_gx.expect_column_values_to_not_be_null('country')
    data_gx.expect_column_values_to_not_be_null('city')

    # Obtenemos la suite de expectativas
    suite = data_gx.get_expectation_suite()
    
    # Ejecutar validación de las expectativas
    validation_result = data_gx.validate(result_format="SUMMARY")

    # Imprimir informe detallado
    print(validation_result)

    return suite


cleaned_data = extract_and_clean_data()
suite = create_expectation_suite(cleaned_data)

# Imprimimos las expectativas
print("Expectativas:")
print(suite)

# Guardamos las expectativas en un archivo JSON
suite_json_dict = suite.to_json_dict()
with open('Great_expectations/api_expectations.json', 'w') as file:
    json.dump(suite_json_dict, file, indent=2)
