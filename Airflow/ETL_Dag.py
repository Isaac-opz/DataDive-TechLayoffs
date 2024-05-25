from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import pandas as pd

sys.path.insert(0, '/home/isaac-opz/code/u/etl/DataDive-TechLayoffs/code')

from Tasks.data_api import extract_data_api, clean_data_api
from Tasks.data_dataset import extract_data_dataset, clean_data_dataset
from Tasks.merge_data import merge_task
from Tasks.load_data import load_data
from Tasks.kafka_stream import produce_to_kafka

default_args = {
    'owner': 'izkopz',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['isaac.piedrahita@uao.edu.co'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('DataDive-Dag',
          default_args=default_args,
          description='ETL DAG for Tech Layoffs data',
          schedule_interval=timedelta(days=1),
          catchup=False)

t1 = PythonOperator(
    task_id='extract_api_data',
    python_callable=extract_data_api,
    dag=dag,
)

t2 = PythonOperator(
    task_id='clean_api_data',
    python_callable=clean_data_api,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='extract_dataset_data',
    python_callable=extract_data_dataset,
    dag=dag,
)

t4 = PythonOperator(
    task_id='clean_dataset_data',
    python_callable=clean_data_dataset,
    provide_context=True,
    dag=dag,
)

t5 = PythonOperator(
    task_id='merge_datasets',
    python_callable=merge_task,
    provide_context=True,
    dag=dag,
)

def load_task(**kwargs):
    ti = kwargs['ti']
    merged_data = ti.xcom_pull(task_ids='merge_datasets')
    # Convert the merged data to a DataFrame if it is not already
    if not isinstance(merged_data, pd.DataFrame):
        merged_data = pd.DataFrame(merged_data)
    load_data(merged_data)

t6 = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    provide_context=True,
    dag=dag,
)

t7 = PythonOperator(
    task_id='produce_to_kafka',
    python_callable=produce_to_kafka,
    provide_context=True,
    dag=dag,
)

# Configure dependencies
t1 >> t2
t3 >> t4
[t2, t4] >> t5 >> t6 >> t7 # merge_datasets depends on clean_api_data and clean_dataset_data to finish
