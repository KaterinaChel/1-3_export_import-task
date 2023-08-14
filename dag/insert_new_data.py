from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from import_f101_2 import execute_f_101, create_table
from export import export_101_f
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag1 = DAG(
    'export_data',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
)

export_data = PythonOperator(
    task_id='export_table_f_101',
    python_callable=export_101_f,
    dag=dag1
)

export_data

dag2 = DAG(
    'execute_stored_procedures_f101_round_f_2',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
)

create_table = PythonOperator(
    task_id='create_table_f_101',
    python_callable=create_table,
    dag=dag2
)


load_procedures_f_101 = PythonOperator(
    task_id='execute_f_101',
    python_callable=execute_f_101,
    dag=dag2
)


create_table >> load_procedures_f_101
