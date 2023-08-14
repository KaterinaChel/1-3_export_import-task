from sqlalchemy import create_engine
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from see_logs import log_action
from datetime import datetime




def create_table():
    conn = BaseHook.get_connection('postgres_ds')
    conn_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(conn_url)
    with engine.connect() as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 AS SELECT * FROM dm.dm_f101_round_f WHERE 1=0")





def execute_f_101():
    start_time = datetime.now()
    
    csv_file ='/home/system/doc/task3/export_f_101.csv'
    conn = BaseHook.get_connection('postgres_ds')
    conn_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(conn_url)

    try:
        with open(csv_file, 'r') as file:
            df = pd.read_csv(csv_file, sep=',', encoding='utf-8')
            df.to_sql('dm_f101_round_f_v2', engine, schema = 'dm', if_exists='replace', index=False)
            end_time=datetime.now()
            log_action(
                start_time,
                end_time,
                source=csv_file,
                success=True,
                error_code=None,
                action_task='load'
            )
    except Exception as e:
        error_message = f'Error during data loading: {str(e)}'
        end_time=datetime.now()
        log_action(
                start_time,
                end_time,
                source=csv_file,
                success=False,
                error_code=error_message,
                action_task='load'
            )
        raise


