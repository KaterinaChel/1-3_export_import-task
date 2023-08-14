from airflow.providers.postgres.hooks.postgres import PostgresHook
from see_logs import log_action
from datetime import datetime

def export_101_f():
    sql_script = """
    COPY (SELECT * FROM dm.dm_f101_round_f) TO STDOUT WITH CSV HEADER;
    """
    hook = PostgresHook(postgres_conn_id='postgres_ds')
    conn = hook.get_conn()

    try:
        with conn.cursor() as cursor:
            
            try:
                with open('/home/system/doc/task3/export_f_101.csv', 'w') as file:
                    start_time = datetime.now()
                    cursor.copy_expert(sql_script, file)
                    end_time = datetime.now()
                    log_action(
                    start_time,
                    end_time,
                    source='dm_f101_round_f',
                    success=True,
                    error_code=None,
                    action_task='export'
                )
            except Exception as e:
                error_message = f'Error during data loading: {str(e)}'
                end_time=datetime.now()
                log_action(
                    start_time,
                    end_time,
                    source='dm_f101_round_f',
                    success=False,
                    error_code=error_message,
                    action_task='export'
                )
                raise  
            finally:
                conn.commit()  

    finally:
        conn.close() 


 