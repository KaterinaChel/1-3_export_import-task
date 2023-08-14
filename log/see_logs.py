
from airflow.providers.postgres.hooks.postgres import PostgresHook

def log_action(start_time, end_time=None, source=None, success=None, error_code=None, action_task=None):
    hook = PostgresHook(postgres_conn_id='postgres_ds')
    conn = hook.get_conn()

    with conn.cursor() as curs:
        curs.execute(
            """
            INSERT INTO logs.data_logs(start_time, end_time, source, success, error_code, action_task)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (start_time, end_time, source, success, error_code, action_task)
        )    

    conn.commit()
    conn.close()
