from airflow import DAG
import datetime

default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': datetime.timedelta(seconds=10),
        'start_date': datetime.datetime(2020, 2, 2)
    }

with DAG(
    'fill_dm_revizion',
    default_args = default_args,
    schedule_interval = '*/30 * * * *',
    max_active_runs = 1,
    catchup = False
) as dag:
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    from airflow.operators.dummy_operator import DummyOperator
    from airflow.operators.python import PythonOperator

    sql_function = 'dm.dm_revizion_f()'
    run_sql_functions = f'SELECT {sql_function};'

    def start_task_f():
        print('Start task')

    start_task = PythonOperator(
            task_id = 'start_task',
            dag = dag,
            python_callable = start_task_f
            )

    def end_task_f():
        print('End task')

    end_task = PythonOperator(
            task_id = 'end_task',
            dag = dag,
            python_callable = end_task_f
            )
    
    fill_dm = PostgresOperator(
            task_id = "fill_dm_revizion",
            postgres_conn_id = "dwh_shops_connection",
            sql = run_sql_functions
            )
    
    start_task >> fill_dm >>  end_task