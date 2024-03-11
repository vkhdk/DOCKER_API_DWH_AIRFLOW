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
    'postgres_test',
    default_args = default_args,
    schedule_interval = None,
    max_active_runs = 1,
    catchup = False
) as dag:
    from airflow.operators.postgres_operator import PostgresOperator
    from airflow.operators.python import PythonOperator

    test_sql = '''
        SELECT version();
    ''' 

    def start_task_f():
        print('Start task')

    start_task = PythonOperator(
            task_id = 'start_task',
            dag = dag,
            python_callable = start_task_f
            )

    def end_task_f(**kwarg):
        print('END task')

    end_task = PythonOperator(
            task_id = 'end_task',
            dag = dag,
            python_callable = end_task_f
            )
    
    postgres_test = PostgresOperator(
            task_id="postgres_test",
            postgres_conn_id="airflow_postgres_con",
            sql=test_sql
            )
    
    start_task >> postgres_test >>  end_task