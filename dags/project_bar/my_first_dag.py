from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_hello():
    print(f"Привет, Airflow! Сейчас: {datetime.now()}")

default_args = {
    'owner': 'sofia',
    'start_date': datetime(2025, 6, 2),
    'retries': 1,
}

dag = DAG(
    'my_first_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
)

task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag,
)
