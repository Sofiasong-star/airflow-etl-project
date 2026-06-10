from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from operators.hello_operator import HelloOperator
import json
import urllib.request
import sqlite3
import logging

default_args = {
    'owner': 'sofia',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'currency_etl',
    default_args=default_args,
    description='Курс доллара из API (без доп. библиотек)',
    schedule_interval=timedelta(minutes=1),
    catchup=False,
)

def etl_currency():
    # 1. EXTRACT: получаем курс через urllib (встроенный модуль)
    url = 'https://api.exchangerate-api.com/v4/latest/USD'
    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read().decode())
        rate = data['rates']['RUB']
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f"Курс доллара: {rate} ₽")
    
    # 2. LOAD: сохраняем в SQLite
    conn = sqlite3.connect('/tmp/currency.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS usd_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            rate REAL
        )
    ''')
    
    cursor.execute('INSERT INTO usd_rates (timestamp, rate) VALUES (?, ?)', (timestamp, rate))
    conn.commit()
    
    cursor.execute('SELECT COUNT(*) FROM usd_rates')
    count = cursor.fetchone()[0]
    logging.info(f"Всего записей в базе: {count}")
    
    conn.close()

task = PythonOperator(
    task_id='etl_currency_task',
    python_callable=etl_currency,
    dag=dag,
)
