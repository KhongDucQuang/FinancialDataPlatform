from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello from Singapore Data Platform!")

with DAG('test_singapore_connection', start_date=datetime(2026, 1, 1), schedule_interval='@once') as dag:
    task = PythonOperator(task_id='hello_task', python_callable=hello)