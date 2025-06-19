from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("✅ Hello from Airflow!")

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["example", "test"],
) as dag:

    task_hello = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello
    )

    task_hello