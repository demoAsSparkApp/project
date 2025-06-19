from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Define base paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SCRIPT_PATH = os.path.join(BASE_DIR, "src", "config", "transform_customer.py")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

# Python task to print final message
def confirm_success():
    print("✅ Transformation complete. Data stored in Parquet and PostgreSQL.")

with DAG(
    dag_id="transform_etl_customer",
    default_args=default_args,
    schedule_interval=None,  # You can change to cron later
    catchup=False,
    tags=["spark", "etl", "transform"],
) as dag:

    start = BashOperator(
        task_id="start_transform",
        bash_command='echo "🔄 Starting Customer Transformation ETL..."',
    )

    run_transform = BashOperator(
        task_id="run_transform_script",
        bash_command=f"""
            source {BASE_DIR}/venv/bin/activate && \
            python {SCRIPT_PATH}
        """,
    )

    confirm = PythonOperator(
        task_id="confirm_transform_success",
        python_callable=confirm_success,
    )

    end = BashOperator(
        task_id="end_transform",
        bash_command='echo "🏁 Customer Transformation ETL Complete."',
    )

    start >> run_transform >> confirm >> end