from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

# Base directory setup
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
VENV_PYTHON = os.path.join(BASE_DIR, "venv", "bin", "python")
SCRIPT_DIR = os.path.join(BASE_DIR, "src", "main")  # Assuming main contains your scripts

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# Generic function to run script by name (no .py required)
def run_etl_script(script_name):
    script_path = os.path.join(SCRIPT_DIR, f"{script_name}.py")
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script not found: {script_path}")
    subprocess.run([VENV_PYTHON, script_path], check=True)

# Success log function
def log_success(task_name):
    print(f"✅ {task_name} completed successfully.")

# Define DAG
with DAG(
    dag_id="imdb_ingest_runner",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "etl", "imdb"]
) as dag:

    run_name_basic = PythonOperator(
        task_id="run_name_basic_etl",
        python_callable=lambda: run_etl_script("name_basic"),
    )

    log_name_basic = PythonOperator(
        task_id="log_name_basic_success",
        python_callable=lambda: log_success("name_basic ETL"),
    )

    run_title_ratings = PythonOperator(
        task_id="run_title_ratings_etl",
        python_callable=lambda: run_etl_script("title_ratings"),
    )

    log_title_ratings = PythonOperator(
        task_id="log_title_ratings_success",
        python_callable=lambda: log_success("title_ratings ETL"),
    )

    # DAG flow
    run_name_basic >> log_name_basic >> run_title_ratings >> log_title_ratings