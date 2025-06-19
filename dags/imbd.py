from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os
import subprocess

# Define paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data", "raw_data")
SCRIPT_DIR = os.path.join(BASE_DIR, "src", "main")
VENV_PYTHON = os.path.join(BASE_DIR, "venv", "bin", "python")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# Generic function to run script
def run_etl_script(script_name):
    script_path = os.path.join(SCRIPT_DIR, f"{script_name}.py")
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script not found: {script_path}")
    subprocess.run([VENV_PYTHON, script_path], check=True)

# Logging success
def log_success(task_name):
    print(f"✅ {task_name} completed successfully.")

with DAG(
    dag_id="imdb_with_sensor",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "etl", "sensor"]
) as dag:

    # 🔍 File sensors
    wait_for_name_basic_file = FileSensor(
        task_id="wait_for_name_basics_csv",
        filepath="name.basics.csv",
        fs_conn_id="fs_default",  # fs_default must point to your /data/raw_data
        poke_interval=10,
        timeout=300,
        mode="poke"
    )

    wait_for_title_ratings_file = FileSensor(
        task_id="wait_for_title_ratings_csv",
        filepath="title.ratings.csv",
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=300,
        mode="poke"
    )

    # ▶️ ETL tasks
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

    # 🔗 DAG Flow
    wait_for_name_basic_file >> run_name_basic >> log_name_basic
    wait_for_title_ratings_file >> run_title_ratings >> log_title_ratings