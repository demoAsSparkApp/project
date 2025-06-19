from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Set up paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SCRIPT_PATH = os.path.join(BASE_DIR, "src", "spark_read", "post_gre_csv.py")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

def print_done():
    print("✅ PostgreSQL connection established. Data upload completed!")

with DAG(
    dag_id="ingenst_data_post_gre",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["csv", "postgres", "spark"],
) as dag:

    start = BashOperator(
        task_id="start_connection",
        bash_command='echo "🔌 PostgreSQL connection initiated..."',
    )

    run_csv_etl_job = BashOperator(
        task_id="run_csv_etl_job",
        bash_command=f"""
            source {BASE_DIR}/venv/bin/activate && \
            python {SCRIPT_PATH}
        """,
    )

    confirm_upload = PythonOperator(
        task_id="confirm_postgres_upload",
        python_callable=print_done
    )

    end = BashOperator(
        task_id="end_pipeline",
        bash_command='echo "🏁 ETL Pipeline Finished."',
    )

    start >> run_csv_etl_job >> confirm_upload >> end