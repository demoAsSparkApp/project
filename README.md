# project
# IMDB Data Engineering Project using PySpark, Airflow, and PostgreSQL

This project demonstrates a production-ready ETL pipeline using PySpark for ingesting and processing IMDB datasets. It covers performance optimization, configuration tuning, scalable joins, caching strategies, and real-time streaming. The project includes local Airflow DAG orchestration and PostgreSQL integration.

---

## 📁 Project Structure
project/
├── data/
│   └── parquet_data/
├── dags/
│   └── imdb_dag.py
├── src/
│   ├── config/
│   │   └── spark_config.py, settings.py, title_ratings_config.py, …
│   ├── main/
│   │   └── name_basic.py, title_ratings.py
│   └── notebooks/
│       └── analysis.ipynb
├── requirements.txt
├── README.md
└── airflow/
└── airflow.cfg (optional)
---

## 🚀 How to Run Locally

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/imdb-pyspark-etl.git

## Create a Virtual Environment
cd imdb-pyspark-etl
python3 -m venv venv
source venv/bin/activate
3. Install Python Dependencies
pip install --upgrade pip
pip install -r requirements.txt
4. Start Airflow
# Set Airflow home directory
export AIRFLOW_HOME=$(pwd)/airflow

# Initialize and start Airflow
airflow db init
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin

# Start scheduler and webserver (use separate terminals)
airflow scheduler
airflow webserver --port 8080
5. Run DAG (from Airflow UI)
	•	Enable and trigger imdb_etl_dag (example DAG).
	•	It runs your ETL scripts in the src/main/ folder and writes outputs to:
	•	data/parquet_data/
	•	PostgreSQL (configured in settings.py)
⚙️ Configuration

Update the following in src/config/settings.py:
    JDBC_URL = "jdbc:postgresql://localhost:5432/imdb"
    DB_USER = "your_user"
    DB_PASSWORD = "your_password"

Run a Script Manually (for testing)

source venv/bin/activate
python test/main/**.py
