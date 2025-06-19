from pyspark.sql import SparkSession
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config import settings  # Secure credentials loaded from .env
def read_table(table_name: str):
    """
    Read a table from PostgreSQL into a Spark DataFrame.

    Args:
        table_name (str): Name of the PostgreSQL table to read.

    Returns:
        tuple: (DataFrame, SparkSession)
    """
    spark = SparkSession.builder \
        .appName(f"Read_{table_name}_From_Postgres") \
        .config("spark.jars", settings.JDBC_PATH) \
        .getOrCreate()

    props = {
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    df = spark.read.jdbc(
        url=settings.JDBC_URL,
        table=table_name,
        properties=props
    )

    return df, spark


# ✅ Run directly to test
if __name__ == "__main__":
    df, spark = read_table("employees")
    df.show(truncate=False)
    spark.stop()