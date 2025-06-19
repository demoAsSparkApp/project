import os
from pyspark.sql import SparkSession
from src.config import settings

def get_spark_session(app_name="ETL_Job", custom_config=None):
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", settings.JDBC_PATH)

    # Default configs
    default_config = {
        "spark.executor.memory": "2g",
        "spark.executor.cores": "2",
        "spark.driver.memory": "2g",
        "spark.sql.shuffle.partitions": "4",
        "spark.default.parallelism": "4"
    }

    # Merge custom config if any
    final_config = {**default_config, **(custom_config or {})}
    for key, value in final_config.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()

def get_jdbc_properties():
    return {
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }