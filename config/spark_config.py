import os
from pyspark.sql import SparkSession
from config import settings

def get_spark_session(app_name="ETL_Job"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", settings.JDBC_PATH) \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .getOrCreate()
    return spark

def get_jdbc_properties():
    return {
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }