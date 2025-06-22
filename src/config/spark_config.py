import os
from pyspark.sql import SparkSession
from src.config import settings

import os
from pyspark.sql import SparkSession
from src.config import settings

def get_spark_session(app_name="ETL_Job", custom_config=None):
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", settings.JDBC_PATH)

    # Default Spark config
    default_config = {
        # Executor/Driver resources
        "spark.executor.memory": "2g",
        "spark.executor.cores": "2",
        "spark.driver.memory": "2g",

        # Shuffle and parallelism
        "spark.sql.shuffle.partitions": "8",
        "spark.default.parallelism": "8",

        # Arrow optimization for Pandas <-> PySpark conversion
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
        "spark.sql.execution.arrow.pyspark.memory": "1g",
        "spark.sql.execution.arrow.pyspark.maxRecordsPerBatch": "10000",

        # Adaptive execution
        "spark.sql.adaptive.enabled": "true",

        # Prevent broadcast joins for large datasets
        "spark.sql.autoBroadcastJoinThreshold": "-1"
    }

    # Merge with any custom config passed
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