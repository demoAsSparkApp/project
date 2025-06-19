from pyspark.sql import SparkSession

import sys
import os
# (Optional in case of direct run)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from config import settings 
# Start Spark session with JDBC
spark = SparkSession.builder \
    .appName("ReadFromPostgres") \
    .config("spark.jars", settings.JDBC_PATH) \
    .getOrCreate()

# JDBC Properties
props = {
    "user": settings.POSTGRES_USER,
    "password": settings.POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Read the PostgreSQL table
df = spark.read.jdbc(
    url=settings.JDBC_URL,
    table="employees",
    properties=props
)

df.show()
spark.stop()