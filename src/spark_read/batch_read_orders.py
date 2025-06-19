import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from config import settings
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BatchReadOrders") \
    .config("spark.jars", settings.JDBC_PATH) \
    .getOrCreate()

df = spark.read.jdbc(
    url=settings.JDBC_URL,
    table="orders",
    properties={
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
)

df.show()
spark.stop()