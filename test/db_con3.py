import sys
import os

# Dynamically add parent `/pro/` folder to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import settings  # ✅ Now it will work!
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Build Spark session with dynamic jar path
spark = SparkSession.builder \
    .appName("SecurePostgresConnection") \
    .config("spark.jars", settings.JDBC_PATH) \
    .getOrCreate()

# Sample DataFrame
data = [("Nagendra", 30), ("Aryan", 25)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Write to PostgreSQL
df.write.jdbc(
    url=settings.JDBC_URL,
    table="customers",
    mode="append",
    properties={
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
)

print("✅ Data securely written to PostgreSQL!")
spark.stop()