import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Fetch values from .env
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
db = os.getenv("POSTGRES_DB")

url = f"jdbc:postgresql://{host}:{port}/{db}"
properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("SecurePostgresConnection") \
    .config("spark.jars", "/Users/aryan/Desktop/project/postgresql-42.7.3.jar") \
    .getOrCreate()

# Sample Data
data = [("Nagendra", 30), ("Aryan", 25)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Write to Postgres
df.write.jdbc(url=url, table="customers", mode="append", properties=properties)

print("✅ Data securely written to PostgreSQL!")

spark.stop()