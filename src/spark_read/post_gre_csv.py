import os
import sys

# Add root project directory to PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from config import settings
from config.spark_config import get_spark_session, get_jdbc_properties
from pyspark.sql.functions import col, trim

# Define BASE_DIR for file paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))


# STEP 1: Get Spark session
spark = get_spark_session("Customer_CSV_ETL")
jdbc_props = get_jdbc_properties()

# STEP 2: Read CSV file
csv_path = os.path.join(BASE_DIR, "data", "raw_data", "customers.csv")
df_raw = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)

print("📥 Raw Data Preview:")
df_raw.show(truncate=False)

# STEP 3: Clean / transform
df_clean = df_raw \
    .drop("Index") \
    .withColumnRenamed("Customer Id", "customer_id") \
    .withColumnRenamed("First Name", "first_name") \
    .withColumnRenamed("Last Name", "last_name") \
    .withColumnRenamed("Company", "company") \
    .withColumnRenamed("City", "city") \
    .withColumnRenamed("Country", "country") \
    .withColumnRenamed("Phone 1", "phone1") \
    .withColumnRenamed("Phone 2", "phone2") \
    .withColumnRenamed("Email", "email") \
    .withColumnRenamed("Subscription Date", "subscription_date") \
    .withColumnRenamed("Website", "website") \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .dropna()

print("🧹 Cleaned Data:")
df_clean.show(truncate=False)

# STEP 4: Write to PostgreSQL
df_clean.write.jdbc(
    url=settings.JDBC_URL,
    table="customers_cleaned",
    mode="overwrite",
    properties=jdbc_props
)

print("✅ Data successfully loaded to PostgreSQL table: customers_cleaned")

# Stop Spark session
spark.stop()