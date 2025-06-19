import os
import sys

# Make sure root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from config import settings
from config.spark_config import get_spark_session, get_jdbc_properties
from pyspark.sql.functions import trim, col

# STEP 1: Spark Setup
spark = get_spark_session("InMemory_ETL")
jdbc_props = get_jdbc_properties()
print("✅ Spark session started.")

# STEP 2: Generate Sample Data (simulate ingestion)
data = [
    (1, 1, "2025-06-19 15:18:00"),
    (2, 2, "2025-06-19 15:23:00 "),
    (3, 3, "2025-06-19 15:28:00"),
    (4, None, "2025-06-19 15:35:00"),
    (5, 2, None),
]
columns = ["order_id", "customer_id", "order_date"]
df_raw = spark.createDataFrame(data, columns)

print("📥 Raw Data:")
df_raw.show()

# STEP 3: Clean the Data
df_clean = df_raw \
    .withColumn("customer_id", col("customer_id").cast("int")) \
    .withColumn("order_date", trim(col("order_date"))) \
    .dropna()

print("🧹 Cleaned Data:")
df_clean.show()

# STEP 4: Write to Parquet
parquet_path = os.path.join(settings.BASE_DIR, "data", "processed", "orders_clean.parquet")
df_clean.write.mode("overwrite").parquet(parquet_path)
print(f"✅ Parquet saved at {parquet_path}")

# STEP 5: Load into PostgreSQL
df_parquet = spark.read.parquet(parquet_path)

df_parquet.write.jdbc(
    url=settings.JDBC_URL,
    table="orders_cleaned",
    mode="overwrite",
    properties=jdbc_props
)

print("✅ PostgreSQL load completed.")
spark.stop()