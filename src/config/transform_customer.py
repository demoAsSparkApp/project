import os
import sys
from pyspark.sql.functions import col, upper, lower, concat_ws, length, current_timestamp

# Setup path to access config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config import settings
from config.spark_config import get_spark_session, get_jdbc_properties

# Spark session
spark = get_spark_session("Customer_Transform_ETL")
jdbc_props = get_jdbc_properties()

# STEP 1: Read from PostgreSQL
df = spark.read.jdbc(
    url=settings.JDBC_URL,
    table="customers_cleaned",
    properties=jdbc_props
)

print("📥 Pulled data from PostgreSQL (customers_cleaned):")
df.show(truncate=False)

# STEP 2: Intermediate Transformations
df_transformed = df \
    .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) \
    .withColumn("email_lower", lower(col("email"))) \
    .withColumn("country_upper", upper(col("country"))) \
    .withColumn("name_length", length(col("full_name"))) \
    .withColumn("ingested_at", current_timestamp())

print("🔁 Transformed Data:")
df_transformed.show(truncate=False)

# STEP 3: Save to Parquet (Intermediate Layer)
parquet_path = os.path.join(settings.BASE_DIR, "data", "parquet_data", "customers_transformed.parquet")
df_transformed.write.mode("overwrite").parquet(parquet_path)
print(f"💾 Parquet saved to: {parquet_path}")

# STEP 4: Write back to PostgreSQL (target table)
df_transformed.write.jdbc(
    url=settings.JDBC_URL,
    table="customers_transformed",
    mode="overwrite",  # or append
    properties=jdbc_props
)

print("✅ Transformed data written to PostgreSQL: customers_transformed")

# Done
spark.stop()