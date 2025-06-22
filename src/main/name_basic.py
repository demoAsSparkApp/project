import os
import sys
from datetime import datetime
# Ensure the project root (not src/) is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, PROJECT_ROOT)

from src.config.spark_config import get_spark_session, get_jdbc_properties
from src.config.settings import JDBC_URL
from src.config.name_config import name_basic_config
from pyspark.sql.functions import col, lit, concat_ws, lower, upper
from src.config.spark_config import get_spark_session, get_jdbc_properties
from src.config.settings import JDBC_URL
from src.config.name_config import name_basic_config

# Start Spark
spark = get_spark_session("IMDB_Name_Basics_ETL", custom_config=name_basic_config["spark_config"])
jdbc_props = get_jdbc_properties()

# Read CSV
csv_path = os.path.abspath(name_basic_config["source"]["file_path"])
df_raw = spark.read.option("header", False).option("sep", ",").csv(csv_path)

print("📥 Raw IMDB Data:")
df_raw.show(5, truncate=False)

# Rename columns
columns = [
    "nconst", "primaryName", "birthYear", "deathYear",
    "primaryProfession", "secondaryProfession", "tertiaryProfession",
    "knownForTitles_1", "knownForTitles_2", "knownForTitles_3", "knownForTitles_4"
]
df = df_raw.toDF(*columns)

# Clean data
df_clean = df.dropna(subset=["nconst", "primaryName"]) \
    .withColumn("full_name", col("primaryName")) \
    .withColumn("name_lower", lower(col("primaryName"))) \
    .withColumn("name_length", col("primaryName").cast("string").alias("len")) \
    .withColumn("ingested_at", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

df_clean.show(5)
csv_path = os.path.abspath(name_basic_config["source"]["file_path"])
parquet_path = os.path.abspath(name_basic_config["target"]["parquet_path"])
table_name = name_basic_config["target"]["table_name"]
# Save as Parquet
parquet_path = os.path.abspath(name_basic_config["target"]["parquet_path"])
df_clean.write.mode("overwrite").parquet(parquet_path)
print(f"✅ Saved cleaned data to Parquet: {parquet_path}")

# Write to PostgreSQL
df_clean.write.jdbc(
    url=JDBC_URL,
    table=name_basic_config["target"]["table_name"],
    mode="overwrite",
    properties=jdbc_props
)
print("✅ Loaded to PostgreSQL!")

spark.stop()