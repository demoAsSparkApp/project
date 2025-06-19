import os
import sys
from datetime import datetime

# Add project root to sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, PROJECT_ROOT)

from src.config.spark_config import get_spark_session, get_jdbc_properties
from src.config.settings import JDBC_URL
from src.config.title_ratings_config import title_ratings_config
from pyspark.sql.functions import col, lit

# 1. Initialize Spark Session
spark = get_spark_session(
    app_name="IMDB_Title_Ratings_ETL",
    custom_config=title_ratings_config["spark_config"]
)
jdbc_props = get_jdbc_properties()

# 2. Load CSV (corrected: no header, '|' separator)
csv_path = os.path.abspath(title_ratings_config["source"]["file_path"])
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"CSV file not found at: {csv_path}")
df_raw = spark.read \
    .option("header", False) \
    .option("sep", ",") \
    .option("inferSchema", True) \
    .csv(csv_path)

df_raw.printSchema()
df_raw.show(5, truncate=False)

# Rename columns only if 3 columns are read
if len(df_raw.columns) == 3:
    df = df_raw.toDF("tconst", "averageRating", "numVotes")
else:
    raise ValueError(f"Expected 3 columns but got {len(df_raw.columns)}")

# Continue transformation
df_clean = df \
    .withColumn("averageRating", col("averageRating").cast("double")) \
    .withColumn("numVotes", col("numVotes").cast("int")) \
    .withColumn("ingested_at", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

df_clean.printSchema()
df_clean.show(5, truncate=False)

# 4. Save as Parquet
parquet_path = os.path.abspath(title_ratings_config["target"]["parquet_path"])
df_clean.write.mode("overwrite").parquet(parquet_path)
print(f"✅ Saved Parquet to: {parquet_path}")

# 5. Write to PostgreSQL
df_clean.write.jdbc(
    url=JDBC_URL,
    table=title_ratings_config["target"]["table_name"],
    mode="overwrite",
    properties=jdbc_props
)
print("✅ Loaded to PostgreSQL!")

# 6. Stop Spark
spark.stop()