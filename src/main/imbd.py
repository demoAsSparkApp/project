import os
import sys
from datetime import datetime
from pyspark.sql.functions import col, lit

# Add project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, PROJECT_ROOT)

from src.config.spark_config import get_spark_session, get_jdbc_properties
from src.config.settings import JDBC_URL
from src.config.imbd import title_config

# Start Spark
spark = get_spark_session("IMDB_Title_ETL", custom_config=title_config["spark_config"])
jdbc_props = get_jdbc_properties()

def process_csv(config_key, columns):
    config = title_config[config_key]
    df = spark.read.option("header", False).option("sep", ",").csv(config["file_path"])
    df = df.toDF(*columns)
    df = df.withColumn("ingested_at", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    
    df.write.mode("overwrite").parquet(config["parquet_path"])
    print(f"✅ Saved Parquet: {config['parquet_path']}")

    df.write.jdbc(url=JDBC_URL, table=config["table_name"], mode="overwrite", properties=jdbc_props)
    print(f"✅ Loaded to PostgreSQL table: {config['table_name']}")

# 1. title.basics.csv
process_csv("title_basics", [
    "tconst", "titleType", "primaryTitle", "originalTitle",
    "isAdult", "startYear", "endYear", "runtimeMinutes",
    "genre_1", "genre_2"
])

# 2. title.crew.csv
process_csv("title_crew", [
    "tconst", "directors", "writers"
])

# 3. title.episode.csv
process_csv("title_episode", [
    "tconst", "parentTconst", "seasonNumber", "episodeNumber"
])

# 4. title.akas.csv
process_csv("title_akas", [
    "titleId", "ordering", "title", "region", "language",
    "types", "attributes", "isOriginalTitle"
])

spark.stop()