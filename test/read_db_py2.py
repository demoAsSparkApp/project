from pyspark.sql import SparkSession
from datetime import datetime
import sys
import os
# (Optional in case of direct run)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from config import settings 
output_dir = os.path.join(os.path.dirname(__file__), "test_db_files")
os.makedirs(output_dir, exist_ok=True)

# Generate current timestamp-based filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
output_path = os.path.join(output_dir, f"employees_{timestamp}.parquet")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadFromPostgresAndWriteParquet") \
    .config("spark.jars", settings.JDBC_PATH) \
    .getOrCreate()

# Connection properties
props = {
    "user": settings.POSTGRES_USER,
    "password": settings.POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Read from PostgreSQL
df = spark.read.jdbc(
    url=settings.JDBC_URL,
    table="employees",
    properties=props
)

# Show the data
df.show()

# Save to timestamped Parquet file
df.write.mode("overwrite").parquet(output_path)

print(f"✅ Data saved to Parquet: {output_path}")
spark.stop()