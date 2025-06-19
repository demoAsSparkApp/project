import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from config import settings
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StreamOrders") \
    .config("spark.jars", settings.JDBC_PATH) \
    .getOrCreate()

def process_batch(df, epoch_id):
    print(f"\n--- Epoch {epoch_id} ---")
    df.show()

# Simulated streaming via JDBC polling
df_stream = spark.readStream \
    .format("jdbc") \
    .option("url", settings.JDBC_URL) \
    .option("dbtable", "(SELECT * FROM orders WHERE order_date > NOW() - INTERVAL '15 minutes') AS recent_orders") \
    .option("user", settings.POSTGRES_USER) \
    .option("password", settings.POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_stream.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="30 seconds") \
    .start() \
    .awaitTermination()