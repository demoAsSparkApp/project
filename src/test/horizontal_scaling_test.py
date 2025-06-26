from pyspark.sql.functions import col
import time

def run_horizontal_scaling_test(spark):
    print("[INFO] Starting horizontal scaling test...")

    df = spark.range(0, 100_000_000)  # Large dataset

    # Prevent overflow
    df = df.withColumn("squared", col("id").cast("double") * col("id").cast("double"))

    start_time = time.time()

    df.select("squared").agg({"squared": "sum"}).show()

    duration = time.time() - start_time
    print(f"[INFO] Time taken: {duration:.2f} seconds")
    return duration