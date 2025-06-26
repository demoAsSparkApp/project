# template/python/vertical_scaling_test.py

from pyspark.sql.functions import col
import time

def run_vertical_scaling_test(spark):
    print("[INFO] Starting vertical scaling test...")

    df = spark.range(0, 10_000_000)

    # Cast to double to avoid long overflow
    df = df.withColumn("squared", (col("id").cast("double") * col("id").cast("double")))

    start_time = time.time()

    df.select("squared").agg({"squared": "sum"}).show()

    duration = time.time() - start_time
    print(f"[INFO] Time taken: {duration:.2f} seconds")
    return duration