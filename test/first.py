from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("VSCode Spark Test") \
    .getOrCreate()

# Create DataFrame
df = spark.range(5)
df.show()

spark.stop()