from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars", "/Users/aryan/Desktop/project/postgresql-42.7.3.jar") \
    .getOrCreate()

# Sample data
data = [("Nagendra", 30), ("Aryan", 25)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Write to PostgreSQL
url = "jdbc:postgresql://localhost:5432/demo_db"
properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

df.write.jdbc(url=url, table="customers", mode="append", properties=properties)

print("✅ Data written to PostgreSQL successfully!")

spark.stop()