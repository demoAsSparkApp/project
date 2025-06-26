# Databricks notebook source

import sys

BASE_PATH = "/Workspace/databricks_project"
SRC_PATH = f"{BASE_PATH}/src"

if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

# ✅ Print to confirm
print(f"✅ src path added: {SRC_PATH}")

# COMMAND ----------

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

from utils import show_schema
from config import TABLES



# COMMAND ----------

# Load the features table
features_df = spark.read.table(TABLES["ml_features"])
show_schema(features_df, "features_df")



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview the feature table
# MAGIC SELECT * FROM workspace.default.imdb_features LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary view
# MAGIC CREATE OR REPLACE TEMP VIEW imdb_features_temp AS
# MAGIC SELECT
# MAGIC   CAST(numVotes AS DOUBLE) AS x,
# MAGIC   CAST(averageRating AS DOUBLE) AS y
# MAGIC FROM workspace.default.imdb_features
# MAGIC WHERE numVotes IS NOT NULL AND averageRating IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run regression manually
# MAGIC SELECT
# MAGIC   COUNT(*) AS n,
# MAGIC   ROUND(AVG(x), 2) AS avg_x,
# MAGIC   ROUND(AVG(y), 2) AS avg_y,
# MAGIC   ROUND(SUM(x * y), 2) AS sum_xy,
# MAGIC   ROUND(SUM(x * x), 2) AS sum_xx
# MAGIC FROM imdb_features_temp;

# COMMAND ----------

# Given values
n = 6101891
avg_x = 15043.13
avg_y = 6.53
sum_xy = 676502744127.07
sum_xx = 5.1054175244726264e16

# Step 1: Compute slope
numerator = sum_xy - (n * avg_x * avg_y)
denominator = sum_xx - (n * avg_x * avg_x)
slope = numerator / denominator

# Step 2: Compute intercept
intercept = avg_y - (slope * avg_x)

print(f"Slope (m): {slope}")
print(f"Intercept (b): {intercept}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   x AS numVotes,
# MAGIC   y AS actualRating,
# MAGIC   ROUND((1.26e-07 * x + 4.63), 2) AS predictedRating
# MAGIC FROM imdb_features_temp;

# COMMAND ----------

# COMMAND ----------

from pyspark.sql.functions import lit, round

# Assuming slope and intercept are already known
slope = 0.00000003  # Replace with your actual value
intercept = 4.63    # From your previous result

# Create predicted column manually
prediction_df = spark.sql("SELECT * FROM imdb_features_temp") \
    .withColumn("predictedRating", round((lit(slope) * col("x") + lit(intercept)), 2))

# Save as Delta table
prediction_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.imdb_predictions")

print("✅ Saved table: workspace.default.imdb_predictions")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.imdb_predictions LIMIT 10;

# COMMAND ----------

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array
from pyspark.ml.regression import LinearRegression

from utils import show_schema
from config import TABLES

# COMMAND ----------

# Get Spark session and read Delta table
spark = SparkSession.getActiveSession()
features_df = spark.read.table(TABLES["ml_features"])

# Show schema
show_schema(features_df, "IMDb Features")

# COMMAND ----------

# Select only numeric features for modeling
numeric_features = ["age", "numVotes"]

# Create a column "features" as an array of numeric values (VectorAssembler not needed)
features_df = features_df.withColumn("features", array(*[col(c).cast("double") for c in numeric_features]))

# Select label and features
final_df = features_df.select("features", "averageRating")

# Show sample data
final_df.show(5, truncate=False)

# COMMAND ----------

# Train/test split
train_df, test_df = final_df.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

# Train a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="averageRating")
model = lr.fit(train_df)

# COMMAND ----------

# Evaluate model on test data
predictions = model.transform(test_df)
predictions.select("features", "averageRating", "prediction").show(5, truncate=False)

# Show evaluation metrics
print(f"R2: {model.summary.r2:.4f}")
print(f"RMSE: {model.summary.rootMeanSquaredError:.4f}")

# COMMAND ----------

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.sql.functions import col, array
from pyspark.ml.regression import LinearRegression

from utils import show_schema
from config import TABLES

# COMMAND ----------

# Get Spark session and read feature table

features_df = spark.read.table(TABLES["ml_features"])

# View schema
show_schema(features_df, "IMDb Features")

# COMMAND ----------

# Define numeric input features (based on your actual columns)
numeric_features = ["age", "numVotes"]

# Create 'features' column as array of numeric columns
features_df = features_df.withColumn("features", array(*[col(c).cast("double") for c in numeric_features]))

# Final dataset with features and label
final_df = features_df.select("features", "averageRating")

# Show sample
final_df.show(5, truncate=False)

# COMMAND ----------

# Train/Test split
train_df, test_df = final_df.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

# MLflow experiment tracking
with mlflow.start_run(run_name="IMDb Rating Prediction"):
    lr = LinearRegression(featuresCol="features", labelCol="averageRating")
    model = lr.fit(train_df)

    # Log model with MLflow
    mlflow.spark.log_model(model, "lr_model")
    mlflow.log_params({
        "features": ",".join(numeric_features),
        "model_type": "LinearRegression"
    })

    # Evaluate
    predictions = model.transform(test_df)
    r2 = model.summary.r2
    rmse = model.summary.rootMeanSquaredError

    mlflow.log_metric("r2", r2)
    mlflow.log_metric("rmse", rmse)

    print(f"✅ Model Trained | R2: {r2:.4f}, RMSE: {rmse:.4f}")

# COMMAND ----------

# Filter out NULL ratings and cast to float
features_df = features_df.filter("averageRating IS NOT NULL") \
                         .withColumn("averageRating", col("averageRating").cast("float"))

# COMMAND ----------


# Select all numeric columns except target
feature_cols = [col for col in features_df.columns if col not in ["nconst", "tconst", "averageRating"]]

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler

# Optional: print columns for verification
print(features_df.columns)

# COMMAND ----------

# Exclude ID and target columns
feature_cols = [c for c in features_df.columns if c not in ["nconst", "tconst", "averageRating"]]

# COMMAND ----------

# assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# # Transform to get final_df with vector features + target
# final_df = assembler.transform(features_df).select("features", "averageRating")

# COMMAND ----------



assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
final_df = assembler.transform(features_df).select("features", "averageRating")
final_df.show(5, truncate=False)

# COMMAND ----------

numeric_cols = ["birthYear", "deathYear", "numVotes"]

# COMMAND ----------

from pyspark.sql.functions import array

features_df = features_df.withColumn("features", array(*[col(c).cast("double") for c in numeric_cols]))

final_df = features_df.select("features", "averageRating")
final_df.show(5, truncate=False)

# COMMAND ----------

# COMMAND ----------

train_df, test_df = final_df.randomSplit([0.8, 0.2], seed=42)

# Start MLflow experiment tracking
mlflow.set_experiment("/Users/nagendra@databricks.com/imdb_experiment")

with mlflow.start_run(run_name="LinearRegression_IMDb"):
    lr = LinearRegression(featuresCol="features", labelCol="averageRating")
    model = lr.fit(train_df)

    predictions = model.transform(test_df)

    rmse = model.summary.rootMeanSquaredError
    r2 = model.summary.r2

    # Log metrics
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)

    # Log model
    mlflow.spark.log_model(model, "imdb_lr_model")

    print(f"RMSE: {rmse:.3f}, R2: {r2:.3f}")

# COMMAND ----------

