# Databricks notebook source
# COMMAND ----------

import sys

# ✅ Define your base project path
BASE_PATH = "/Workspace/databricks_project"
SRC_PATH = f"{BASE_PATH}/src"

# 🔁 Add /src to system path for importing modules
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

# ✅ Import config + utilities
from config import TABLES, get_output_table
from utils import show_schema, save_table

# COMMAND ----------

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

# Load bronze table (from basics CSV)
bronze_df = spark.read.table(TABLES["bronze"])

# Load ratings data from its Delta table (we saved it as bronze too)
ratings_df = spark.read.table(TABLES["bronze"] + "_ratings")

# Show schema of both
show_schema(bronze_df, "bronze_df")
show_schema(ratings_df, "ratings_df")

# COMMAND ----------

from pyspark.sql.functions import split, explode, trim

# Step 1: Explode knownForTitles from comma-separated to rows
exploded_df = (
    bronze_df
    .withColumn("title_id", explode(split("knownForTitles", ",")))
    .withColumn("title_id", trim("title_id"))
)

# Step 2: Join with ratings_df on title_id == tconst
joined_df = exploded_df.join(
    ratings_df,
    exploded_df["title_id"] == ratings_df["tconst"],
    how="left"
)

# Step 3: Drop duplicate or unneeded columns
joined_df = joined_df.drop("tconst")

# Step 4: Show schema
show_schema(joined_df, "Joined DF")

# COMMAND ----------

# from pyspark.sql.functions import size, when, length,col

# # Step 1: Add number of known titles per person
# features_df = (
#     joined_df
#     .withColumn("num_known_titles", size(split("knownForTitles", ",")))
# )

# # Step 2: Convert birthYear and deathYear to integers, calculate age
# features_df = (
#     features_df
#     .withColumn("birthYear", when(length("birthYear") > 0, col("birthYear").cast("int")))
#     .withColumn("deathYear", when(length("deathYear") > 0, col("deathYear").cast("int")))
#     .withColumn("age", when(col("deathYear").isNotNull(), col("deathYear") - col("birthYear")))
# )

# # Step 3: Keep relevant ML features only
# selected_cols = [
#     "nconst", "primaryName", "primaryProfession",
#     "averageRating", "numVotes", "num_known_titles", "age"
# ]

# features_df = features_df.select(*selected_cols)

# # Step 4: Show schema and sample
# show_schema(features_df, "features_df")
# features_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import size, split, when, col, expr

# Step 1: Add number of known titles per person
features_df = joined_df.withColumn(
    "num_known_titles", 
    size(split("knownForTitles", ","))
)

# Step 2: Convert birthYear and deathYear to int safely using try_cast
features_df = features_df.withColumn(
    "birthYear", expr("try_cast(birthYear as int)")
).withColumn(
    "deathYear", expr("try_cast(deathYear as int)")
)

# Step 3: Calculate age only if deathYear is not null
features_df = features_df.withColumn(
    "age", when(col("deathYear").isNotNull(), col("deathYear") - col("birthYear"))
)

# Step 4: Select relevant features
selected_cols = [
    "nconst", "primaryName", "primaryProfession",
    "averageRating", "numVotes", "num_known_titles", "age"
]

features_df = features_df.select(*selected_cols)

# Step 5: Show schema and sample
show_schema(features_df, "features_df")
features_df.show(5)

# COMMAND ----------

# Save the engineered features as a new Delta table in Unity Catalog
save_table(features_df, table_name=TABLES["ml_features"])

# COMMAND ----------

