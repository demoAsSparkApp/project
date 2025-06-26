# Databricks notebook source
# COMMAND ----------

# Automatically append the correct /src path dynamically (works in Databricks Workspace)
import sys
from pyspark.sql import SparkSession

# Get current user (email-style workspace ID)
# spark = SparkSession.builder.getOrCreate()
# current_user = spark.sparkContext._jvm.com.databricks.backend.daemon.dbutils.FileInfoUtils.getCurrentUser()

# # Define your base project folder name
# project_folder = "databricks_project"  # change this if you used another folder

# # Build the src path
# src_path = f"/Workspace/Users/{current_user}/{project_folder}/src"

# # Add to Python path if not already added
# if src_path not in sys.path:
#     sys.path.append(src_path)

# print(f"src path added: {src_path}")

# COMMAND ----------

# # COMMAND ----------
# #You’re working in Workspace folders, not Repos
# #Needs to match your real workspace structure
# import sys

# # ✅ This is your actual base path
# BASE_PATH = "/Workspace/databricks_project"

# # Build /src path
# SRC_PATH = f"{BASE_PATH}/src"

# # Add to sys.path if not already added
# if SRC_PATH not in sys.path:
#     sys.path.append(SRC_PATH)

# print(f"✅ src path added: {SRC_PATH}")

# COMMAND ----------

# # COMMAND ----------

# from config import get_input_path, get_input_paths, TABLES
# from utils import read_csv, show_schema, save_table

# COMMAND ----------

# dbutils.fs.ls("file:/Workspace/databricks_project/src")

# COMMAND ----------

# display(dbutils.fs.ls("dbfs:/Workspace/"))

# COMMAND ----------

# #Gets the current working dir (only works inside a Databricks Repo)
# import sys
# import os

# repo_root = os.getcwd()
# src_path = os.path.abspath(os.path.join(repo_root, "src"))

# if src_path not in sys.path:
#     sys.path.append(src_path)

# from config import get_input_path
# from utils import read_csv

# COMMAND ----------

# import os
# print(os.getcwd())

# COMMAND ----------

# COMMAND ----------

import sys

BASE_PATH = "/Workspace/databricks_project"
SRC_PATH = f"{BASE_PATH}/src"

if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

# ✅ Print to confirm
print(f"✅ src path added: {SRC_PATH}")

# COMMAND ----------

# COMMAND ----------

from config import get_input_path, get_input_paths, IS_PRODUCTION, IS_FREE_TIER, USE_UNITY_CATALOG, TABLES
from utils import read_csv, show_schema, clean_nulls, save_table
# COMMAND ----------



# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

# Input filenames (without path)
files = ["name.basics", "title.ratings"]

# Get full paths dynamically
paths = get_input_paths(files)

# Read both files into DataFrames (no headers)
df_basics = read_csv(spark, paths[0], header=False)
df_ratings = read_csv(spark, paths[1], header=False)

# Show schemas
show_schema(df_basics, "df_basics")
show_schema(df_ratings, "df_ratings")

# COMMAND ----------

# COMMAND ----------

# Corrected column names for 11 columns in name.basics.csv
basic_columns = [
    "nconst", "primaryName", "birthYear", "deathYear",
    "primaryProfession", "knownForTitles",
    "extra1", "extra2", "extra3", "extra4", "extra5"
]

# Only rename if number of columns match
if len(df_basics.columns) == len(basic_columns):
    df_basics = df_basics.toDF(*basic_columns)
else:
    raise ValueError(f"Expected {len(basic_columns)} columns, but got {len(df_basics.columns)}")

# Column names for title.ratings.csv
rating_columns = ["tconst", "averageRating", "numVotes"]

if len(df_ratings.columns) == len(rating_columns):
    df_ratings = df_ratings.toDF(*rating_columns)
else:
    raise ValueError(f"Expected {len(rating_columns)} columns, but got {len(df_ratings.columns)}")

# Show renamed schema
show_schema(df_basics, "Renamed df_basics")
show_schema(df_ratings, "Renamed df_ratings")

# COMMAND ----------

# COMMAND ----------

# Save basics table
save_table(
    df=df_basics,
    table_name=TABLES["bronze"] + "_names"  # e.g., workspace.default.imdb_bronze_names
)

# Save ratings table
save_table(
    df=df_ratings,
    table_name=TABLES["bronze"] + "_ratings"  # e.g., workspace.default.imdb_bronze_ratings
)

# COMMAND ----------

# COMMAND ----------

# Read the saved bronze tables
bronze_names = spark.read.table(TABLES["bronze"] + "_names")
bronze_ratings = spark.read.table(TABLES["bronze"] + "_ratings")

# Show schema for sanity
show_schema(bronze_names, "Bronze - Names")
show_schema(bronze_ratings, "Bronze - Ratings")

# Just preview 5 rows each
bronze_names.show(5)
bronze_ratings.show(5)

# COMMAND ----------

# COMMAND ----------

from pyspark.sql.functions import split, explode, trim,col

# 1. Split `knownForTitles` by comma into an array
df_exploded = bronze_names.withColumn("tconst", explode(split(col("knownForTitles"), ",")))

# 2. Optional: Trim to clean whitespace
df_exploded = df_exploded.withColumn("tconst", trim(col("tconst")))

# 3. Join with ratings
df_joined = df_exploded.join(bronze_ratings, on="tconst", how="left")

# 4. Drop unnecessary fields (if needed)
df_silver = df_joined.drop("knownForTitles")

# 5. Show sample
df_silver.show(5)

# COMMAND ----------

# Save bronze table first
save_table(df_basics, table_name=TABLES["bronze"])

# COMMAND ----------

# COMMAND ----------

# Save as Delta table using Unity Catalog
save_table(df_silver, table_name=TABLES["silver"])

# COMMAND ----------

save_table(df_silver, table_name=TABLES["silver"])