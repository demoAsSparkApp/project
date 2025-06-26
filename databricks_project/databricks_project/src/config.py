# src/config.py

# ------------------------
# Dynamic Path Config
# ------------------------

# Base path for input files
INPUT_FOLDER = "/Volumes/workspace/imdb/stream_input/"

# Optional: output folder for free-tier (no Unity Catalog)
LOCAL_PARQUET_OUTPUT = "/tmp/processed/imdb_output/"

# Automatically complete input path
def get_input_path(filename: str) -> str:
    if not filename.endswith(".csv"):
        filename += ".csv"
    return f"{INPUT_FOLDER}{filename}"

def get_input_paths(file_list: list) -> list:
    return [get_input_path(fname) for fname in file_list]


# ------------------------
# Environment & Flags
# ------------------------

IS_PRODUCTION = False
IS_FREE_TIER = True  # Set True for Databricks Community Edition
USE_UNITY_CATALOG = not IS_FREE_TIER

# Unity Catalog naming
CATALOG = "workspace"
SCHEMA = "default"

# ------------------------
#  Delta Table Names
# ------------------------

TABLES = {
    "bronze": f"{CATALOG}.{SCHEMA}.imdb_bronze",
    "silver": f"{CATALOG}.{SCHEMA}.imdb_silver",
    "ml_features": f"{CATALOG}.{SCHEMA}.imdb_features"
}

# ------------------------
# Pipeline Config (for DLT or job reuse)
# ------------------------

DLT_PIPELINE_NAME = "etl_pipeline"

# ------------------------
# Optional Auto Paths
# ------------------------

def get_output_table(stage: str = "bronze") -> str:
    return TABLES.get(stage, f"{CATALOG}.{SCHEMA}.unknown_table")