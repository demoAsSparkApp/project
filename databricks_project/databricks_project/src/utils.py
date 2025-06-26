# src/utils.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import os

def read_csv(spark: SparkSession, path: str, header: bool = False, inferSchema: bool = True) -> DataFrame:
    try:
        print(f"Reading CSV from: {path}")
        return (
            spark.read
            .option("header", str(header).lower())
            .option("inferSchema", str(inferSchema).lower())
            .csv(path)
        )
    except Exception as e:
        print(f" Error reading file {path}: {str(e)}")
        return spark.createDataFrame([], schema=None)

def show_schema(df: DataFrame, name: str = ""):
    print(f"Schema for {name}:")
    df.printSchema()

def clean_nulls(df: DataFrame, columns: list) -> DataFrame:
    print(f"Dropping nulls from columns: {columns}")
    return df.dropna(subset=columns)

def save_table(df: DataFrame, path: str = None, table_name: str = None, mode: str = "overwrite"):
    if table_name:
        print(f"Saving as Unity Catalog table: {table_name}")
        df.write.format("delta").mode(mode).saveAsTable(table_name)
    elif path:
        print(f" Saving to path: {path}")
        df.write.format("parquet").mode(mode).save(path)
    else:
        raise ValueError("Please provide either a table name or path to save.")