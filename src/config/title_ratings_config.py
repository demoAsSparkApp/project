import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

title_ratings_config  = {
    "spark_config": {
        "spark.executor.memory": "2g",
        "spark.driver.memory": "2g",
        "spark.executor.cores": "2",
        "spark.executor.memoryOverhead": "512",
        "spark.sql.shuffle.partitions": "4",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "3",
        "spark.shuffle.service.enabled": "true"
    },
    "source": {
        "file_path": os.path.join(BASE_DIR, "data", "raw_data", "title.ratings.csv"),
        "format": "csv",
        "has_header": False,
        "delimiter": ","
    },
    "target": {
        "parquet_path": "data/parquet_data/title_ratings.parquet",
        "table_name": "title_ratings"
    },
    "output": {
        "parquet_path": os.path.join(BASE_DIR, "data", "parquet_data", "title_ratings.parquet"),
        "table_name": "title_ratings"
    }
}