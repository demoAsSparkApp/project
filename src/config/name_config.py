import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

name_basic_config = {
    "spark_config": {
        "spark.sql.shuffle.partitions": "4",
        "spark.executor.memory": "2g"
    },
    "source": {
        "file_path": os.path.join(BASE_DIR, "data", "raw_data", "name.basics.csv"),
        "format": "csv",
        "has_header": False,
        "delimiter": ","
    },
    "target": {
        "parquet_path": "data/parquet_data/name_basics.parquet",
        "table_name": "imdb_name_basics"
    },
    "output": {
        "parquet_path": os.path.join(BASE_DIR, "data", "parquet_data", "name_basics.parquet"),
        "table_name": "imdb_name_basics"
    }
}