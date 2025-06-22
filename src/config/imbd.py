import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

title_config = {
    "spark_config": {
        "spark.sql.shuffle.partitions": "8",
        "spark.executor.memory": "1g"
    },
    "title_basics": {
        "file_path": os.path.join(BASE_DIR, "data", "raw_data", "title.basics.csv"),
        "parquet_path": os.path.join(BASE_DIR, "data", "parquet_data", "title_basics.parquet"),
        "table_name": "imdb_title_basics"
    },
    "title_crew": {
        "file_path": os.path.join(BASE_DIR, "data", "raw_data", "title.crew.csv"),
        "parquet_path": os.path.join(BASE_DIR, "data", "parquet_data", "title_crew.parquet"),
        "table_name": "imdb_title_crew"
    },
    "title_episode": {
        "file_path": os.path.join(BASE_DIR, "data", "raw_data", "title.episode.csv"),
        "parquet_path": os.path.join(BASE_DIR, "data", "parquet_data", "title_episode.parquet"),
        "table_name": "imdb_title_episode"
    },
    "title_akas": {
        "file_path": os.path.join(BASE_DIR, "data", "raw_data", "title.akas.csv"),
        "parquet_path": os.path.join(BASE_DIR, "data", "parquet_data", "title_akas.parquet"),
        "table_name": "imdb_title_akas"
    }
}