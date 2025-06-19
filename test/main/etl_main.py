import sys
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from test.test_db.read_post import read_table
from test.test_db.write_post import write_parquet

# Step 1: Read from Postgres
df, spark = read_table("employees")

# Step 2: Write to timestamped Parquet
output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test_db/test_db_files"))
write_parquet(df, output_dir=output_dir, base_name="employees")

spark.stop()