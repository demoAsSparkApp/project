import os
import sys
from datetime import datetime

# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, PROJECT_ROOT)

from src.config.spark_config import get_spark_session
from src.test.horizontal_scaling_test import run_horizontal_scaling_test
import csv

# Simulate horizontal scaling by increasing local parallelism
test_cases = [
    {
        "name": "local_2_threads",
        "config": {
            "spark.master": "local[2]",
            "spark.sql.shuffle.partitions": "8",
            "spark.default.parallelism": "8"
        }
    },
    {
        "name": "local_4_threads",
        "config": {
            "spark.master": "local[4]",
            "spark.sql.shuffle.partitions": "16",
            "spark.default.parallelism": "16"
        }
    },
    {
        "name": "local_8_threads",
        "config": {
            "spark.master": "local[8]",
            "spark.sql.shuffle.partitions": "32",
            "spark.default.parallelism": "32"
        }
    }
]

results = []

for test in test_cases:
    print(f"\n[RUNNING TEST] {test['name']}")
    spark = get_spark_session(app_name=f"HorizontalScaling_{test['name']}", custom_config=test["config"])
    duration = run_horizontal_scaling_test(spark)
    results.append([test['name'], duration])
    spark.stop()

# Save results to CSV
with open("horizontal_scaling_results.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Config", "Time Taken (s)"])
    writer.writerows(results)

print("\n✅ All tests completed. Results saved to 'horizontal_scaling_results.csv'")