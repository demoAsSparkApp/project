import os
import sys
from datetime import datetime

# Add project root to sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, PROJECT_ROOT)

from src.config.spark_config import get_spark_session

# Import the vertical scaling test function
from src.test.vertical_scaling_test import run_vertical_scaling_test
import csv

test_cases = [
    {
        "name": "2g_2cores",
        "config": {
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            "spark.driver.memory": "2g",
            "spark.sql.shuffle.partitions": "8",
            "spark.default.parallelism": "8"
        }
    },
    {
        "name": "4g_4cores",
        "config": {
            "spark.executor.memory": "4g",
            "spark.executor.cores": "4",
            "spark.driver.memory": "4g",
            "spark.sql.shuffle.partitions": "16",
            "spark.default.parallelism": "16"
        }
    },
    {
        "name": "6g_6cores",
        "config": {
            "spark.executor.memory": "6g",
            "spark.executor.cores": "6",
            "spark.driver.memory": "6g",
            "spark.sql.shuffle.partitions": "24",
            "spark.default.parallelism": "24"
        }
    },
]

results = []

for test in test_cases:
    print(f"\n[RUNNING TEST] {test['name']}")
    spark = get_spark_session(app_name=f"VerticalScaling_{test['name']}", custom_config=test["config"])
    duration = run_vertical_scaling_test(spark)
    results.append([test['name'], duration])
    spark.stop()

# Save results to CSV
with open("vertical_scaling_results.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Config", "Time Taken (s)"])
    writer.writerows(results)

print("\n✅ All tests completed. Results saved to 'vertical_scaling_results.csv'")