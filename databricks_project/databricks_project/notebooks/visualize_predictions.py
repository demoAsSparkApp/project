# Databricks notebook source
# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

# Set visual style
sns.set(style="whitegrid")

print("✅ Libraries imported")

# COMMAND ----------

# COMMAND ----------

# Load the table from Unity Catalog
df_pred = spark.read.table("workspace.default.imdb_predictions")

# Show sample
df_pred.show(5)

# COMMAND ----------

# COMMAND ----------

# Rename x → numVotes and y → actualRating for easier plotting
df_pred = df_pred.withColumnRenamed("x", "numVotes").withColumnRenamed("y", "actualRating")

# Convert to Pandas for plotting (limit to 5000 rows)
pdf = df_pred.select("numVotes", "actualRating", "predictedRating").limit(5000).toPandas()

# Ensure numeric dtypes
pdf = pdf.astype({
    "numVotes": "float",
    "actualRating": "float",
    "predictedRating": "float"
})

pdf.head()

# COMMAND ----------

# COMMAND ----------

plt.figure(figsize=(10, 6))
sns.scatterplot(data=pdf, x="numVotes", y="actualRating", label="Actual", alpha=0.6)
sns.lineplot(data=pdf, x="numVotes", y="predictedRating", label="Predicted", color="red")
plt.title("Actual vs Predicted Rating by numVotes")
plt.xlabel("Number of Votes")
plt.ylabel("Rating")
plt.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# COMMAND ----------

pdf["error"] = pdf["actualRating"] - pdf["predictedRating"]

plt.figure(figsize=(8, 5))
sns.histplot(pdf["error"], kde=True, bins=40)
plt.title("Distribution of Prediction Errors")
plt.xlabel("Prediction Error")
plt.ylabel("Count")
plt.tight_layout()
plt.show()

# COMMAND ----------

# COMMAND ----------

plt.figure(figsize=(10, 6))
sns.scatterplot(data=pdf, x="predictedRating", y="error", alpha=0.4)
plt.axhline(0, color='red', linestyle='--')
plt.title("Residuals vs Predicted Rating")
plt.xlabel("Predicted Rating")
plt.ylabel("Residual (Actual - Predicted)")
plt.tight_layout()
plt.savefig("/Workspace/tmp/residuals_vs_predicted.png")
plt.show()

# COMMAND ----------

# COMMAND ----------

from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np

mae = mean_absolute_error(pdf["actualRating"], pdf["predictedRating"])
rmse = np.sqrt(mean_squared_error(pdf["actualRating"], pdf["predictedRating"]))
r2 = r2_score(pdf["actualRating"], pdf["predictedRating"])

print(f"📊 MAE:  {mae:.4f}")
print(f"📊 RMSE: {rmse:.4f}")
print(f"📊 R²:   {r2:.4f}")

# COMMAND ----------

# COMMAND ----------

# Save the scatter + lineplot (from Cell 4) again
plt.figure(figsize=(10, 6))
sns.scatterplot(data=pdf, x="numVotes", y="actualRating", label="Actual", alpha=0.6)
sns.lineplot(data=pdf, x="numVotes", y="predictedRating", label="Predicted", color="red")
plt.title("Actual vs Predicted Rating by numVotes")
plt.xlabel("Number of Votes")
plt.ylabel("Rating")
plt.legend()
plt.tight_layout()
plt.savefig("/Workspace/tmp/actual_vs_predicted.png")
plt.show()

# Save error distribution plot (from Cell 5) again
plt.figure(figsize=(8, 5))
sns.histplot(pdf["error"], kde=True, bins=40)
plt.title("Distribution of Prediction Errors")
plt.xlabel("Prediction Error")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig("/Workspace/tmp/error_distribution.png")
plt.show()

# COMMAND ----------

