# Databricks notebook source
from pyspark.sql import functions as F

# Location of our uploaded source data
raw_path = "/Volumes/project/retail/retail_raw/online_retail.csv"

# Read CSV using Spark
raw_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .load(raw_path)
)

# Inspect the dataset
print("Number of rows:", raw_df.count())
print("Number of columns:", len(raw_df.columns))

print("\nDataset schema:")
raw_df.printSchema()

display(raw_df.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

# Add ingestion metadata without changing the source values
bronze_df = (
    raw_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# Write the Bronze layer as a managed Delta table
table_name = "project.retail.bronze_transactions"

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)

print(f"Bronze table created: {table_name}")
print("Rows written:", spark.table(table_name).count())

display(spark.table(table_name).limit(10))