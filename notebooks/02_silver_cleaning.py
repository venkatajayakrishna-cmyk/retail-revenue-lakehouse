# Databricks notebook source

from pyspark.sql import functions as F

# Read the Bronze table
bronze = spark.table("project.retail.bronze_transactions")

# Standardize column names and apply consistent data types
base = bronze.select(
    F.trim(F.col("InvoiceNo")).alias("invoice_no"),
    F.trim(F.col("StockCode")).alias("stock_code"),

    F.when(
        F.col("Description").isNull() |
        (F.trim(F.col("Description")) == ""),
        "Unknown"
    ).otherwise(F.trim(F.col("Description"))).alias("description"),

    F.col("Quantity").cast("int").alias("quantity"),
    F.col("InvoiceDate").cast("timestamp").alias("invoice_date"),
    F.col("UnitPrice").cast("decimal(12,2)").alias("unit_price"),
    F.col("CustomerID").cast("long").alias("customer_id"),

    F.when(
        F.col("Country").isNull() |
        (F.trim(F.col("Country")) == ""),
        "Unknown"
    ).otherwise(F.trim(F.col("Country"))).alias("country"),

    F.col("_ingested_at").alias("ingested_at"),
    F.col("_source_file").alias("source_file")
)

# Remove exact duplicate business records.
# Ingestion metadata is deliberately excluded from the comparison.
business_columns = [
    "invoice_no", "stock_code", "description", "quantity",
    "invoice_date", "unit_price", "customer_id", "country"
]

deduplicated = base.dropDuplicates(business_columns)

# Records that cannot be used reliably for sales analysis
invalid_condition = (
    F.col("invoice_no").isNull() |
    (F.col("invoice_no") == "") |
    F.col("stock_code").isNull() |
    (F.col("stock_code") == "") |
    F.col("invoice_date").isNull() |
    F.col("quantity").isNull() |
    (F.col("quantity") == 0) |
    F.col("unit_price").isNull() |
    (F.col("unit_price") <= 0)
)

# Separate rejected records so they can be investigated
quarantine_df = deduplicated.filter(invalid_condition)

# Keep valid transactions, including negative quantities and cancellations
silver_df = (
    deduplicated
    .filter(~invalid_condition)
    .withColumn(
        "is_cancelled",
        F.upper(F.col("invoice_no")).startswith("C")
    )
    .withColumn(
        "line_amount",
        (F.col("quantity") * F.col("unit_price"))
        .cast("decimal(18,2)")
    )
)

# Persist both outputs as Delta tables
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("project.retail.silver_transactions")
)

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("project.retail.quarantined_transactions")
)

# Report the results
print("Bronze rows:", bronze.count())
print("After deduplication:", deduplicated.count())
print("Silver valid rows:", silver_df.count())
print("Quarantined rows:", quarantine_df.count())

display(silver_df.limit(10))
