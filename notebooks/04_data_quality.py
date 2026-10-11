# Databricks notebook source

from pyspark.sql import functions as F

bronze = spark.table("project.retail.bronze_transactions")
silver = spark.table("project.retail.silver_transactions")
quarantine = spark.table("project.retail.quarantined_transactions")

# Count records at each stage
bronze_count = bronze.count()
silver_count = silver.count()
quarantine_count = quarantine.count()

# Check that required fields are populated
missing_required = silver.filter(
    F.col("invoice_no").isNull()
    | (F.trim(F.col("invoice_no")) == "")
    | F.col("stock_code").isNull()
    | (F.trim(F.col("stock_code")) == "")
    | F.col("invoice_date").isNull()
    | F.col("quantity").isNull()
    | F.col("unit_price").isNull()
).count()

# Check that invalid sales values weren't allowed into Silver
invalid_values = silver.filter(
    (F.col("quantity") == 0)
    | (F.col("unit_price") <= 0)
).count()

# Check for remaining exact duplicate business records
business_columns = [
    "invoice_no", "stock_code", "description", "quantity",
    "invoice_date", "unit_price", "customer_id", "country"
]

duplicate_count = (
    silver.count()
    - silver.dropDuplicates(business_columns).count()
)

# Confirm Gold tables exist and contain data
country_count = spark.table(
    "project.retail.gold_revenue_by_country"
).count()

daily_count = spark.table(
    "project.retail.gold_daily_revenue"
).count()

checks = [
    ("bronze_rows", bronze_count),
    ("silver_rows", silver_count),
    ("quarantined_rows", quarantine_count),
    ("silver_missing_required_fields", missing_required),
    ("silver_invalid_values", invalid_values),
    ("silver_duplicate_rows", duplicate_count),
    ("gold_country_rows", country_count),
    ("gold_daily_rows", daily_count),
]

display(
    spark.createDataFrame(
        [(name, int(value)) for name, value in checks],
        ["check_name", "result"]
    )
)

# Fail the job if any critical quality check fails
assert missing_required == 0, "Silver has missing required fields"
assert invalid_values == 0, "Silver has invalid quantity or price"
assert duplicate_count == 0, "Silver contains duplicate business records"
assert country_count > 0, "Country revenue table is empty"
assert daily_count > 0, "Daily revenue table is empty"

print("All critical data-quality checks passed!")


# COMMAND ----------

