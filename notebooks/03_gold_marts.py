# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE project.retail.gold_revenue_by_country
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     country,
# MAGIC     ROUND(SUM(line_amount), 2) AS net_revenue,
# MAGIC     SUM(quantity) AS net_units,
# MAGIC     COUNT(*) AS transaction_lines,
# MAGIC     COUNT(DISTINCT invoice_no) AS invoice_count
# MAGIC FROM project.retail.silver_transactions
# MAGIC GROUP BY country;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE project.retail.gold_daily_revenue
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     CAST(invoice_date AS DATE) AS sales_date,
# MAGIC     ROUND(SUM(line_amount), 2) AS net_revenue,
# MAGIC     SUM(quantity) AS net_units,
# MAGIC     COUNT(*) AS transaction_lines,
# MAGIC     COUNT(DISTINCT invoice_no) AS invoice_count
# MAGIC FROM project.retail.silver_transactions
# MAGIC GROUP BY CAST(invoice_date AS DATE);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE project.retail.gold_sales_fact
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (invoice_month)
# MAGIC AS
# MAGIC SELECT
# MAGIC     invoice_no,
# MAGIC     stock_code,
# MAGIC     description,
# MAGIC     quantity,
# MAGIC     invoice_date,
# MAGIC     CAST(date_trunc('MONTH', invoice_date) AS DATE) AS invoice_month,
# MAGIC     unit_price,
# MAGIC     customer_id,
# MAGIC     country,
# MAGIC     is_cancelled,
# MAGIC     line_amount,
# MAGIC     ingested_at
# MAGIC FROM project.retail.silver_transactions;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE project.retail.gold_sales_fact
# MAGIC ZORDER BY (country, invoice_date);

# COMMAND ----------

