# Retail Revenue Lakehouse



An end-to-end cloud data engineering project built with **Databricks, PySpark, Delta Lake and SQL**. The project processes online retail transactions through Bronze, Silver and Gold layers, validates data quality, automates execution with Databricks Jobs, and presents business insights through an interactive dashboard.



## 1. Problem Statement



Retail businesses generate large volumes of transaction data. Before this data can support business decisions, it must be ingested, cleaned, validated and transformed into reliable analytical datasets.



This project builds a batch data pipeline for answering questions such as:



- Which countries generate the highest net revenue?

- How does revenue change over time?

- How many invoice lines and invoices are processed?

- How should returns, cancellations and invalid records be handled?

- How can data quality be checked automatically before results are used?



## 2. Dataset



**Source:** [UCI Online Retail Dataset](https://archive.ics.uci.edu/dataset/352/online%2Bretail)



The dataset contains approximately 542,000 transaction lines from a UK-based online retailer, covering transactions from 2010 to 2011.



Important fields include invoice number, product code, product description, quantity, invoice date, unit price, customer ID and country.



## 3. Architecture



```text

UCI Online Retail Dataset (CSV)

&#x20;             |

&#x20;             v

Databricks Unity Catalog Volume

&#x20;             |

&#x20;             v

Bronze Delta Table

Raw ingested transactions

&#x20;             |

&#x20;             v

Silver Transformation

Type normalization, deduplication,

validation and quarantine

&#x20;             |

&#x20;             v

Gold Analytical Tables

&#x20;             |

&#x20;      +------+------+

&#x20;      |             |

&#x20;      v             v

Revenue by       Daily Revenue

Country          Trend

&#x20;      |             |

&#x20;      +------+------+

&#x20;             |

&#x20;             v

Databricks Dashboard



Databricks Job:

Bronze Ingestion

&#x20;      |

&#x20;      v

Silver Cleaning

&#x20;      |

&#x20;      v

Gold Marts

&#x20;      |

&#x20;      v

Data Quality Checks

```



## 4. Technology Stack



| Technology | Purpose |

|---|---|

| Databricks Free Edition | Cloud development and execution environment |

| Apache Spark / PySpark | Distributed data processing and transformations |

| Delta Lake | Transactional table storage |

| Unity Catalog Volumes | Source file storage |

| Spark SQL | Analytical transformations |

| Databricks Jobs | Multi-task pipeline orchestration |

| Databricks AI/BI Dashboards | Data visualisation |



## 5. Data Pipeline



### Bronze Layer



Table: `project.retail.bronze_transactions`



The ingestion task reads the uploaded CSV using Spark and writes it to a Delta table. It records ingestion metadata to support traceability.



### Silver Layer



Table: `project.retail.silver_transactions`



The cleaning task:



- Standardizes column names and data types.

- Trims whitespace from text fields.

- Replaces missing descriptions and countries with `Unknown`.

- Removes duplicate records according to the selected business columns.

- Calculates line amount as quantity multiplied by unit price.

- Flags cancellation-related invoices.

- Preserves valid negative quantities so returns can contribute to net revenue.

- Separates records that fail required-field and sales-value checks.



### Quarantine Layer



Table: `project.retail.quarantined_transactions`



Records failing the selected validation rules are retained separately for investigation instead of being silently discarded.



### Gold Layer



The pipeline creates these analytical tables:



- `project.retail.gold_revenue_by_country` — revenue, units, transaction lines and invoice counts by country.

- `project.retail.gold_daily_revenue` — daily revenue and transaction aggregates.

- `project.retail.gold_sales_fact` — transaction-level fact table partitioned by invoice month.



The sales fact table is optimised using Delta `OPTIMIZE` and Z-ordering on country and invoice date. This design is intended to support common country and date filters; performance improvement should be measured on representative workloads.



## 6. Data Quality



The final workflow includes automated checks for:



- Required-field completeness in Silver.

- Invalid quantities and unit prices.

- Duplicate business records.

- Non-empty Gold analytical tables.

- Record counts at different pipeline stages.



### Verified sample run



| Metric | Result |

|---|---:|

| Bronze rows | 541,909 |

| Silver rows | 534,125 |

| Quarantined rows | 2,516 |

| Rows removed by deduplication | 5,268 |

| Silver net line revenue | £9,748,131.07 |



Counts and revenue values describe the recorded successful run and should be revalidated when the pipeline is rerun.



## 7. Dashboard



The dashboard provides two main visualisations:



1. **Revenue by Country:** A bar chart comparing net line revenue across countries.

2. **Revenue Trend:** A time-series chart showing how net line revenue changes over time.



The Gold tables separate transformation logic from visualisation, making the dashboard simpler to maintain.



## 8. Orchestration



The Databricks Job contains four dependent tasks:



1. `bronze_ingestion`

2. `silver_cleaning`

3. `gold_marts`

4. `data_quality`



Each task must succeed before the dependent task can run. The workflow has been executed successfully in Databricks.



The current pipeline uses a full-refresh approach. Incremental ingestion, automated scheduling and recovery from partial processing are potential future improvements.



## 9. Reproducing the Project



1. Create a Databricks workspace with access to Unity Catalog, volumes and serverless compute.

2. Download the UCI Online Retail dataset.

3. Upload the source CSV to a volume in the workspace.

4. Import the notebook source files from this repository.

5. Update source paths and catalog/schema names to match your environment.

6. Run the Bronze, Silver, Gold and data-quality notebooks in order.

7. Configure the four-task Databricks Job and execute it.

8. Create or refresh the dashboard using the Gold tables.



Workspace names, resource availability and permissions may differ between Databricks environments.



## 10. Limitations and Future Improvements



- The current pipeline performs a full refresh instead of incremental processing.

- Deduplication is based on matching selected business fields; identical invoice lines may require more careful business-level investigation.

- Missing customer IDs are retained, so customer-level analysis may cover only a subset of transactions.

- Revenue represents quantity multiplied by unit price, with negative quantities reducing revenue. Its business definition should not be assumed to equal accounting revenue without reconciliation.

- The current project has not yet implemented Terraform-based infrastructure provisioning.

- Further improvements could include automated unit tests, CI/CD, incremental loading, a run-audit table and measured performance comparisons.



## 11. Responsible Data Engineering



The pipeline keeps rejected records available for investigation, validates outputs before downstream use, and documents business assumptions affecting revenue calculations.



No credentials or secrets should be committed to the repository.



## 12. Acknowledgements



Dataset: UCI Machine Learning Repository, Online Retail.



Built as a hands-on data engineering portfolio project using Databricks and open-source data engineering technologies.

