# RetailOps Analytics — End-to-End Data Engineering in Microsoft Fabric

This is my first real end-to-end data engineering project. I built it from scratch in Microsoft Fabric — from generating synthetic retail data all the way through to a Power BI report. No templates, no follow-along tutorials. Just me, a lakehouse, and a lot of coffee at Brot in Orange, CA.

I'm building this portfolio to land a data engineering role. If you're here because you're hiring or because you're on the same path — welcome.

---

## What I Built

A full retail analytics pipeline using the **medallion architecture** (Bronze → Silver → Gold) inside Microsoft Fabric. The project simulates a mid-size retail operation with 20K customers, 3K products, and 150K orders across multiple sales channels.

**The pipeline:**
1. Generate realistic retail data using Python (Faker + Pandas)
2. Ingest raw CSVs into Bronze Delta tables with metadata tracking
3. Clean, standardize, and deduplicate in the Silver layer
4. Model a star schema in Gold using Spark SQL
5. Build a semantic model on top of the Gold tables
6. Create a Power BI report for business consumption

Everything lives in a single Fabric workspace. One lakehouse. Four notebooks. One pipeline. One report.

---

## Architecture

```
Raw CSVs (Faker)
    │
    ▼
┌──────────────────┐
│   BRONZE LAYER   │  Notebook_01 — Raw ingest with batch ID,
│   (Delta Tables)  │  source tracking, ingestion timestamps
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   SILVER LAYER   │  Notebook_02 — Trim strings, cast types,
│   (Delta Tables)  │  dedupe by email (window functions), standardize
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│    GOLD LAYER    │  Notebook_03 — Star schema via Spark SQL
│   (Delta Tables)  │  dim_date, dim_customer, dim_product, fact_sales
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  SEMANTIC MODEL  │  RetailOps_SemanticModel — relationships,
│   + POWER BI     │  DAX measures, interactive report
└──────────────────┘
```

---

## Workspace Items

| Item | Type | What It Does |
|------|------|-------------|
| `Notebook_00_generate_data` | Notebook | Generates 20K customers, 3K products, 150K orders, ~360K line items using Faker |
| `Notebook_01_bronze_ingest` | Notebook | Reads CSVs from Files/raw/, adds metadata columns, writes Bronze Delta tables |
| `Notebook_02_Silver Clean and Conform` | Notebook | Standardizes strings, casts timestamps, dedupes customers by email using window functions |
| `Notebook_03_Gold Modeling (SQL executed via Spark)` | Notebook | Builds dim_date, dim_customer, dim_product, fact_sales, and aggregate tables in Spark SQL |
| `RetailOps_Bronze_Ingest` | Pipeline | Data Factory pipeline for orchestrating Bronze ingestion |
| `RetailOps_LH` | Lakehouse | Single lakehouse — all Bronze, Silver, and Gold tables live here |
| `RetailOps_SemanticModel` | Semantic Model | Star schema model with relationships and DAX measures |
| `RetailOps Analytics Report` | Report | Power BI report — revenue by month/category/channel, AOV, customer repeat analysis |

---

## Lakehouse Structure

```
RetailOps_LH/
├── Tables/
│   ├── dbo/
│   │   ├── bronze_customers
│   │   ├── bronze_orders
│   │   ├── bronze_order_items
│   │   ├── bronze_products
│   │   ├── silver_customers
│   │   ├── silver_orders
│   │   ├── silver_order_items
│   │   ├── silver_products
│   │   ├── gold_dim_date
│   │   ├── gold_dim_customer
│   │   ├── gold_dim_product
│   │   ├── gold_fact_sales
│   │   ├── gold_daily_sales
│   │   ├── gold_category_performance
│   │   └── gold_customer_value
│   │
├── Files/
│   ├── raw/retailops/
│   │   ├── customers.csv      (2 MB — 20K rows)
│   │   ├── orders.csv         (13 MB — 150K rows)
│   │   ├── order_items.csv    (20 MB — ~360K rows)
│   │   └── products.csv       (215 KB — 3K rows)
│   ├── archive/
│   └── processed/
```

---

## What Each Layer Actually Does

### Bronze — Raw Ingestion

Reads CSVs, adds three metadata columns (`_ingested_at`, `_source_file`, `_batch_id`), and writes to Delta. No transformations. Just a clean landing zone.

```python
def add_meta(df, source_name):
    return (df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_name))
        .withColumn("_batch_id", batch_id))
```

### Silver — Clean and Conform

This is where the real data engineering happens. For customers:
- Trim and standardize strings (initcap names, lowercase emails, uppercase states)
- Cast `created_at` to proper timestamp
- Deduplicate by email using a window function — keeps the most recent record

```python
w_email = Window.partitionBy("email").orderBy(F.col("created_at").desc_nulls_last())
customers_dedup = (customers
    .withColumn("_rn",
        F.when(F.col("email").isNotNull(), F.row_number().over(w_email)).otherwise(F.lit(1)))
    .filter(F.col("_rn") == 1)
    .drop("_rn"))
```

Similar patterns for products (enforce decimal types, clean strings) and orders (cast dates, validate statuses).

### Gold — Star Schema Modeling

Built entirely in Spark SQL using `CREATE OR REPLACE TABLE`:

- **`gold_dim_date`** — Generated date dimension (2024-01-01 through 2026-12-31) with year, quarter, month, day, day_name, week_of_year
- **`gold_dim_customer`** — Customer attributes from Silver
- **`gold_dim_product`** — Product catalog with brand, category, pricing, active status
- **`gold_fact_sales`** — Grain = one order line item. Joins orders + items + calculates gross_sales, discount_amount, net_sales

Plus three aggregate tables: `gold_daily_sales`, `gold_category_performance`, `gold_customer_value`.

### Semantic Model

Built on top of the Gold tables with proper relationships:
- `gold_fact_sales` → `gold_dim_date` (many-to-one on date_key/order_date)
- `gold_fact_sales` → `gold_dim_customer` (many-to-one on customer_id)
- `gold_fact_sales` → `gold_dim_product` (many-to-one on product_id)
- `Measure_Table` for DAX measures (AOV, etc.)

### Power BI Report

Single-page dashboard with:
- KPI cards: Recognized Revenue ($92.93M), Recognized Orders (128K), AOV ($727), Est. Gross Profit ($49.2M)
- Revenue by month (line chart)
- Revenue by category (bar chart — Training, Accessories, Cycling, Apparel, Nutrition, Running)
- Revenue by channel (horizontal bar — web, mobile, store, marketplace)
- Revenue Contribution by Repeat Bucket (combo chart)
- Customer Repeat Distribution (histogram)
- Date range slicer + channel/category filters

---

## Tools and Tech

- **Microsoft Fabric** (Trial capacity)
- **Lakehouse** with Delta tables on OneLake
- **PySpark** + **Spark SQL** in Fabric Notebooks
- **Python** — Faker, Pandas, NumPy for data generation
- **Data Factory** — Pipeline for orchestration
- **Semantic Model** — Star schema with DAX measures
- **Power BI** — Report layer

---

## Screenshots

| | |
|---|---|
| ![Workspace](screenshots/workspace_overview.png) | ![Lakehouse](screenshots/lakehouse_structure.png) |
| Workspace overview | Lakehouse with all medallion layers |
| ![Bronze Ingest](screenshots/notebook_bronze_ingest.png) | ![Silver Clean](screenshots/notebook_silver_clean.png) |
| Bronze ingestion with metadata | Silver dedup with window functions |
| ![Gold Modeling](screenshots/notebook_gold_model.png) | ![Report](screenshots/power_bi_report.png) |
| Gold star schema in Spark SQL | Final Power BI dashboard |
| ![Semantic Model](screenshots/semantic_model_overview.png) | ![Semantic Diagram](screenshots/semantic_model_diagram.png) |
| Semantic model tables | Star schema relationships |

---

## What I'd Do Differently / What's Next

This project was about getting the full pipeline working end to end. Now I'm building on it:

- **Pipeline orchestration** — Chain all notebooks in a master Data Factory pipeline with error handling, retry logic, and audit logging
- **Parameterization** — Make notebooks accept run_date, source_folder, and write mode from the pipeline
- **External data source** — Replace Faker with Copy Activity pulling from Azure Blob or SQL
- **Real-time project** — Eventstreams + Eventhouse + KQL for streaming analytics
- **Data Warehouse + dbt** — SQL-first transformations to show I can work in both paradigms

---

## How to Use This Repo

**If you have Fabric access:**
1. Create a new workspace
2. Import the `.ipynb` notebooks (Workspace → Import → Notebook)
3. Create a lakehouse called `RetailOps_LH` and attach it to each notebook
4. Run notebooks in order: 00 → 01 → 02 → 03
5. Build a semantic model from the Gold tables
6. Create your report

**If you just want to read the code:**
The notebooks in the `notebooks/` folder show the full pipeline logic. The `sample-data/` folder has small CSVs if you want to test locally.

---

## About Me

I'm Aaron — based in Orange County, CA. I'm transitioning into data engineering and building my skills by actually building things instead of just studying for exams. I also built [DirectLakeLab](https://directlakelab.com), a practice hub for Microsoft Fabric certifications.

Currently preparing for the **DP-600** and **DP-700** Fabric certifications.

If you have questions about the project or want to connect:
- [LinkedIn](https://linkedin.com/in/your-profile) *(update with your actual link)*
- [DirectLakeLab](https://directlakelab.com)

---

*This is Project 1 in my Data Engineering Portfolio Series.*
