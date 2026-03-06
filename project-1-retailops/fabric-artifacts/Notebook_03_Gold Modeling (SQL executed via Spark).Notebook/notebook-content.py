# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0ccb13ab-c4a2-4919-abdb-4c4966e4adf0",
# META       "default_lakehouse_name": "RetailOps_LH",
# META       "default_lakehouse_workspace_id": "bfb13f36-a445-478e-a53d-9f19dcb161ab",
# META       "known_lakehouses": [
# META         {
# META           "id": "0ccb13ab-c4a2-4919-abdb-4c4966e4adf0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================
# PARAMETERS
# ============================
run_date = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Cell 1 Dim Date

spark.sql("""
CREATE OR REPLACE TABLE gold_dim_date AS
WITH dates AS (
  SELECT explode(sequence(to_date('2024-01-01'), to_date('2026-12-31'), interval 1 day)) AS d
)
SELECT
  d                                   AS date_key,
  year(d)                              AS year,
  quarter(d)                           AS quarter,
  month(d)                             AS month,
  date_format(d, 'MMMM')               AS month_name,
  day(d)                               AS day,
  date_format(d, 'E')                  AS day_name,
  weekofyear(d)                        AS week_of_year
FROM dates
""")
print("gold_dim_date:", spark.table("gold_dim_date").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2 — DimCustomer

spark.sql("""
CREATE OR REPLACE TABLE gold_dim_customer AS
SELECT
  customer_id,
  created_at,
  first_name,
  last_name,
  email,
  phone,
  city,
  state,
  postal_code,
  country,
  loyalty_tier,
  is_active
FROM silver_customers
""")
print("gold_dim_customer:", spark.table("gold_dim_customer").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3 — DimProduct

spark.sql("""
CREATE OR REPLACE TABLE gold_dim_product AS
SELECT
  product_id,
  sku,
  product_name,
  category,
  brand,
  unit_cost,
  list_price,
  is_discontinued,
  effective_from,
  effective_to
FROM silver_products
""")
print("gold_dim_product:", spark.table("gold_dim_product").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4 — FactSales (star fact table)

spark.sql("""
CREATE OR REPLACE TABLE gold_fact_sales AS
SELECT
  oi.order_item_id,
  o.order_id,
  o.customer_id,
  oi.product_id,
  to_date(o.order_datetime)        AS order_date,
  o.order_datetime,
  o.order_status,
  o.channel,
  o.payment_method,
  oi.quantity,
  oi.unit_price,
  oi.discount_amount,
  oi.tax_amount,
  oi.gross_sales,
  oi.net_sales
FROM silver_order_items oi
JOIN silver_orders o
  ON oi.order_id = o.order_id
""")
print("gold_fact_sales:", spark.table("gold_fact_sales").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold Aggregates (3 tables)
# Cell 5 — Daily Sales

spark.sql("""
CREATE OR REPLACE TABLE gold_daily_sales AS
SELECT
  order_date,
  channel,
  COUNT(DISTINCT order_id) AS orders,
  SUM(gross_sales)         AS gross_sales,
  SUM(net_sales)           AS net_sales,
  SUM(tax_amount)          AS tax_amount,
  SUM(CASE WHEN order_status IN ('cancelled','refunded') THEN net_sales ELSE 0 END) AS cancelled_refunded_net_sales,
  SUM(CASE WHEN order_status NOT IN ('cancelled','refunded') THEN net_sales ELSE 0 END) AS recognized_net_sales
FROM gold_fact_sales
GROUP BY order_date, channel
""")
print("gold_daily_sales rows:", spark.table("gold_daily_sales").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6 — Category Performance

spark.sql("""
CREATE OR REPLACE TABLE gold_category_performance AS
SELECT
  fs.order_date,
  p.category,
  COUNT(DISTINCT fs.order_id) AS orders,
  SUM(fs.quantity)            AS units,
  SUM(fs.net_sales)           AS net_sales,
  SUM(fs.quantity * p.unit_cost) AS est_cogs,
  SUM(fs.net_sales) - SUM(fs.quantity * p.unit_cost) AS est_gross_profit
FROM gold_fact_sales fs
JOIN gold_dim_product p
  ON fs.product_id = p.product_id
WHERE fs.order_status NOT IN ('cancelled','refunded')
GROUP BY fs.order_date, p.category
""")
print("gold_category_performance rows:", spark.table("gold_category_performance").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 7 — Customer Value

spark.sql("""
CREATE OR REPLACE TABLE gold_customer_value AS
SELECT
  customer_id,
  COUNT(DISTINCT order_id) AS lifetime_orders,
  SUM(net_sales)           AS lifetime_net_sales,
  AVG(net_sales)           AS avg_order_value,
  MAX(order_datetime)      AS last_order_datetime
FROM gold_fact_sales
WHERE order_status NOT IN ('cancelled','refunded')
GROUP BY customer_id
""")
print("gold_customer_value:", spark.table("gold_customer_value").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation (don’t skip this)
# Cell 8 — quick sanity checks

spark.sql("SELECT order_status, COUNT(*) cnt FROM gold_fact_sales GROUP BY order_status ORDER BY cnt DESC").show()
spark.sql("SELECT channel, COUNT(DISTINCT order_id) orders FROM gold_fact_sales GROUP BY channel ORDER BY orders DESC").show()
spark.sql("SELECT MIN(order_date) min_date, MAX(order_date) max_date FROM gold_fact_sales").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Fix customer_value (and make your Gold more robust)
## Step 1 — Inspect statuses in Gold

spark.sql("""
SELECT order_status, COUNT(*) AS cnt
FROM gold_fact_sales
GROUP BY order_status
ORDER BY cnt DESC
""").show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 2 — Create a cleaned version of FactSales (overwrite)

# Run this to hard-normalize status/channel/payment at the Gold layer:

spark.sql("""
CREATE OR REPLACE TABLE gold_fact_sales AS
SELECT
  oi.order_item_id,
  o.order_id,
  o.customer_id,
  oi.product_id,
  to_date(o.order_datetime)                          AS order_date,
  o.order_datetime,
  lower(trim(o.order_status))                        AS order_status,
  lower(trim(o.channel))                             AS channel,
  lower(trim(o.payment_method))                      AS payment_method,
  oi.quantity,
  oi.unit_price,
  oi.discount_amount,
  oi.tax_amount,
  oi.gross_sales,
  oi.net_sales
FROM silver_order_items oi
JOIN silver_orders o
  ON oi.order_id = o.order_id
""")
print("gold_fact_sales:", spark.table("gold_fact_sales").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3 — Rebuild gold_customer_value

spark.sql("""
CREATE OR REPLACE TABLE gold_customer_value AS
SELECT
  customer_id,
  COUNT(DISTINCT order_id) AS lifetime_orders,
  SUM(net_sales)           AS lifetime_net_sales,
  AVG(net_sales)           AS avg_order_value,
  MAX(order_datetime)      AS last_order_datetime
FROM gold_fact_sales
WHERE order_status NOT IN ('cancelled','refunded')
GROUP BY customer_id
""")
print("gold_customer_value:", spark.table("gold_customer_value").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 4 — Rebuild the two aggregates (since FactSales changed)

spark.sql("""
CREATE OR REPLACE TABLE gold_daily_sales AS
SELECT
  order_date,
  channel,
  COUNT(DISTINCT order_id) AS orders,
  SUM(gross_sales)         AS gross_sales,
  SUM(net_sales)           AS net_sales,
  SUM(tax_amount)          AS tax_amount,
  SUM(CASE WHEN order_status IN ('cancelled','refunded') THEN net_sales ELSE 0 END) AS cancelled_refunded_net_sales,
  SUM(CASE WHEN order_status NOT IN ('cancelled','refunded') THEN net_sales ELSE 0 END) AS recognized_net_sales
FROM gold_fact_sales
GROUP BY order_date, channel
""")

spark.sql("""
CREATE OR REPLACE TABLE gold_category_performance AS
SELECT
  fs.order_date,
  p.category,
  COUNT(DISTINCT fs.order_id) AS orders,
  SUM(fs.quantity)            AS units,
  SUM(fs.net_sales)           AS net_sales,
  SUM(fs.quantity * p.unit_cost) AS est_cogs,
  SUM(fs.net_sales) - SUM(fs.quantity * p.unit_cost) AS est_gross_profit
FROM gold_fact_sales fs
JOIN gold_dim_product p
  ON fs.product_id = p.product_id
WHERE fs.order_status NOT IN ('cancelled','refunded')
GROUP BY fs.order_date, p.category
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sanity query check

spark.sql("""
SELECT
  COUNT(DISTINCT customer_id) AS customers_any,
  COUNT(DISTINCT CASE WHEN order_status NOT IN ('cancelled','refunded') THEN customer_id END) AS customers_recognized
FROM gold_fact_sales
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# That’s coming from your synthetic generator: the Zipf distribution is extremely skewed, so a tiny fraction of customers account for almost all orders.

# Not “Pareto 80/20” skew. More like “99.9/0.1” skew.

# Confirm it (run this)

spark.sql("""
SELECT
  COUNT(DISTINCT customer_id) AS distinct_customers_in_orders
FROM silver_orders
""").show()

spark.sql("""
SELECT
  COUNT(DISTINCT customer_id) AS distinct_customers_in_recognized
FROM gold_fact_sales
WHERE order_status NOT IN ('cancelled','refunded')
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# rebuild gold_customer_value as a left join from customers → aggregated sales.

# Replace gold_customer_value with this


spark.sql("""
CREATE OR REPLACE TABLE gold_customer_value AS
WITH agg AS (
  SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS lifetime_orders,
    SUM(net_sales)           AS lifetime_net_sales,
    AVG(net_sales)           AS avg_order_value,
    MAX(order_datetime)      AS last_order_datetime
  FROM gold_fact_sales
  WHERE order_status NOT IN ('cancelled','refunded')
  GROUP BY customer_id
)
SELECT
  c.customer_id,
  COALESCE(a.lifetime_orders, 0)        AS lifetime_orders,
  COALESCE(a.lifetime_net_sales, 0.00)  AS lifetime_net_sales,
  a.avg_order_value                     AS avg_order_value,
  a.last_order_datetime                 AS last_order_datetime
FROM gold_dim_customer c
LEFT JOIN agg a
  ON c.customer_id = a.customer_id
""")

print("gold_customer_value:", spark.table("gold_customer_value").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quick validation (to confirm the fix worked)

# After you rebuild Bronze/Silver/Gold, run:

spark.sql("""
SELECT COUNT(DISTINCT customer_id) AS customers_in_orders
FROM silver_orders
""").show()

spark.sql("""
SELECT COUNT(DISTINCT customer_id) AS customers_with_recognized_orders
FROM gold_fact_sales
WHERE order_status NOT IN ('cancelled','refunded')
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sanity checks

spark.sql("""
SELECT
  MIN(lifetime_orders),
  MAX(lifetime_orders),
  AVG(lifetime_orders)
FROM gold_customer_value
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT
  APPROX_PERCENTILE(lifetime_orders, 0.5) AS median_orders
FROM gold_customer_value
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2 — DimCustomer
spark.sql("""
CREATE OR REPLACE TABLE gold_dim_customer AS
SELECT
    c.customer_id,
    c.created_at,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.city,
    c.state,
    c.postal_code,
    c.country,
    c.loyalty_tier,
    c.is_active,
    CASE
        WHEN order_counts.order_count IS NULL THEN '0'
        WHEN order_counts.order_count = 1  THEN '1'
        WHEN order_counts.order_count <= 3  THEN '2-3'
        WHEN order_counts.order_count <= 5  THEN '4-5'
        WHEN order_counts.order_count <= 10 THEN '6-10'
        WHEN order_counts.order_count <= 20 THEN '11-20'
        ELSE '20+'
    END AS Order_Bucket
FROM silver_customers c
LEFT JOIN (
    SELECT customer_id, COUNT(DISTINCT order_id) AS order_count
    FROM gold_fact_sales
    GROUP BY customer_id
) order_counts
    ON c.customer_id = order_counts.customer_id
""")
print("gold_dim_customer:", spark.table("gold_dim_customer").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(f'{{"status":"SUCCESS","run_date":"{run_date}","layer":"gold"}}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
