-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "48344a44-8caa-4366-bc3f-9419899cbb42",
-- META       "default_lakehouse_name": "Integration_LH",
-- META       "default_lakehouse_workspace_id": "5f3856d9-6ef8-4920-8750-dbc806add301",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "48344a44-8caa-4366-bc3f-9419899cbb42"
-- META         }
-- META       ]
-- META     },
-- META     "mirrored_db": {
-- META       "known_mirrored_dbs": [
-- META         {
-- META           "id": "3a4c1c69-5ea8-45f5-842b-7f527ed12ea3"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- #### Verify all data sources are accessible

-- CELL ********************


-- Count rows from each source to confirm connectivity

SELECT 'RetailOps - Sales Facts' AS source, COUNT(*) AS row_count FROM gold_fact_sales
UNION ALL
SELECT 'RetailOps - Customers', COUNT(*) FROM gold_dim_customer
UNION ALL
SELECT 'HR - Employees', COUNT(*) FROM gold_dim_employee
UNION ALL
SELECT 'HR - Departments', COUNT(*) FROM gold_dim_department
UNION ALL
SELECT 'CRM - Customers (Mirrored)', COUNT(*) FROM crm_customers
UNION ALL
SELECT 'ERP - Orders (Mirrored)', COUNT(*) FROM erp_orders;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### Cross-domain — Revenue by department (Retail + HR)

-- CELL ********************


-- Join retail sales data with HR department data
-- This query combines two completely separate projects
CREATE OR REPLACE TABLE unified_revenue_by_dept AS
SELECT
    d.department_name,
    d.division,
    ds.active_headcount,
    COUNT(DISTINCT fs.order_id) AS total_orders,
    ROUND(SUM(fs.net_sales), 2) AS total_revenue,
    ROUND(SUM(fs.net_sales) / ds.active_headcount, 2) AS revenue_per_employee
FROM gold_fact_sales fs
CROSS JOIN gold_agg_dept_summary ds
INNER JOIN gold_dim_department d ON ds.department_id = d.department_id
WHERE d.division = 'Revenue'
GROUP BY d.department_name, d.division, ds.active_headcount
ORDER BY total_revenue DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### Mirrored CRM enrichment — Customer 360 view

-- CELL ********************


-- Combine mirrored CRM data with retail purchase history
CREATE OR REPLACE TABLE unified_customer_360 AS
WITH order_summary AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(order_total), 2) AS total_spend,
        ROUND(AVG(order_total), 2) AS avg_order_value,
        MAX(order_date) AS last_order_date
    FROM erp_orders
    WHERE order_status = 'Completed'
    GROUP BY customer_id
)
SELECT
    c.customer_id AS crm_customer_id,
    c.first_name,
    c.last_name,
    c.company_name,
    c.industry,
    c.region,
    c.lifetime_value AS crm_lifetime_value,
    os.total_orders,
    os.total_spend,
    os.avg_order_value,
    os.last_order_date
FROM crm_customers c
LEFT JOIN order_summary os
    ON c.customer_id = os.customer_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### Workforce-to-revenue efficiency analysis

-- CELL ********************


-- How much revenue does each division generate per employee?
CREATE OR REPLACE TABLE unified_efficiency_metrics AS
SELECT
    d.division,
    SUM(ds.active_headcount) AS total_headcount,
    SUM(ds.avg_salary * ds.active_headcount) AS total_compensation,
    COUNT(DISTINCT eo.order_id) AS mirrored_order_count,
    ROUND(SUM(eo.order_total), 2) AS mirrored_revenue
FROM gold_dim_department d
INNER JOIN gold_agg_dept_summary ds ON d.department_id = ds.department_id
CROSS JOIN erp_orders eo
--WHERE eo.order_status = 'Completed'
GROUP BY d.division;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### 

-- MARKDOWN ********************

-- #### 
