-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "aefedc79-f516-4dd5-802f-6908eab8505b",
-- META       "default_lakehouse_name": "HR_LH",
-- META       "default_lakehouse_workspace_id": "43d31b4f-3433-413a-b1c7-24d6452587f4",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "aefedc79-f516-4dd5-802f-6908eab8505b"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "80d95df3-8245-4d1f-89ce-0d827a8dd8df",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "80d95df3-8245-4d1f-89ce-0d827a8dd8df",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!


-- dim_department
CREATE OR REPLACE TABLE gold_dim_department AS
SELECT
    department_id,
    department_name,
    division,
    location,
    cost_center,
    head_count_budget
FROM bronze_departments;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************



-- dim_employee (slowly changing — current state only for this project)
CREATE OR REPLACE TABLE gold_dim_employee AS
SELECT
    e.employee_id,
    e.full_name,
    e.first_name,
    e.last_name,
    e.email,
    e.job_title,
    e.department_id,
    d.department_name,
    d.division,
    e.hire_date,
    e.termination_date,
    e.termination_reason,
    e.employment_type,
    e.salary,
    e.gender,
    e.ethnicity,
    e.education_level,
    e.manager_id,
    e.is_active,
    e.location,
    e.tenure_years,
    CASE
        WHEN e.tenure_years < 1 THEN 'New (<1 yr)'
        WHEN e.tenure_years < 3 THEN 'Developing (1-3 yrs)'
        WHEN e.tenure_years < 5 THEN 'Experienced (3-5 yrs)'
        ELSE 'Veteran (5+ yrs)'
    END AS tenure_band
FROM bronze_employees e
LEFT JOIN bronze_departments d ON e.department_id = d.department_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************



-- fact_performance
CREATE OR REPLACE TABLE gold_fact_performance AS
SELECT
    pr.review_id,
    pr.employee_id,
    pr.review_cycle,
    pr.overall_rating,
    pr.goals_met_pct,
    pr.performance_band,
    pr.reviewer_id,
    pr.review_date,
    e.department_id,
    e.job_title,
    e.tenure_years
FROM bronze_performance_reviews pr
INNER JOIN bronze_employees e ON pr.employee_id = e.employee_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************



-- fact_salary_history
CREATE OR REPLACE TABLE gold_fact_salary_history AS
SELECT
    sh.salary_history_id,
    sh.employee_id,
    sh.effective_date,
    sh.salary_amount,
    sh.change_type,
    sh.change_pct,
    e.department_id,
    e.job_title
FROM bronze_salary_history sh
INNER JOIN bronze_employees e ON sh.employee_id = e.employee_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************



-- Department-level headcount and compensation summary
CREATE OR REPLACE TABLE gold_agg_dept_summary AS
SELECT
    d.department_id,
    d.department_name,
    d.division,
    d.head_count_budget,
    COUNT(CASE WHEN e.is_active = true THEN 1 END) AS active_headcount,
    d.head_count_budget - COUNT(CASE WHEN e.is_active = true THEN 1 END) AS headcount_variance,
    ROUND(AVG(CASE WHEN e.is_active = true THEN e.salary END), 0) AS avg_salary,
    ROUND(MIN(CASE WHEN e.is_active = true THEN e.salary END), 0) AS min_salary,
    ROUND(MAX(CASE WHEN e.is_active = true THEN e.salary END), 0) AS max_salary,
    ROUND(AVG(CASE WHEN e.is_active = true THEN e.tenure_years END), 1) AS avg_tenure_years,
    COUNT(CASE WHEN e.is_active = false THEN 1 END) AS terminated_count,
    ROUND(COUNT(CASE WHEN e.is_active = false THEN 1 END) * 100.0 / COUNT(*), 1) AS turnover_pct
FROM bronze_departments d
LEFT JOIN bronze_employees e ON d.department_id = e.department_id
GROUP BY d.department_id, d.department_name, d.division, d.head_count_budget;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************



-- Performance distribution by department
CREATE OR REPLACE TABLE gold_agg_performance_by_dept AS
SELECT
    e.department_id,
    d.department_name,
    pr.review_cycle,
    COUNT(*) AS review_count,
    ROUND(AVG(pr.overall_rating), 2) AS avg_rating,
    ROUND(AVG(pr.goals_met_pct), 1) AS avg_goals_met_pct,
    SUM(CASE WHEN pr.overall_rating >= 4 THEN 1 ELSE 0 END) AS high_performers,
    SUM(CASE WHEN pr.overall_rating <= 2 THEN 1 ELSE 0 END) AS low_performers
FROM bronze_performance_reviews pr
INNER JOIN bronze_employees e ON pr.employee_id = e.employee_id
INNER JOIN bronze_departments d ON e.department_id = d.department_id
GROUP BY e.department_id, d.department_name, pr.review_cycle;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- Build Aggregate Tables

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
