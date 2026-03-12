
# Project 3 — HR Analytics with Microsoft Fabric (Dataflows Gen2)

## Overview
This project demonstrates a complete **HR / People Analytics data pipeline built in Microsoft Fabric** using a hybrid approach:

- **Low‑code ingestion and cleaning with Dataflows Gen2**
- **Code‑first dimensional modeling using Spark SQL in a Notebook**
- **Star schema semantic model for analytics**
- **Power BI report for HR insights**
- **Pipeline orchestration for automated refresh**

The goal of the project is to show that the engineer can **choose the right tool for the job**, combining visual ETL with SQL‑based modeling inside a Fabric Lakehouse.

---

# Architecture

```
Source CSV Files
        │
        ▼
Dataflows Gen2 (Power Query transformations)
        │
        ▼
Lakehouse Bronze Tables
        │
        ▼
Notebook (Spark SQL)
        │
        ▼
Gold Star Schema
        │
        ▼
Semantic Model
        │
        ▼
Power BI Report
        │
        ▼
Fabric Pipeline (Scheduled)
```

---

# Technology Stack

- Microsoft Fabric
- Dataflows Gen2 (Power Query)
- Lakehouse (Delta Tables)
- Spark SQL (Notebook)
- Power BI Semantic Model
- Fabric Data Pipelines
- GitHub (Version Control)

---

# Dataset

Four HR datasets are used:

| File | Rows | Description |
|-----|-----|-------------|
employees.csv | ~472 | Employee master data |
departments.csv | 10 | Department reference table |
performance_reviews.csv | ~1,179 | Employee review history |
salary_history.csv | ~2,204 | Salary change history |

---

# Phase 1 — Workspace Setup

1. Create a new Fabric workspace:
```
HR_Analytics
```

2. Create a **Lakehouse**
```
HR_LH
```

3. Upload the four CSV files to:

```
Files/
```

---

# Phase 2 — Dataflow Gen2 Ingestion

Create a Dataflow:

```
DF_HR_Ingest_and_Clean
```

### Transformations

Key Power Query steps applied:

- Promote headers
- Data type enforcement
- Replace errors with NULL
- Trim / Clean text columns
- Conditional column for salary corrections
- Remove invalid rows (future hire dates)
- Compute tenure
- Create employee full name column

### Data Quality Fixes

| Issue | Fix |
|-----|-----|
Negative salaries | Converted to positive |
Future hire dates | Filtered out |
Blank termination dates | Replaced with NULL |

### Dataflow Outputs

| Query | Lakehouse Table |
|------|----------------|
employees | bronze_employees |
departments | bronze_departments |
performance_reviews | bronze_performance_reviews |
salary_history | bronze_salary_history |

---

# Phase 3 — Gold Layer Modeling

A Fabric notebook builds the **star schema** using Spark SQL.

Notebook:

```
NB_Gold_HR_Model
```

## Dimension Tables

### gold_dim_department

Department reference dimension.

### gold_dim_employee

Employee dimension including:

- demographic attributes
- job information
- tenure bands
- department attributes

Example logic:

```
CASE
 WHEN tenure_years < 1 THEN 'New (<1 yr)'
 WHEN tenure_years < 3 THEN 'Developing (1‑3 yrs)'
 WHEN tenure_years < 5 THEN 'Experienced (3‑5 yrs)'
 ELSE 'Veteran (5+ yrs)'
END
```

---

## Fact Tables

### gold_fact_performance

Contains review events.

Metrics:

- overall_rating
- goals_met_pct
- performance_band

### gold_fact_salary_history

Contains salary changes over time.

---

## Aggregate Tables

### gold_agg_dept_summary

Department KPIs:

- active headcount
- salary distribution
- turnover percentage
- tenure averages

### gold_agg_performance_by_dept

Department performance metrics by review cycle.

---

# Phase 4 — Semantic Model

Semantic model:

```
HR_SemanticModel
```

Tables included:

- gold_dim_department
- gold_dim_employee
- gold_fact_performance
- gold_fact_salary_history
- gold_agg_dept_summary
- gold_agg_performance_by_dept

## Relationships

```
gold_fact_performance.employee_id → gold_dim_employee.employee_id

gold_fact_performance.department_id → gold_dim_department.department_id

gold_fact_salary_history.employee_id → gold_dim_employee.employee_id

gold_dim_employee.department_id → gold_dim_department.department_id
```

---

# Phase 5 — Power BI Report

## Page 1 — Workforce Overview

Visuals:

- Total Headcount KPI
- Active Employees KPI
- Turnover Rate KPI
- Average Salary KPI
- Headcount by Department (Budget vs Actual)
- Gender Distribution
- Division and Department slicers

---

## Page 2 — Performance Analytics

Visuals:

- Average rating trend by review cycle
- High vs Low performers by department
- Employee performance table
- Department and cycle slicers

---

## Page 3 — Compensation Analysis

Visuals:

- Salary vs Tenure scatter plot
- Average salary by department
- Salary history drill‑through
- Education level slicer

---

# Phase 6 — Pipeline Orchestration

Pipeline:

```
PL_HR_Analytics
```

Activities:

```
Dataflow Gen2
     ↓
Notebook
```

Schedule:

```
Daily at 06:00 AM
```

This demonstrates Fabric **orchestration of low‑code and code workloads**.

---

# Data Quality Issues (Intentional)

These issues were deliberately introduced to simulate real enterprise data problems.

| Issue | Impact |
|-----|-----|
Invalid department IDs | Causes mismatches during joins |
Negative salary values | Data entry errors |
Future hire dates | Invalid employee records |

---

# Engineering Decisions

### Why Dataflows for ingestion

The transformations are simple:

- type casting
- error handling
- conditional logic

Power Query handles these quickly without writing PySpark.

### Why Spark SQL for modeling

Dimensional modeling with:

- joins
- fact tables
- aggregations

is cleaner and more maintainable in SQL.

---

# Skills Demonstrated

- Fabric Lakehouse architecture
- Dataflows Gen2 transformations
- Spark SQL dimensional modeling
- Star schema design
- Semantic modeling
- BI dashboard design
- Pipeline orchestration
- Git integration

---

# Repository Structure

```
project-3-hr-analytics/
│
├── notebooks/
│   NB_Gold_HR_Model.ipynb
│
├── dataflows/
│   DF_HR_Ingest_and_Clean.json
│
├── pipeline/
│   PL_HR_Analytics.json
│
├── datasets/
│   employees.csv
│   departments.csv
│   performance_reviews.csv
│   salary_history.csv
│
└── README.md
```

---

# Estimated Build Time

| Phase | Time |
|-----|-----|
Workspace setup | 15 min |
Dataflow transformations | 45–60 min |
Notebook modeling | 20 min |
Semantic model + report | 1–2 hours |
Pipeline orchestration | 15 min |

Total build time:

```
~2–4 hours
```

---

# Portfolio Context

This project is part of a **Data Engineering Portfolio Series**.

| Project | Focus |
|-------|------|
Project 1 | Retail Lakehouse Analytics |
Project 2 | RetailOps Pipeline Orchestration |
Project 3 | HR Analytics (Dataflows Gen2) |
Project 4 | SmartFactory IoT Streaming |
Project 5 | Financial Data Platform |

---

# Key Takeaway

This project demonstrates the ability to:

- use **low‑code tools where appropriate**
- use **code when modeling complexity increases**
- orchestrate **end‑to‑end Fabric data pipelines**
