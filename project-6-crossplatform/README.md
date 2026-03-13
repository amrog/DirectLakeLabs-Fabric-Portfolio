# Project 6: CrossPlatform Integration — Mirroring + Shortcuts

**Workspace:** `CrossPlatform_Integration`
**Status:** Complete
**Fabric Workloads:** Mirroring, OneLake Shortcuts, Lakehouse, Notebooks, Power BI

---

## What This Project Does

Demonstrates Fabric's data virtualization capabilities by combining three completely different data sources into a single queryable lakehouse — without copying any of the underlying data. An Azure SQL Database mirrors into Fabric in near-real-time (simulating a CRM/ERP system), while OneLake Shortcuts reference Gold tables from the RetailOps and HR Analytics workspaces. A notebook then joins all three sources in a single Spark SQL query.

This is the only project in the portfolio that connects data across external systems and other Fabric workspaces, demonstrating the "single copy" principle that makes OneLake unique.

---

## Architecture

```
External                              Fabric
─────────                             ──────

┌─────────────────┐     Mirroring     ┌─────────────────────────────────────┐
│ Azure SQL DB    │ ═══(real-time)═══►│ CrossPlatform_Mirror                │
│ CrossPlatformDB │                   │  crm_customers                      │
│                 │                   │  erp_orders                          │
│ • crm_customers │                   │  erp_products                        │
│ • erp_orders    │                   │  erp_order_items                     │
│ • erp_products  │                   └──────────────┬──────────────────────┘
│ • erp_order_    │                                  │
│    items        │                                  │ OneLake Shortcut
└─────────────────┘                                  │
                                                     ▼
┌─────────────────┐                   ┌─────────────────────────────────────┐
│ RetailOps_      │  OneLake Shortcut │ Integration_LH (Unified Lakehouse)  │
│ Analytics       │ ────────────────► │                                     │
│ (Project 1)     │                   │  Mirrored:                          │
│                 │                   │   crm_customers ──► from Mirror     │
│ gold_fact_sales │                   │   erp_orders    ──► from Mirror     │
│ gold_dim_       │                   │   erp_products  ──► from Mirror     │
│  customer       │                   │   erp_order_items► from Mirror      │
│ gold_dim_       │                   │                                     │
│  product        │                   │  Shortcutted (RetailOps):           │
└─────────────────┘                   │   gold_fact_sales ► from Project 1  │
                                      │   gold_dim_customer► from Project 1 │
┌─────────────────┐                   │   gold_dim_product ► from Project 1 │
│ HR_Analytics    │  OneLake Shortcut │                                     │
│ (Project 3)     │ ────────────────► │  Shortcutted (HR):                  │
│                 │                   │   gold_dim_employee► from Project 3  │
│ gold_dim_       │                   │   gold_dim_dept   ► from Project 3  │
│  employee       │                   │   gold_agg_dept   ► from Project 3  │
│ gold_dim_       │                   │                                     │
│  department     │                   │  Unified Tables (computed locally):  │
│ gold_agg_dept_  │                   │   unified_customer_360              │
│  summary        │                   │   unified_revenue_by_dept           │
└─────────────────┘                   │   unified_efficiency_metrics        │
                                      └──────────────┬──────────────────────┘
                                                     │
                                                     ▼
                                      ┌─────────────────────────────────────┐
                                      │ Power BI Report                     │
                                      │  Cross-Platform Overview            │
                                      │  Data Integration Map               │
                                      └─────────────────────────────────────┘
```

---

## Data Sources

| Source | Type | Replication | Tables | Rows |
|--------|------|-------------|--------|------|
| Azure SQL Database (`CrossPlatformDB`) | Mirroring (near-real-time) | Continuous, automatic, ~seconds latency | 4 tables (CRM + ERP) | ~1,862 |
| RetailOps Lakehouse (Project 1) | OneLake Shortcut (zero-copy) | Instant — reads source directly | 3 Gold tables | Varies |
| HR Analytics Lakehouse (Project 3) | OneLake Shortcut (zero-copy) | Instant — reads source directly | 3 Gold tables | Varies |

### Mirrored Data (Azure SQL → Fabric)

| Table | Rows | Description |
|-------|------|-------------|
| `crm_customers` | 100 | CRM customer records with industry, region, lifetime value |
| `erp_orders` | 500 | Order headers spanning 2023–2025 |
| `erp_products` | 15 | Product catalog across 5 categories |
| `erp_order_items` | 1,247 | Order line items with quantity and pricing |

---

## Key Concepts Demonstrated

### Database Mirroring
Near-real-time, continuous replication from Azure SQL Database into Fabric OneLake. Data lands as Delta tables, queryable via SQL analytics endpoint and Spark. Changes in the source database appear in Fabric within seconds — no pipeline to build or maintain. Fabric compute for replication is free; only query compute is charged.

### OneLake Shortcuts
Zero-copy data references that make tables from other Fabric workspaces (or external storage like ADLS/S3) appear as local tables. No data movement, no staleness. When the source updates, the shortcut reflects it immediately. Supports internal OneLake sources, ADLS Gen2, Amazon S3, and S3-compatible storage.

### Unified Lakehouse Pattern
A single lakehouse that aggregates shortcutted and mirrored tables, enabling cross-domain queries. Local transformation notebooks create unified analytical tables by joining data from all sources.

---

## Cross-Domain Queries

The notebook (`NB_CrossPlatform_Analytics`) demonstrates queries that would be impossible without data virtualization:

**Customer 360 View** — Joins mirrored CRM customer data with mirrored ERP order history to create a complete customer profile with purchase behavior, without copying either dataset.

**Revenue by Department** — Combines shortcutted RetailOps sales data with shortcutted HR department headcount data to calculate revenue-per-employee by department.

**Workforce Efficiency Metrics** — Joins HR compensation data with mirrored ERP revenue data to analyze cost-to-revenue ratios across business divisions.

---

## Workspace Items

| Item | Type | Purpose |
|------|------|---------|
| `CrossPlatform_Mirror` | Mirrored Database | Near-real-time replication from Azure SQL Database |
| `Integration_LH` | Lakehouse | Unified lakehouse with shortcuts and local tables |
| `NB_CrossPlatform_Analytics` | Notebook | Cross-domain Spark SQL queries |
| `CrossPlatform_SemanticModel` | Semantic Model | Report data model |
| `CrossPlatform_Report` | Power BI Report | 2-page cross-platform dashboard |

---

## Key Engineering Decisions

**Why Mirroring instead of Copy Activity for CRM/ERP data:**
Copy Activity runs on a schedule and creates pipeline maintenance overhead. Mirroring is continuous, automatic, and zero-ETL. For operational data that drives customer-facing analytics, near-real-time replication is the correct pattern. It also means one fewer pipeline to monitor and debug.

**Why Shortcuts instead of duplicating Gold tables:**
The RetailOps and HR data already exists as clean, modeled Gold tables in their respective workspaces. Copying them would create redundant copies that could become stale. Shortcuts reference the source of truth directly — when Project 1's pipeline runs and updates `gold_fact_sales`, this project sees the update immediately without any additional action.

**Why a separate workspace:**
This project consumes data from other workspaces; it doesn't own any primary data sources. A separate workspace makes the access pattern clear, enables independent access control, and prevents this integration layer from cluttering the source workspaces.

**Why Azure SQL Database as the mirroring source:**
Azure SQL is the most common enterprise OLTP database in the Microsoft ecosystem. Most companies have operational databases (CRM, ERP, HRIS) in Azure SQL or SQL Server that need to flow into analytics. This demonstrates a real-world pattern that's directly transferable to production environments.

**Why the free tier:**
The Azure SQL Database free offer provides enough capacity for this portfolio project at zero cost. The Basic tier (~$5/month) is the fallback. Either way, the mirroring setup, verification, and cross-domain queries are identical regardless of tier.

---

## How to Build

### Prerequisites
- Azure subscription (free tier works)
- Fabric capacity with Mirroring enabled (tenant admin setting)
- Completed Project 1 (RetailOps) and Project 3 (HR Analytics) — their Gold tables are referenced by shortcuts

### Steps
1. Create Azure SQL Database → run `00_create_tables.sql` → run insert scripts `01` through `04`
2. Enable System Managed Identity on the SQL Server
3. Create `CrossPlatform_Integration` workspace in Fabric
4. Create Mirrored Azure SQL Database → connect to `CrossPlatformDB` → select all tables
5. Create `Integration_LH` lakehouse
6. Create OneLake Shortcuts to RetailOps Gold tables and HR Gold tables
7. Create shortcuts to the mirrored database tables
8. Run `NB_CrossPlatform_Analytics` notebook to build unified tables
9. Create semantic model and Power BI report

See `BUILD_GUIDE.md` for detailed step-by-step instructions.

---

## Folder Structure

```
project-6-crossplatform/
├── README.md                       ← You are here
├── BUILD_GUIDE.md                  # Detailed build instructions
├── datasets/
│   ├── generate_crossplatform_data.py  # Python generator for SQL inserts
│   ├── 00_create_tables.sql        # DDL for Azure SQL Database
│   ├── 01_insert_customers.sql     # 100 CRM customer records
│   ├── 02_insert_products.sql      # 15 product catalog entries
│   ├── 03_insert_orders.sql        # 500 order headers
│   └── 04_insert_order_items.sql   # 1,247 order line items
├── screenshots/
│   ├── mirroring_status.png
│   ├── shortcuts_in_lakehouse.png
│   ├── cross_domain_query.png
│   ├── customer_360_results.png
│   └── pbi_crossplatform_report.png
└── fabric-artifacts/               # Git-synced from CrossPlatform_Integration workspace
```

---

## What Makes This Project Different

Every other project in this portfolio creates and manages its own data. This project creates nothing — it references everything. That's the point. In production, the most valuable data engineering skill isn't always building new pipelines; it's connecting existing systems without creating copies, managing staleness, or building maintenance overhead.

The hiring manager takeaway: "I know when to build a pipeline, and I know when not to."

---

*This is Project 6 in the [Microsoft Fabric Data Engineering Portfolio](../README.md).*
*Project 5: [FinanceOps — Data Warehouse + dbt](../project-5-financeops/README.md)*
