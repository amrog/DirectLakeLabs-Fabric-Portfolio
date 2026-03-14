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
│ (Project 1)     │                   │  Mirrored + Shortcutted + Unified   │
│                 │                   │                                     │
│ gold_fact_sales │                   │  ┌─────────────────────────────┐    │
│ gold_dim_       │                   │  │ unified_customer_360        │    │
│  customer       │                   │  │ unified_revenue_by_dept     │    │
│ gold_dim_       │                   │  │ unified_efficiency_metrics  │    │
│  product        │                   │  └─────────────────────────────┘    │
└─────────────────┘                   └──────────────┬──────────────────────┘
                                                     │
┌─────────────────┐                                  ▼
│ HR_Analytics    │  OneLake Shortcut  ┌─────────────────────────────────────┐
│ (Project 3)     │ ────────────────► │ Power BI Report (2 pages)           │
│                 │                   │  Cross-Platform Overview             │
│ gold_dim_       │                   │  Data Integration Map                │
│  employee       │                   └─────────────────────────────────────┘
│ gold_dim_       │
│  department     │
│ gold_agg_dept_  │
│  summary        │
└─────────────────┘
```

---

## Building It: The Story

### Step 1: Provision the External Source

The project starts outside of Fabric entirely. An Azure SQL Database (`CrossPlatformDB`) was created on server `flowanalytics` in the `rg-fabric-portfolio` resource group to simulate a CRM/ERP system that exists outside the analytics platform.

![Azure SQL Database deployment complete](screenshots/azure_sql_creation.png)

Four tables were created and loaded: `crm_customers` (100 records), `erp_orders` (500), `erp_products` (15), and `erp_order_items` (1,247). These represent the kind of operational data that typically lives in a transactional database and needs to flow into analytics without building traditional ETL.

---

### Step 2: Create the Operational Schema

To simulate a production CRM/ERP environment, the operational schema was created directly inside the Azure SQL Database using the Azure Portal Query Editor. This approach mirrors how engineering teams often provision lightweight development or staging schemas without requiring external tooling.

![Azure SQL Query Editor showing CRM table schema creation](screenshots/azure_sql_table_creation.png)

The schema includes four tables that represent a simplified operational system:

- `crm_customers` — Customer master records (CRM system)
- `erp_orders` — Order headers
- `erp_order_items` — Line items associated with each order
- `erp_products` — Product catalog

These tables simulate the type of transactional data typically produced by operational applications.

---

### Step 3: Verify the Operational Tables

After creating the schema and loading sample data, the Azure SQL Query Editor confirms that all operational tables exist and are accessible.

![Azure SQL database tables visible in query editor](screenshots/azure_sql_table_complete.png)

This step mirrors a common production validation practice: verifying that source system tables exist and contain data before configuring downstream replication or integration services.

---

### Step 4: Mirror the Database into Fabric

A Mirrored Azure SQL Database item (`CrossPlatform_Mirror`) was created in the Fabric workspace, connecting to the Azure SQL source. Fabric discovers the tables automatically and begins continuous replication into OneLake as Delta tables.

![Mirrored database verification — 4 tables replicated, query confirms data](screenshots/mirror_verification.png)

The verification query confirms all 4 source tables are visible in the mirrored database's SQL analytics endpoint. The data replicates in near-real-time — when a new row is inserted in Azure SQL, it appears in Fabric within seconds. No pipeline, no schedule, no code.

---

### Step 5: Create OneLake Shortcuts

This is where data virtualization comes together. Inside `Integration_LH`, OneLake Shortcuts were created to reference Gold tables from two other Fabric workspaces — without copying any data.

![Right-click on dbo schema to create table shortcuts](screenshots/shortcut_creation_steps.png)

The shortcut creation process: right-click on the `dbo` schema (not the Tables folder) → select **New table shortcut** → navigate to the source workspace and select the target table.

![RetailOps Gold tables appearing as shortcuts in Integration_LH](screenshots/established_shortcuts.png)

After creating shortcuts, the `Integration_LH` lakehouse shows RetailOps Gold tables (`gold_dim_customer`, `gold_dim_product`, `gold_fact_sales`) as if they were local — but the data physically lives in the `RetailOps_Analytics` workspace. Any updates to those tables are reflected immediately.

---

### Step 6: The OneLake Catalog View

The OneLake catalog provides a unified view of all data assets across the tenant. Here you can see the integration pattern clearly — `Integration_LH` and `CrossPlatform_Mirror` are the two primary items in the `CrossPlatform_Integration` workspace, alongside data from other workspaces (HR_LH, FinanceOps_DW, RetailOps_LH) that are referenced through shortcuts.

![OneLake catalog showing integrated source attachments and mirrored operational database](screenshots/data_connection_view.png)

---

### Step 7: Build Unified Analytical Tables

A notebook (`NB_CrossPlatform_Analytics`) joins data across all three sources using Spark SQL — mirrored CRM/ERP data, shortcutted RetailOps sales data, and shortcutted HR workforce data — to produce three unified analytical tables.

![Semantic model showing the three unified tables with their columns](screenshots/semantic_model_unified_tables.png)

The semantic model exposes these three tables:
- **unified_customer_360** — Mirrored CRM customer profiles enriched with ERP order history (orders, spend, average order value, last order date)
- **unified_efficiency_metrics** — HR headcount and compensation data crossed with mirrored ERP revenue by division
- **unified_revenue_by_dept** — RetailOps sales combined with HR department headcount to calculate revenue-per-employee

---

### Step 8: Cross-Platform Power BI Report

**Page 1: Cross-Platform Overview**

![Report page 1 — KPI cards, revenue by department, and Customer 360 table](screenshots/report_page_1.png)

The overview page shows data flowing from all three sources into a single dashboard: Total Customers and Total Orders from the mirrored CRM/ERP, Retail Revenue from the shortcutted RetailOps workspace, and Total Headcount from the shortcutted HR workspace. The Customer 360 table combines mirrored customer profiles with their order history — including the "Test Mirroring" record that was inserted to verify real-time sync.

**Page 2: Data Integration Map**

![Report page 2 — Architecture diagram showing data flow from sources through unified tables to report](screenshots/report_page_2.png)

This page serves as a portfolio showcase. It visually maps each data source (CRM Customers, ERP Orders, HR Workforce, Retail Sales) to its integration type (Mirrored or Shortcut), the unified table it feeds, and the key metric it contributes. The architecture flows from source → Integration_LH → Unified Tables → Power BI Report, making the data virtualization pattern immediately clear to anyone viewing the report.

---

### The Complete Workspace

![CrossPlatform_Integration workspace with all items](screenshots/workspace_view.png)

The final workspace contains: a Mirrored Database with its SQL analytics endpoint, the Integration Lakehouse with shortcuts and unified tables, the analytics notebook, a semantic model, and the 2-page Power BI report. Seven items total — lean and focused.

---

## Data Sources

| Source | Type | Replication | Tables | Rows |
|--------|------|-------------|--------|------|
| Azure SQL Database (`CrossPlatformDB`) | Mirroring (near-real-time) | Continuous, automatic, ~seconds latency | 4 tables (CRM + ERP) | ~1,862 |
| RetailOps Lakehouse (Project 1) | OneLake Shortcut (zero-copy) | Instant — reads source directly | 3 Gold tables | Varies |
| HR Analytics Lakehouse (Project 3) | OneLake Shortcut (zero-copy) | Instant — reads source directly | 3 Gold tables | Varies |

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

---

## Workspace Items

| Item | Type | Purpose |
|------|------|---------|
| `CrossPlatform_Mirror` | Mirrored Database | Near-real-time replication from Azure SQL Database |
| `Integration_LH` | Lakehouse | Unified lakehouse with shortcuts and local unified tables |
| `NB_CrossPlatform_Analytics` | Notebook | Cross-domain Spark SQL queries joining all sources |
| `CrossPlatform_SemanticModel` | Semantic Model | Three unified analytical tables |
| `CrossPlatform_Report` | Power BI Report | 2-page cross-platform dashboard |

---

## What Makes This Project Different

Every other project in this portfolio creates and manages its own data. This project creates nothing — it references everything. That's the point. In production, the most valuable data engineering skill isn't always building new pipelines; it's connecting existing systems without creating copies, managing staleness, or building maintenance overhead.

The hiring manager takeaway: **"I know when to build a pipeline, and I know when not to."**

---

## Folder Structure

```
project-6-crossplatform/
├── README.md                       ← You are here
├── BUILD_GUIDE.md                  # Detailed build instructions
├── datasets/
│   ├── generate_crossplatform_data.py
│   ├── 00_create_tables.sql
│   ├── 01_insert_customers.sql
│   ├── 02_insert_products.sql
│   ├── 03_insert_orders.sql
│   └── 04_insert_order_items.sql
├── screenshots/
│   ├── azure_sql_creation.png
│   ├── mirror_verification.png
│   ├── shortcut_creation_steps.png
│   ├── established_shortcuts.png
│   ├── data_connection_view.png
│   ├── semantic_model_unified_tables.png
│   ├── report_page_1.png
│   ├── report_page_2.png
│   └── workspace_view.png
└── fabric-artifacts/               # Git-synced from CrossPlatform_Integration workspace
```

---

*This is Project 6 in the [Microsoft Fabric Data Engineering Portfolio](../README.md).*
*Project 5: [FinanceOps — Data Warehouse + dbt](../project-5-financeops/README.md)*
