# Microsoft Fabric Data Engineering Portfolio

**Aaron Rogers** — Data Engineer | Orange County, CA
[LinkedIn](https://linkedin.com/in/yourprofile) · [DirectLakeLab](https://directlakelab.com)

---

## What This Is

This is a hands-on portfolio of six data engineering projects built entirely in Microsoft Fabric. Each project targets a different Fabric workload and solves a different class of data problem — batch ETL, pipeline orchestration, low-code ingestion, real-time streaming, SQL-first transformations, and cross-platform data integration.

The projects are designed to demonstrate breadth across the Fabric platform and depth in the areas that matter most in production environments: data modeling, orchestration, error handling, real-time analytics, and architectural decision-making.

Every project was built from scratch — no templates, no quickstarts, no sample solutions. The code, the pipelines, the dashboards, and the mistakes along the way are all mine.

---

## Projects

### Project 1: RetailOps — Medallion Architecture
**Workspace:** `RetailOps_Analytics`
**Status:** Complete

An end-to-end batch analytics pipeline for a retail dataset. Raw CSV files flow through a Bronze → Silver → Gold medallion architecture using PySpark notebooks, then surface in a semantic model and Power BI report.

**What it covers:**
Lakehouse, PySpark notebooks, Spark SQL, Delta Lake, data quality (deduplication, type casting, string standardization), star schema modeling (dim_date, dim_customer, dim_product, fact_sales), semantic model with DAX measures, and a Power BI report.

**Key engineering decisions:**
Window-based deduplication in Silver using ROW_NUMBER() partitioned by business key. Gold layer built entirely in Spark SQL with CREATE OR REPLACE TABLE for idempotency. Metadata stamping (_ingested_at, _source_file, _batch_id) on every record at ingestion.

📁 [project-1-retailops/](project-1-retailops/)

---

### Project 2: RetailOps — Pipeline Orchestration
**Workspace:** `RetailOps_Analytics`
**Status:** Complete

Evolved the manual notebook workflow from Project 1 into a production-grade Data Factory pipeline. Notebooks are parameterized, chained with success/failure branching, and wrapped in audit logging that writes to a Delta table.

**What it covers:**
Data Factory pipelines, notebook parameterization (run_date, source_folder, run_mode), pipeline variables, Set Variable activity, sequential activity dependencies (On Success / On Failure), retry logic with layer-appropriate strategies, mssparkutils.notebook.exit() for inter-activity communication, and a reusable audit logging notebook writing to a pipeline_audit_log Delta table.

**Key engineering decisions:**
Two retries with 30-second intervals for Bronze (external data, transient failures likely) versus one retry with 60-second interval for Gold (failures are code bugs, not transient). Static error messages in failure paths because Fabric's expression builder doesn't support optional chaining. Audit table lives alongside the data in the Lakehouse so operations history is queryable with Spark SQL, not locked in the Monitor hub.

📁 [project-1-retailops/](project-1-retailops/)

---

### Project 3: HR Analytics — Dataflows Gen2
**Workspace:** `HR_Analytics`
**Status:** Complete

A people analytics project built with Dataflows Gen2 instead of notebooks to demonstrate low-code ETL capabilities. Employee data, performance reviews, and department hierarchies are ingested and transformed through the visual Power Query editor, then modeled in a Gold layer for reporting.

**What it will cover:**
Dataflows Gen2, Power Query transformations (merges, pivots, conditional columns, data type enforcement), Lakehouse destination configuration, and the differences between code-first and low-code ingestion approaches in Fabric.

**Why it matters:**
Not every transformation needs PySpark. This project shows the judgment to pick the right tool — visual ETL for straightforward business data, code for complex logic. Many Fabric teams use both, and hiring managers want to see that versatility.

---

### Project 4: SmartFactory — Real-Time IoT Monitoring
**Workspace:** `SmartFactory_IoT_Monitoring`
**Status:** Complete

A real-time streaming analytics solution that simulates IoT sensor data from manufacturing equipment. Ten sensors report temperature, pressure, vibration, and humidity every two seconds. Events flow through an Eventstream into an Eventhouse (KQL database) and are visualized on a live Real-Time Dashboard with anomaly detection.

**What it covers:**
Eventstreams with custom endpoint sources, the azure-eventhub Python SDK for event production, Eventhouse and KQL databases, Kusto Query Language (KQL) for time-series analytics (rolling averages, threshold detection, time-bucketed aggregations, zone-level summaries), Real-Time Dashboards with auto-refresh, and dual-destination routing (Eventhouse for real-time queries, Lakehouse for historical batch analysis).

**Key engineering decisions:**
Separate workspace from RetailOps because batch and streaming workloads have different capacity profiles, access control needs, and monitoring patterns. Five percent anomaly rate in the simulator produces enough alert data to validate threshold queries without drowning the dashboard. KQL chosen over Spark SQL for the streaming layer because it's purpose-built for sub-second time-series queries. Dual routing to both Eventhouse and Lakehouse demonstrates the Lambda architecture pattern.

📁 [project-4-smartfactory/](project-4-smartfactory/)

---

### Project 5: FinanceOps — Data Warehouse + dbt
**Workspace:** `FinanceOps_Warehouse`
**Status:** Complete

A financial analytics project built on a Fabric Data Warehouse instead of a Lakehouse, using dbt jobs for SQL-first transformations. This project demonstrates the ELT pattern and the architectural judgment of knowing when a warehouse is the better fit.

**What it will cover:**
Fabric Data Warehouse (full T-SQL DML, not the read-only SQL analytics endpoint), dbt jobs for model definition and testing, Copy Activity or Mirroring for ingestion, star schema built entirely in T-SQL, and a semantic model sourced from the warehouse.

**Why it matters:**
Lakehouses and warehouses solve different problems. This project shows the ability to articulate when structured, SQL-native workloads belong in a warehouse — and how dbt brings software engineering practices (version control, testing, documentation) to SQL transformations.

---

### Project 6: CrossPlatform — Mirroring + Shortcuts
**Workspace:** `CrossPlatform_Integration`
**Status:** Planned

A data integration project focused on connecting external sources to Fabric without traditional ETL. Mirroring replicates data from Azure SQL in near real-time, OneLake Shortcuts virtualize data from external storage, and both are combined into a unified Gold layer.

**What it will cover:**
Database Mirroring (Azure SQL → Fabric), OneLake Shortcuts (ADLS, S3, Dataverse), zero-copy data access patterns, and combining mirrored and shortcut data with Lakehouse data for unified analytics.

**Why it matters:**
Production environments rarely have all their data in one place. This project demonstrates Fabric's data virtualization capabilities — accessing data where it lives without moving it, reducing cost and latency while maintaining a single analytics layer.

---

## Technology Map

| Fabric Workload | Projects |
|---|---|
| Data Engineering (Lakehouse, Notebooks, Spark) | 1, 2 |
| Data Factory (Pipelines, Copy Activity) | 2, 5 |
| Dataflows Gen2 (Power Query) | 3 |
| Real-Time Intelligence (Eventstreams, Eventhouse, KQL) | 4 |
| Data Warehouse (T-SQL, dbt) | 5 |
| OneLake (Shortcuts, Mirroring) | 6 |
| Power BI (Semantic Models, Reports) | 1, 3, 5 |
| Real-Time Dashboards (KQL Visuals) | 4 |

---

## Repository Structure

```
DirectLakeLabs-Fabric-Portfolio/
├── README.md                              ← You are here
├── project-1-retailops/
│   ├── README.md                          ← RetailOps deep-dive (Projects 1 & 2)
│   ├── screenshots/
│   └── fabric-artifacts/                  ← Git-synced from RetailOps_Analytics workspace
│
├── project-4-smartfactory/
│   ├── README.md                          ← SmartFactory deep-dive (Project 4)
│   ├── screenshots/
│   └── fabric-artifacts/                  ← Git-synced from SmartFactory_IoT_Monitoring workspace
│
├── project-3-hr-analytics/               ← Complete
├── project-5-financeops/                  ← Complete
└── project-6-crossplatform/              ← Coming soon
```

---

## About Me

I'm a data analyst transitioning into data engineering. My background includes SQL Server, Power BI, Salesforce CRMA, and SAP HANA — plus a few years as a personal trainer, which taught me more about understanding what people actually need than any technical certification ever could.

I have a Master's in Information Systems, an SAP HANA certification, and a Microsoft Certified Professional designation. I'm currently preparing for the DP-600 (Fabric Analytics Engineer) and DP-700 (Fabric Data Engineer) certifications.

I built [DirectLakeLab](https://directlakelab.com) as a practice tool for Fabric certifications. This portfolio is the result of using it — and then going far beyond it.

The long-term goal is consulting. I want to help small and mid-size companies build data platforms that actually get used, not just data warehouses that collect dust.

---

## Certifications

- Microsoft Certified Professional (MCP)
- SAP Certified Technology Associate — SAP HANA 2.0
- DP-600: Fabric Analytics Engineer *(in progress)*
- DP-700: Fabric Data Engineer *(in progress)*
