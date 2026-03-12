# Project 4: SmartFactory — Real-Time IoT Monitoring

**Workspace:** `SmartFactory_IoT_Monitoring`
**Status:** Complete
**Portfolio:** [← Back to main README](../README.md)

---

## What This Project Does

This is a real-time streaming analytics solution for a simulated smart factory. Ten IoT sensors mounted on manufacturing equipment report temperature, pressure, vibration, and humidity readings every two seconds. The data flows through a Fabric Eventstream into an Eventhouse (KQL database), where KQL queries power anomaly detection, rolling averages, and zone-level monitoring. A Real-Time Dashboard renders live metrics with auto-refresh, and a secondary route sends the same events to a Lakehouse for historical batch analysis.

The goal was to build something that looks and behaves like a production IoT monitoring system — not just prove that Eventstreams work.

---

## Architecture

```
┌─────────────────────┐
│  IoT Sensor          │    Python script using azure-eventhub SDK
│  Simulator           │    10 devices × 4 metrics × every 2 seconds
│  (Fabric Notebook)   │    ~5% anomaly injection rate
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  IoT_Eventstream     │    Custom endpoint source
│                      │    Receives JSON events via AMQP
└─────────┬───────────┘
          │
     ┌────┴─────┐
     │          │
     ▼          ▼
┌─────────┐  ┌──────────────┐
│Eventhouse│  │SmartFactory  │
│ KQL DB   │  │   _LH        │
│          │  │ (Lakehouse)  │
│ Real-time│  │ Historical   │
│ queries  │  │ batch layer  │
└────┬─────┘  └──────────────┘
     │
     ▼
┌─────────────────────┐
│  Real-Time Dashboard │    Auto-refresh tiles
│                      │    KQL-powered visuals
└─────────────────────┘
```

This is a Lambda architecture pattern — the same event stream is routed to two destinations simultaneously. The Eventhouse handles sub-second queries for live monitoring, and the Lakehouse stores the same data as Delta tables for historical trend analysis and batch reporting.

---

## Workspace Contents

```
SmartFactory_IoT_Monitoring/
├── IoT_Sensor_Simulator          (Notebook — event producer)
├── IoT_Eventstream               (Eventstream — ingestion + routing)
├── SmartFactory_Eventhouse        (Eventhouse — KQL engine)
│   └── SmartFactory_KQLDB         (KQL Database)
│       └── sensorreadings         (KQL table — streaming destination)
├── SmartFactory_LH                (Lakehouse — batch destination)
│   └── Tables/
│       └── iot_sensor_raw         (Delta table — historical data)
└── SmartFactory_Dashboard         (Real-Time Dashboard — live visuals)
```

---

## How It Works

### Phase 1: Sensor Simulation

A Fabric notebook generates realistic IoT telemetry using the `azure-eventhub` Python SDK. Each event is a JSON payload representing one sensor reading:

```json
{
  "device_id": "sensor-003",
  "zone": "Assembly",
  "temperature_celsius": 72.4,
  "pressure_psi": 38.1,
  "vibration_mms": 3.2,
  "humidity_percent": 55.8,
  "event_time": "2026-03-03T02:45:00.000Z"
}
```

The simulator runs 10 devices across 4 factory zones (Welding, Assembly, Painting, Packaging). About 5% of readings are injected as anomalies — temperature spikes above 100°C, pressure exceeding 42 PSI, vibration over 5.5 mm/s, or humidity above 75%. These provide enough alert data to validate threshold queries without making the dashboard unusable.

Events are sent to the Eventstream's custom endpoint using AMQP, which is the same protocol Azure IoT Hub and Event Hubs use in production.

### Phase 2: Event Ingestion and Routing

The Eventstream (`IoT_Eventstream`) receives events from the custom endpoint and routes them to two destinations simultaneously:

**Destination 1: Eventhouse** — Events land in the `sensorreadings` table in the KQL database. This is optimized for sub-second time-series queries. Data is available for KQL queries within seconds of production.

**Destination 2: Lakehouse** — The same events are routed to `SmartFactory_LH` as a Delta table (`iot_sensor_raw`). This provides a durable historical store for batch analysis, trend detection over weeks/months, and potential future joins with data from other projects.

### Phase 3: KQL Analytics

KQL (Kusto Query Language) powers all the analytics. Here are the key query patterns used in this project:

**Sensor overview with rolling averages:**

```kql
sensorreadings
| where ingestion_time() > ago(24h)
| summarize 
    avg_temp = round(avg(temperature_celsius), 2),
    avg_pressure = round(avg(pressure_psi), 2),
    avg_vibration = round(avg(vibration_mms), 2),
    avg_humidity = round(avg(humidity_percent), 2)
  by device_id
| order by device_id asc
```

**Anomaly detection with threshold filtering:**

```kql
sensorreadings
| where temperature_celsius > 100 
     or pressure_psi > 42 
     or vibration_mms > 5.5 
     or humidity_percent > 75
| project 
    ["Time"] = ingestion_time(),
    Device = device_id,
    Zone = zone,
    ["Temp"] = temperature_celsius,
    ["Pressure"] = pressure_psi,
    ["Vibration"] = vibration_mms
| order by ["Time"] desc
| take 20
```

**Time-bucketed aggregation by zone:**

```kql
sensorreadings
| where ingestion_time() > ago(1h)
| summarize 
    avg_temp = round(avg(temperature_celsius), 1),
    max_temp = round(max(temperature_celsius), 1),
    alert_count = countif(temperature_celsius > 100 
                       or pressure_psi > 42 
                       or vibration_mms > 5.5)
  by zone, bin(ingestion_time(), 5m)
| order by ingestion_time() desc
```

Note: These queries use `ingestion_time()` instead of `event_time` because the initial schema evolution from string to datetime left existing `event_time` values null. New data ingested after the schema change populates `event_time` correctly. This is documented intentionally — it's a real data engineering scenario where you adapt your queries to handle schema evolution without reprocessing historical data.

### Phase 4: Real-Time Dashboard

The dashboard includes four tiles, each powered by a KQL query with auto-refresh:

**Sensor Status Table** — Current readings from all 10 sensors with their latest metric values. Highlights devices with readings outside normal thresholds.

**Temperature Trend** — Line chart showing temperature across sensors over the last hour, bucketed in 5-minute intervals. Makes it easy to spot gradual drift versus sudden spikes.

**Anomalies by Zone** — Bar chart summarizing how many threshold violations occurred in each factory zone. Useful for identifying which zones need attention.

**Critical Alerts Table** — The 20 most recent readings that exceeded any threshold, ordered by time descending. This is the "action items" view for the operations team.

---

## Key Engineering Decisions

**Separate workspace from RetailOps.** Batch and streaming workloads have fundamentally different capacity profiles. RetailOps runs Spark jobs that spike CPU and memory for minutes at a time, then go idle. Streaming workloads maintain a steady baseline. Mixing them in one workspace would make capacity planning harder and create noisy neighbor problems. In production, you'd also want different access control — operations teams monitoring the factory floor shouldn't need access to retail sales data.

**Custom endpoint over Event Hub.** The Eventstream custom endpoint was chosen because it doesn't require provisioning a separate Azure Event Hub resource. For a portfolio project, this keeps everything inside Fabric. In a production environment with real IoT devices, you'd likely use Azure IoT Hub or Event Hubs as the source, which Eventstreams also support natively.

**5% anomaly injection rate.** This was calibrated through testing. At 1%, the anomaly queries returned too few results to be interesting on the dashboard. At 10%, the alerts table was so noisy it was hard to distinguish real patterns. 5% produces enough data to validate threshold detection while keeping the dashboard meaningful.

**KQL over Spark SQL for the streaming layer.** Spark SQL can query Delta tables, but it's designed for batch workloads — you'd need to schedule refresh cycles. KQL is purpose-built for sub-second queries against streaming data. The trade-off is that KQL has a different syntax and learning curve, but for time-series analytics it's dramatically more efficient.

**Dual routing (Lambda pattern).** Routing to both Eventhouse and Lakehouse simultaneously means you don't have to choose between real-time and historical analysis. The Eventhouse handles "what's happening right now" queries. The Lakehouse handles "what happened last month" queries. In a production system, the Lakehouse data could feed into a medallion architecture (Bronze → Silver → Gold) similar to Project 1, or be joined with retail data for cross-domain analysis.

**`ingestion_time()` as the time axis.** When the `event_time` column type was altered from string to datetime, existing rows became null. Rather than reprocessing all historical data, the queries were adapted to use KQL's built-in `ingestion_time()` function. This is a pragmatic decision — in production, you'd document the schema change, use `ingestion_time()` for older data, and `event_time` for new data. The queries in this project demonstrate that pattern.

---

## What This Project Demonstrates

**Real-Time Intelligence workload.** Most Fabric portfolio projects only cover Lakehouses and notebooks. This project uses Eventstreams, Eventhouse, KQL databases, and Real-Time Dashboards — the streaming side of Fabric that maps directly to DP-700 exam topics.

**Event-driven architecture.** The sensor simulator produces events at a steady cadence, the Eventstream routes them, and the dashboard consumes them — all without batch scheduling. This is fundamentally different from the extract-load-transform pattern in Projects 1 and 2.

**KQL fluency.** KQL is its own query language with syntax and patterns that differ significantly from SQL. The queries in this project demonstrate time-series aggregation, threshold detection, time bucketing, schema projection, and the use of built-in ingestion metadata — all common patterns in operational monitoring.

**Architectural decision-making.** Choosing an Eventhouse over a Lakehouse for the primary analytics layer, splitting into a separate workspace, calibrating the anomaly rate, and handling schema evolution — these are judgment calls that show you think about trade-offs, not just follow tutorials.

**Lambda architecture.** Dual-routing the same event stream to both a real-time and batch store is a well-known distributed systems pattern. Demonstrating it in Fabric shows you understand how streaming and batch workloads complement each other.

---

## How to Run This Project

If you're importing this into your own Fabric workspace:

1. Create a new workspace (e.g., `SmartFactory_IoT_Monitoring`)
2. Create an Eventhouse with a KQL database
3. Create an Eventstream with a custom endpoint source and two destinations (the KQL database and a Lakehouse)
4. Import the sensor simulator notebook and attach it to the Lakehouse
5. Update the Eventstream connection string in the notebook (you'll get this from the custom endpoint configuration)
6. Run the simulator — events should appear in both the KQL database and the Lakehouse within seconds
7. Create a Real-Time Dashboard and add tiles using the KQL queries documented above

The simulator needs the `azure-eventhub` Python package. In Fabric notebooks, install it with:

```python
%pip install azure-eventhub
```

---

## Fabric Items in This Folder

The `fabric-artifacts/` subfolder contains workspace items synced via Git integration. These include the notebook code, Eventstream definition, Eventhouse metadata, and dashboard configuration. Actual data (KQL table contents, Lakehouse Delta tables) is not stored in Git — this is expected behavior.

---

## Screenshots

*(Add screenshots to the `screenshots/` folder)*

Recommended captures:
- Eventstream canvas showing source → two destinations
- KQL query results (sensor overview, anomaly detection)
- Real-Time Dashboard with all four tiles populated
- Lakehouse showing the `iot_sensor_raw` Delta table
- Workspace overview showing all items

---

*This is Project 4 in the [Microsoft Fabric Data Engineering Portfolio](../README.md).*
*Project 1: RetailOps Medallion Architecture (Complete ✅)*
*Project 2: RetailOps Pipeline Orchestration (Complete ✅)*
*Project 3: HR Analytics with Dataflows Gen2 (Planned)*
*Project 5: FinanceOps Data Warehouse + dbt (Planned)*
*Project 6: CrossPlatform Mirroring + Shortcuts (Planned)*
