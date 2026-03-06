# Project 4: Real-Time Intelligence — IoT Sensor Monitoring

## Streaming Manufacturing Telemetry with Eventstreams, Eventhouse, KQL, and Real-Time Dashboards

---

## Project Overview

**Goal:** Build an end-to-end real-time analytics solution that simulates IoT sensor data from manufacturing equipment, streams it through Fabric Eventstreams, stores it in an Eventhouse (KQL database), and visualizes live metrics on a Real-Time Dashboard with anomaly detection.

**Scenario:** A manufacturing plant has 10 sensor devices monitoring industrial equipment. Each sensor reports temperature (°C), pressure (PSI), vibration (mm/s), and humidity (%) every 2 seconds. The operations team needs a live dashboard showing current readings, rolling averages, and automatic alerts when values exceed safety thresholds.

**Workspace:** RetailOps_Analytics (same workspace — extends your existing portfolio)

**What You'll Build:**
- `Notebook_05_iot_simulator` — Python notebook that generates realistic sensor events and pushes them to an Eventstream
- `IoT_Eventstream` — Eventstream with a custom endpoint source, routing events to an Eventhouse
- `Manufacturing_Eventhouse` — Eventhouse containing a KQL database for time-series storage
- `sensor_readings` — KQL table holding all ingested sensor telemetry
- KQL Queries — Anomaly detection, rolling averages, threshold alerts
- `IoT_Monitoring_Dashboard` — Real-Time Dashboard with live visualizations

**New Skills Demonstrated:**
- Real-Time Intelligence (Eventstreams, Eventhouse, KQL)
- Streaming ingestion via the `azure-eventhub` Python SDK
- Kusto Query Language (KQL) for time-series analytics
- Real-Time Dashboards (not Power BI — this is a different visualization engine)
- Anomaly detection on streaming data

---

## Architecture

```
[Notebook_05_iot_simulator]
        |
        | azure-eventhub SDK (Event Hub compatible endpoint)
        v
[IoT_Eventstream] ——> Custom Endpoint Source
        |
        | Eventstream routes events
        v
[Manufacturing_Eventhouse]
        |
        | KQL Database: sensor_db
        | Table: sensor_readings
        v
[KQL Queries + Real-Time Dashboard]
```

---

## Phase 1: Create the Eventhouse and KQL Database

The Eventhouse is where your streaming data lands and lives. Think of it as a specialized database engine optimized for time-series and event data — it's built on the same technology as Azure Data Explorer.

### Step 1.1 — Create the Eventhouse

1. In your `RetailOps_Analytics` workspace, click **+ New item**
2. Search for **Eventhouse** in the filter
3. Name it: `Manufacturing_Eventhouse`
4. Click **Create**

When it's created, Fabric automatically provisions a KQL database with the same name (`Manufacturing_Eventhouse`). This is your default database.

### Step 1.2 — Verify the KQL Database

1. Open the Eventhouse — you'll see the **System overview** page
2. In the left **Explorer pane**, under **KQL Databases**, confirm `Manufacturing_Eventhouse` exists
3. Click on the database name to see the **Database details** page

> **Why an Eventhouse instead of a Lakehouse?**
> Lakehouses are optimized for batch analytics — large-volume reads and writes in Delta format. Eventhouses are optimized for streaming ingestion and sub-second queries over time-series data. The KQL query engine handles millions of events per second with time-based aggregations, pattern detection, and anomaly analysis that would be slow or awkward in Spark SQL. In production, you'd often have both — streaming data in an Eventhouse for real-time monitoring, with periodic exports to a Lakehouse for historical batch analytics.

---

## Phase 2: Create the Eventstream with Custom Endpoint

The Eventstream is the ingestion pipeline — it receives events from producers and routes them to destinations.

### Step 2.1 — Create the Eventstream

1. In your workspace, click **+ New item** → **Eventstream**
2. Name it: `IoT_Eventstream`
3. Click **Create**

This opens the Eventstream canvas — a visual designer where you define sources, transformations, and destinations.

### Step 2.2 — Add a Custom Endpoint Source

1. On the Eventstream canvas, click **+ Add source** (or the source node)
2. Select **Custom Endpoint** (also called "Custom App" in some versions)
3. Name the source: `iot_sensor_input`
4. Click **Add**
5. Click **Publish** in the top-right corner to deploy the Eventstream

After publishing, Fabric provisions an Event Hub endpoint behind the scenes. This is the connection your Python notebook will send data to.

### Step 2.3 — Get the Connection String

1. Click on the `iot_sensor_input` source node on the canvas
2. In the details pane below, look for **Keys** or **SAS Key Authentication**
3. You'll see:
   - **Event Hub name** (Entity Path)
   - **Connection string–primary key**

The connection string looks like:
```
Endpoint=sb://eventstream-xxxxx.servicebus.windows.net/;SharedAccessKeyName=key_xxxxx;SharedAccessKey=xxxxx;EntityPath=es_xxxxx
```

**Copy this connection string** — you'll paste it into your Python notebook.

> **What's happening behind the scenes?**
> When you create a Custom Endpoint source, Fabric creates an Event Hub-compatible endpoint. Your Python code uses the `azure-eventhub` SDK to send events to this endpoint using the standard AMQP protocol. The Eventstream then routes these events to whatever destinations you configure. This is the same pattern used in production IoT architectures — the Event Hub protocol is the industry standard for high-throughput event ingestion.

### Step 2.4 — Add the Eventhouse Destination

1. On the Eventstream canvas, click **+ Add destination**
2. Select **Eventhouse** (or **KQL Database**)
3. Configure:
   - **Destination name:** `sensor_kql_destination`
   - **Workspace:** RetailOps_Analytics
   - **Eventhouse:** Manufacturing_Eventhouse
   - **KQL Database:** Manufacturing_Eventhouse
   - **Destination table:** Click **Create new** → name it `sensor_readings`
   - **Input data format:** JSON
4. Click **Add**
5. Click **Publish** to deploy the updated Eventstream

The connection line between your source and destination now shows on the canvas. Events that arrive at the custom endpoint will automatically flow into the `sensor_readings` table in your KQL database.

> **Why route through an Eventstream instead of writing directly to KQL?**
> Eventstreams give you a decoupled architecture. Your producer (the notebook) doesn't need to know anything about the destination. If you later want to add a second destination — say, routing the same events to a Lakehouse for batch processing — you add another destination node on the canvas without changing any code. This is the pub/sub pattern, and it's fundamental to streaming architectures.

---

## Phase 3: Build the IoT Sensor Simulator Notebook

This notebook generates realistic sensor data and pushes it to your Eventstream.

### Step 3.1 — Create the Notebook

1. In your workspace, click **+ New item** → **Notebook**
2. Rename it: `Notebook_05_iot_simulator`

### Step 3.2 — Install the Azure Event Hub SDK

In the first cell, install the required package:

```python
%pip install azure-eventhub --quiet
```

Run this cell. It installs the `azure-eventhub` Python SDK into your Spark session.

### Step 3.3 — Configure the Connection

In the second cell, paste your connection string:

```python
# ============================
# CONFIGURATION
# ============================
CONNECTION_STR = "Endpoint=sb://your-eventstream-endpoint.servicebus.windows.net/;SharedAccessKeyName=key_xxxxx;SharedAccessKey=xxxxx;EntityPath=es_xxxxx"

# Simulator settings
NUM_DEVICES = 10           # Number of simulated sensors
EVENTS_PER_BATCH = 10      # Events sent per batch (1 per device)
NUM_BATCHES = 150          # Total batches to send (150 × 2s = ~5 minutes)
BATCH_INTERVAL_SEC = 2     # Seconds between batches
```

**Replace the connection string** with the one you copied from your Eventstream.

### Step 3.4 — Build the Event Generator

This cell creates realistic sensor data with normal operating ranges and occasional anomalies:

```python
import json
import random
import time
from datetime import datetime, timezone

def generate_sensor_event(device_id):
    """
    Generate a single sensor reading for a manufacturing device.
    
    Normal operating ranges:
    - Temperature: 60-85°C (warning > 90, critical > 100)
    - Pressure: 28-35 PSI (warning > 38, critical > 42)
    - Vibration: 0.5-3.0 mm/s (warning > 4.0, critical > 5.5)
    - Humidity: 30-55% (warning > 65, critical > 75)
    """
    
    # 5% chance of anomalous reading per sensor
    is_anomaly = random.random() < 0.05
    
    if is_anomaly:
        # Generate an out-of-range value for one random metric
        anomaly_type = random.choice(["temperature", "pressure", "vibration", "humidity"])
        temperature = random.uniform(95, 115) if anomaly_type == "temperature" else random.uniform(60, 85)
        pressure = random.uniform(40, 50) if anomaly_type == "pressure" else random.uniform(28, 35)
        vibration = random.uniform(5.0, 8.0) if anomaly_type == "vibration" else random.uniform(0.5, 3.0)
        humidity = random.uniform(70, 90) if anomaly_type == "humidity" else random.uniform(30, 55)
    else:
        temperature = random.uniform(60, 85)
        pressure = random.uniform(28, 35)
        vibration = random.uniform(0.5, 3.0)
        humidity = random.uniform(30, 55)
    
    # Equipment zones in the plant
    zones = ["Assembly_A", "Assembly_B", "Welding", "Paint", "QC_Lab"]
    equipment_types = ["CNC_Mill", "Press", "Conveyor", "Compressor", "Dryer"]
    
    event = {
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "device_id": f"SENSOR-{device_id:03d}",
        "zone": zones[device_id % len(zones)],
        "equipment_type": equipment_types[device_id % len(equipment_types)],
        "temperature_celsius": round(temperature, 2),
        "pressure_psi": round(pressure, 2),
        "vibration_mms": round(vibration, 2),
        "humidity_percent": round(humidity, 2),
        "is_anomaly": is_anomaly
    }
    
    return event
```

### Step 3.5 — Send Events to the Eventstream

This cell sends batches of events at regular intervals:

```python
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

async def send_sensor_data():
    """Send simulated IoT sensor data to Fabric Eventstream."""
    
    producer = EventHubProducerClient.from_connection_string(CONNECTION_STR)
    
    total_events = 0
    
    async with producer:
        for batch_num in range(NUM_BATCHES):
            # Create a batch
            event_data_batch = await producer.create_batch()
            
            # Generate one reading per device
            for device_id in range(1, NUM_DEVICES + 1):
                event = generate_sensor_event(device_id)
                event_data_batch.add(EventData(json.dumps(event)))
            
            # Send the batch
            await producer.send_batch(event_data_batch)
            total_events += NUM_DEVICES
            
            if (batch_num + 1) % 10 == 0:
                print(f"Batch {batch_num + 1}/{NUM_BATCHES} sent | Total events: {total_events}")
            
            # Wait before next batch
            await asyncio.sleep(BATCH_INTERVAL_SEC)
    
    print(f"\nSimulation complete. Total events sent: {total_events}")

# Run the simulator
await send_sensor_data()
```

> **What this does step by step:**
> 1. Creates an Event Hub producer client using your connection string
> 2. For each batch, generates 10 events (one per sensor device)
> 3. Each event includes timestamp, device ID, zone, equipment type, and 4 metric readings
> 4. 5% of readings are intentional anomalies (values outside normal range)
> 5. Sends the batch to the Eventstream endpoint
> 6. Waits 2 seconds, then repeats
> 7. After 150 batches (~5 minutes), stops — giving you 1,500 events to work with

### Step 3.6 — Run the Notebook

Run all cells in order. While it's running, switch to your Eventstream to see events flowing in:

1. Open `IoT_Eventstream`
2. Click on the custom endpoint source node
3. Select the **Data preview** tab at the bottom
4. You should see JSON events appearing in near real-time

Let the notebook run to completion (about 5 minutes). You can stop it early if needed — even 30 seconds of data gives you enough to work with.

---

## Phase 4: Query the Data with KQL

Now the fun part — your data is in the Eventhouse, and you can query it using Kusto Query Language (KQL).

### Step 4.1 — Open the KQL Query Editor

1. Open your `Manufacturing_Eventhouse`
2. Click on the `Manufacturing_Eventhouse` database
3. Click **Query with code** (or open the KQL Queryset)

### Step 4.2 — Verify Data Landed

Run this basic query to confirm your data is there:

```kql
sensor_readings
| take 10
```

You should see rows with your sensor fields. The table was auto-created by the Eventstream based on the JSON schema.

### Step 4.3 — Count Total Events

```kql
sensor_readings
| count
```

If you ran the full 150 batches, you should see approximately 1,500 records.

### Step 4.4 — Latest Reading Per Device

This is a common operations query — "what's the current state of each sensor?"

```kql
sensor_readings
| summarize arg_max(event_time, *) by device_id
| project event_time, device_id, zone, equipment_type, 
          temperature_celsius, pressure_psi, vibration_mms, humidity_percent
| order by device_id asc
```

`arg_max(event_time, *)` returns the row with the most recent timestamp for each device — the KQL equivalent of your Silver layer's window-based dedup, but purpose-built for time-series data.

### Step 4.5 — Rolling Average (Last 5 Minutes)

```kql
sensor_readings
| where event_time > ago(5m)
| summarize 
    avg_temp = round(avg(temperature_celsius), 2),
    avg_pressure = round(avg(pressure_psi), 2),
    avg_vibration = round(avg(vibration_mms), 2),
    avg_humidity = round(avg(humidity_percent), 2)
  by device_id
| order by device_id asc
```

### Step 4.6 — Anomaly Detection: Threshold Breach

Find all readings where any metric exceeded warning thresholds:

```kql
sensor_readings
| where temperature_celsius > 90 
     or pressure_psi > 38 
     or vibration_mms > 4.0 
     or humidity_percent > 65
| project event_time, device_id, zone, equipment_type,
          temperature_celsius, pressure_psi, vibration_mms, humidity_percent
| order by event_time desc
```

### Step 4.7 — Time-Bucketed Trend (30-Second Windows)

Aggregate sensor readings into 30-second windows for trend analysis:

```kql
sensor_readings
| summarize 
    avg_temp = round(avg(temperature_celsius), 2),
    max_temp = round(max(temperature_celsius), 2),
    avg_vibration = round(avg(vibration_mms), 2),
    anomaly_count = countif(is_anomaly == true)
  by bin(event_time, 30s)
| order by event_time asc
```

`bin(event_time, 30s)` is the KQL equivalent of a tumbling window — it groups events into fixed 30-second buckets. This is a core streaming analytics pattern.

### Step 4.8 — Zone-Level Summary

Aggregate by manufacturing zone to see which areas have the most issues:

```kql
sensor_readings
| where event_time > ago(10m)
| summarize 
    total_readings = count(),
    anomaly_count = countif(is_anomaly == true),
    anomaly_rate = round(100.0 * countif(is_anomaly == true) / count(), 2),
    max_temp = round(max(temperature_celsius), 2),
    max_vibration = round(max(vibration_mms), 2)
  by zone
| order by anomaly_rate desc
```

---

## Phase 5: Build the Real-Time Dashboard

Real-Time Dashboards are a separate visualization engine from Power BI — they're built specifically for KQL data and support auto-refresh.

### Step 5.1 — Create the Dashboard

1. In your KQL database, click **New related item** → **Real-Time Dashboard**
   - OR: In the workspace, click **+ New item** → **Real-Time Dashboard**
2. Name it: `IoT_Monitoring_Dashboard`

### Step 5.2 — Add a Data Source

1. In the dashboard toolbar, click **New data source**
2. Select **Eventhouse / KQL Database**
3. Select your `Manufacturing_Eventhouse` Eventhouse
4. Select the `Manufacturing_Eventhouse` database
5. Click **Add**

### Step 5.3 — Tile 1: Current Status Table

1. Click **Add tile** on the dashboard canvas
2. In the KQL query editor, enter:

```kql
sensor_readings
| summarize arg_max(event_time, *) by device_id
| project 
    Device = device_id, 
    Zone = zone,
    Equipment = equipment_type,
    ["Temp (°C)"] = temperature_celsius, 
    ["Pressure (PSI)"] = pressure_psi, 
    ["Vibration (mm/s)"] = vibration_mms,
    ["Last Reading"] = event_time
| order by Device asc
```

3. Click **Run** to verify results
4. Click **Apply changes**
5. In the **Visual formatting** pane:
   - **Tile name:** "Current Sensor Status"
   - **Visual type:** Table
6. Click **Apply changes**

### Step 5.4 — Tile 2: Temperature Trend Line

1. Click **Add tile**
2. Query:

```kql
sensor_readings
| summarize avg_temp = round(avg(temperature_celsius), 2) by bin(event_time, 30s)
| order by event_time asc
```

3. Run the query → **Apply changes**
4. Visual formatting:
   - **Tile name:** "Average Temperature (30s windows)"
   - **Visual type:** Line chart
   - **X axis:** event_time
   - **Y axis:** avg_temp

### Step 5.5 — Tile 3: Anomaly Count by Zone (Bar Chart)

1. Click **Add tile**
2. Query:

```kql
sensor_readings
| where event_time > ago(10m)
| summarize anomalies = countif(is_anomaly == true) by zone
| order by anomalies desc
```

3. Visual formatting:
   - **Tile name:** "Anomalies by Zone (Last 10 min)"
   - **Visual type:** Bar chart
   - **X axis:** zone
   - **Y axis:** anomalies

### Step 5.6 — Tile 4: Vibration Gauge / Stat Card

1. Click **Add tile**
2. Query:

```kql
sensor_readings
| where event_time > ago(5m)
| summarize 
    current_avg = round(avg(vibration_mms), 2),
    max_reading = round(max(vibration_mms), 2)
| project strcat("Avg: ", tostring(current_avg), " mm/s  |  Max: ", tostring(max_reading), " mm/s")
```

3. Visual formatting:
   - **Tile name:** "Vibration (Last 5 min)"
   - **Visual type:** Stat (single value)

### Step 5.7 — Tile 5: Critical Alerts Table

1. Click **Add tile**
2. Query:

```kql
sensor_readings
| where temperature_celsius > 100 
     or pressure_psi > 42 
     or vibration_mms > 5.5 
     or humidity_percent > 75
| project 
    ["Time"] = event_time,
    Device = device_id,
    Zone = zone,
    ["Temp"] = temperature_celsius,
    ["Pressure"] = pressure_psi,
    ["Vibration"] = vibration_mms
| order by event_time desc
| take 20
```

3. Visual formatting:
   - **Tile name:** "Critical Threshold Breaches"
   - **Visual type:** Table

### Step 5.8 — Enable Auto-Refresh

1. In the dashboard toolbar, click the **Auto-refresh** toggle (or the refresh icon)
2. Set refresh interval to **30 seconds**
3. Click **Save** to save the dashboard

Now if you run the simulator notebook again, the dashboard tiles will update automatically every 30 seconds.

---

## Phase 6: (Optional) Add Eventstream Transformations

For extra credit, you can add in-stream transformations before data reaches the Eventhouse.

### Step 6.1 — Add a Filter Transformation

1. Open `IoT_Eventstream`
2. Between the source and destination, click **+ Add transformation**
3. Select **Filter**
4. Configure: Only pass events where `temperature_celsius > 50` (filters out any corrupt readings with impossible values)
5. Publish

### Step 6.2 — Add a Group By (Windowed Aggregation)

1. Add another transformation: **Group By**
2. Configure:
   - **Group by:** `device_id`, `zone`
   - **Time window:** Tumbling, 30 seconds
   - **Aggregation:** Average of `temperature_celsius`, Max of `vibration_mms`
3. Route this aggregated stream to a **second destination** (a different KQL table called `sensor_aggregates`)
4. Publish

This shows you can do stream processing inside the Eventstream itself — a common pattern for reducing data volume before storage.

---

## Phase 7: (Optional) Route to Lakehouse for Historical Analysis

This connects your real-time project back to your medallion architecture.

### Step 7.1 — Add a Lakehouse Destination

1. Open `IoT_Eventstream`
2. Click **+ Add destination** → **Lakehouse**
3. Configure:
   - **Lakehouse:** RetailOps_LH
   - **Table:** `iot_sensor_raw` (create new)
   - **Input data format:** JSON
4. Publish

Now the same sensor events flow to both the Eventhouse (for real-time KQL queries) and the Lakehouse (for historical batch analysis with Spark). This dual-destination pattern is called **Lambda architecture** — real-time and batch processing from the same event stream.

---

## What This Project Demonstrates (Interview Talking Points)

### Structure
- "I built an end-to-end real-time analytics solution: simulated IoT data → Eventstream → Eventhouse → KQL queries → Real-Time Dashboard"
- "The architecture uses a custom endpoint source, which is the same Event Hub-compatible protocol used in production IoT deployments"
- "I wrote KQL queries for rolling averages, anomaly detection, time-bucketed trends, and zone-level aggregations"

### Judgment
- "I chose Eventhouse over Lakehouse for the streaming layer because KQL is purpose-built for sub-second time-series queries — Spark SQL on Delta tables would introduce latency inappropriate for real-time monitoring"
- "I set a 5% anomaly rate in the simulator to produce realistic but testable alert data — enough to validate threshold queries without drowning the dashboard in noise"
- "I implemented a dual-destination pattern (Eventhouse + Lakehouse) to serve both real-time monitoring and historical batch analytics from the same event stream"

### Ownership
- "The dashboard auto-refreshes every 30 seconds and includes both trend visualizations and a critical alerts table for immediate operator response"
- "The simulator is parameterized — I can adjust device count, batch interval, and anomaly rate without changing the streaming infrastructure"
- "KQL queries cover the full operations workflow: current state, trends, anomalies, and zone-level rollups"

---

## Updated Workspace Structure

After completing this project, your workspace will contain:

```
RetailOps_Analytics/
├── Notebook_00_generate_data             (Notebook - data gen)
├── Notebook_01_bronze_ingest             (Notebook - parameterized)
├── Notebook_02_Silver Clean and Conform  (Notebook - parameterized)
├── Notebook_03_Gold Modeling             (Notebook - parameterized)
├── Notebook_04_audit_log                 (Notebook - audit logger)
├── Notebook_05_iot_simulator             (Notebook - NEW, IoT event generator)
├── RetailOps_Master_Pipeline             (Pipeline - orchestrator)
├── RetailOps_LH                          (Lakehouse)
│   └── Tables/
│       ├── bronze_*, silver_*, gold_*
│       ├── pipeline_audit_log
│       └── iot_sensor_raw                (NEW - optional, from Eventstream)
├── IoT_Eventstream                       (Eventstream - NEW)
├── Manufacturing_Eventhouse              (Eventhouse - NEW)
│   └── KQL Database: Manufacturing_Eventhouse
│       ├── sensor_readings               (NEW - streaming data)
│       └── sensor_aggregates             (NEW - optional, windowed)
├── IoT_Monitoring_Dashboard              (Real-Time Dashboard - NEW)
├── RetailOps_SemanticModel               (Semantic Model)
└── RetailOps Analytics Report            (Power BI Report)
```

---

## Checklist

- [ ] Create `Manufacturing_Eventhouse` (Eventhouse + KQL database)
- [ ] Create `IoT_Eventstream` with custom endpoint source
- [ ] Copy the Event Hub connection string from the Eventstream
- [ ] Add Eventhouse destination (`sensor_readings` table)
- [ ] Publish the Eventstream
- [ ] Create `Notebook_05_iot_simulator`
- [ ] Install `azure-eventhub` SDK
- [ ] Configure connection string and simulator settings
- [ ] Build the event generator function (10 devices, 4 metrics, 5% anomalies)
- [ ] Build the async send function
- [ ] Run the simulator — verify events appear in Eventstream Data Preview
- [ ] Open KQL query editor — verify data in `sensor_readings` table
- [ ] Write KQL queries: latest per device, rolling averages, threshold breaches, time-bucketed trends, zone summary
- [ ] Create `IoT_Monitoring_Dashboard`
- [ ] Add tiles: status table, temperature trend, anomaly bar chart, vibration stat, critical alerts
- [ ] Enable 30-second auto-refresh
- [ ] (Optional) Add Eventstream filter/aggregation transformations
- [ ] (Optional) Add Lakehouse destination for dual routing
- [ ] Take screenshots for portfolio

---

## KQL Quick Reference (For Your First Time)

If you've been writing SQL and PySpark, KQL will feel familiar but has its own syntax. Here's a cheat sheet:

| SQL / Spark SQL | KQL Equivalent |
|-----------------|----------------|
| `SELECT * FROM table LIMIT 10` | `table \| take 10` |
| `SELECT col1, col2 FROM table` | `table \| project col1, col2` |
| `WHERE col > 100` | `table \| where col > 100` |
| `GROUP BY col` | `table \| summarize count() by col` |
| `AVG(col)` | `avg(col)` |
| `ORDER BY col DESC` | `table \| order by col desc` |
| `DATE_TRUNC('hour', ts)` | `bin(ts, 1h)` |
| `ROW_NUMBER() OVER (PARTITION BY...)` | `arg_max(ts, *) by partition_col` |
| `CASE WHEN ... THEN ...` | `iff(condition, true_val, false_val)` |
| `WHERE ts > NOW() - INTERVAL 5 MINUTES` | `where ts > ago(5m)` |

The pipe (`|`) operator is the core of KQL — you chain operations left to right, each one transforming the result set. It reads like a data pipeline: take this table → filter it → summarize it → sort it.

---

**Estimated Time:** 3-4 hours
**Difficulty:** Intermediate-Advanced
**Key Skills:** Real-Time Intelligence, Eventstreams, Eventhouse, KQL, Real-Time Dashboards, IoT Streaming, azure-eventhub SDK

---

*This is Project 4 in your Data Engineering Portfolio Series.*
*Project 1: RetailOps Medallion Architecture (Completed ✅)*
*Project 2: Pipeline Orchestration (Completed ✅)*
*Project 3: Dataflows Gen2 — HR Analytics (Skipped for now)*
*Project 5: Data Warehouse + dbt Jobs (Coming Next)*
