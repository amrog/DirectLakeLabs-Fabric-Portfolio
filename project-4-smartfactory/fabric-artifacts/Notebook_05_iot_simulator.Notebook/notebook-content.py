# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

%pip install azure-eventhub --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================
# CONFIGURATION
# ============================
CONNECTION_STR = "Endpoint=sb://esehchpxwzdfaho07q1y3u.servicebus.windows.net/;SharedAccessKeyName=key_750df199-0252-4315-9bb6-cd76383353fd;SharedAccessKey=YvmjWhV2+Q8J1ozyny34Aey3feVqqtKUX+AEhHGdDWE=;EntityPath=es_fce02774-7f12-4617-be64-cc806fd8036c"

# Simulator settings
NUM_DEVICES = 10           # Number of simulated sensors
EVENTS_PER_BATCH = 10      # Events sent per batch (1 per device)
NUM_BATCHES = 150          # Total batches to send (150 × 2s = ~5 minutes)
BATCH_INTERVAL_SEC = 2     # Seconds between batches

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
                print(f"Batch{batch_num + 1}/{NUM_BATCHES} sent | Total events:{total_events}")

            # Wait before next batch
            await asyncio.sleep(BATCH_INTERVAL_SEC)

    print(f"\nSimulation complete. Total events sent:{total_events}")

# Run the simulator
await send_sensor_data()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
