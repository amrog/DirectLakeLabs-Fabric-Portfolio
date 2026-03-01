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
pipeline_name = ""
pipeline_run_id = ""
run_date = ""
layer = ""            # "bronze", "silver", "gold", "pipeline"
status = ""           # "STARTED", "SUCCESS", "FAILED"
error_message = ""
duration_seconds = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime

schema = StructType([
    StructField("pipeline_name", StringType()),
    StructField("pipeline_run_id", StringType()),
    StructField("run_date", StringType()),
    StructField("layer", StringType()),
    StructField("status", StringType()),
    StructField("error_message", StringType()),
    StructField("duration_seconds", IntegerType()),
    StructField("logged_at", StringType())
])

audit_record = Row(
    pipeline_name=pipeline_name,
    pipeline_run_id=pipeline_run_id,
    run_date=run_date,
    layer=layer,
    status=status,
    error_message=error_message if error_message else None,
    duration_seconds=int(duration_seconds) if duration_seconds else None,
    logged_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
)

audit_df = spark.createDataFrame([audit_record], schema=schema)

audit_df.write.mode("append").format("delta").saveAsTable("pipeline_audit_log")

print(f"Audit logged: {pipeline_name} | {layer} | {status}")
mssparkutils.notebook.exit(f'{{"status":"LOGGED"}}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
