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
# PARAMETERS (Toggle this cell as Parameter cell)
# ============================
run_date = ""          # Pipeline passes YYYY-MM-DD; empty = today
source_folder = "raw/retailops"   # Subfolder under Files/
run_mode = "overwrite"            # "overwrite" or "append"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

from pyspark.sql import functions as F
from datetime import datetime

# Resolve run_date
if not run_date:
    run_date = datetime.now().strftime("%Y-%m-%d")

batch_id = F.lit(run_date.replace("-", "") + datetime.now().strftime("%H%M%S"))

# Dynamic base path using parameter
BASE = f"Files/{source_folder}"

def read_csv(name):
    return (spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{BASE}/{name}.csv"))

def add_meta(df, source_name):
    return (df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(source_name))
            .withColumn("_batch_id", batch_id)
            .withColumn("_run_date", F.lit(run_date)))


batch_id = F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")

# Read and add metadata
customers = add_meta(read_csv("customers"), "customers.csv")
products  = add_meta(read_csv("products"), "products.csv")
orders    = add_meta(read_csv("orders"), "orders.csv")
items     = add_meta(read_csv("order_items"), "order_items.csv")

# Write to Bronze using parameterized mode
customers.write.mode(run_mode).format("delta").option("mergeSchema",  "true").saveAsTable("bronze_customers")
products.write.mode(run_mode).format("delta").option("mergeSchema",  "true").saveAsTable("bronze_products")
orders.write.mode(run_mode).format("delta").option("mergeSchema",  "true").saveAsTable("bronze_orders")
items.write.mode(run_mode).format("delta").option("mergeSchema",  "true").saveAsTable("bronze_order_items")

# Output for pipeline tracking
print(f"Bronze ingest complete | run_date={run_date} | mode={run_mode}")
for t in ["bronze_customers", "bronze_products", "bronze_orders", "bronze_order_items"]:
    print(t, spark.table(t).count())

# Exit value for pipeline (used to pass data between activities)
mssparkutils.notebook.exit(f'{{"status":"SUCCESS","run_date":"{run_date}","tables_loaded":4}}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
