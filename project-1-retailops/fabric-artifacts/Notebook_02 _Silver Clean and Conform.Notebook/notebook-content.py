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
run_date = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load Bronze
bc = spark.table("bronze_customers")
bp = spark.table("bronze_products")
bo = spark.table("bronze_orders")
bi = spark.table("bronze_order_items")

# -------------------------
# SILVER: CUSTOMERS
# - standardize strings
# - cast timestamps
# - dedupe by email (keep latest created_at)
# -------------------------
customers = (bc
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("phone", F.trim(F.col("phone")))
    .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
    .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("state", F.upper(F.trim(F.col("state"))))
    .withColumn("postal_code", F.trim(F.col("postal_code")))
)

w_email = Window.partitionBy("email").orderBy(F.col("created_at").desc_nulls_last())
customers_dedup = (customers
    .withColumn("_rn",
        F.when(F.col("email").isNotNull(), F.row_number().over(w_email)).otherwise(F.lit(1))
    )
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# -------------------------
# SILVER: PRODUCTS
# - enforce decimals
# - clean strings
# -------------------------
products = (bp
    .withColumn("sku", F.trim(F.col("sku")))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
    .withColumn("brand", F.initcap(F.trim(F.col("brand"))))
    .withColumn("unit_cost", F.col("unit_cost").cast("decimal(12,2)"))
    .withColumn("list_price", F.col("list_price").cast("decimal(12,2)"))
    .withColumn("effective_from", F.to_date("effective_from"))
    .withColumn("effective_to", F.to_date("effective_to"))
)

# -------------------------
# SILVER: ORDERS
# - enforce timestamps
# - normalize status/channel/payment
# - dedupe by order_id using updated_at
# -------------------------
orders = (bo
    .withColumn("order_datetime", F.to_timestamp("order_datetime"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
    .withColumn("channel", F.lower(F.trim(F.col("channel"))))
    .withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))
    .withColumn("ship_city", F.initcap(F.trim(F.col("ship_city"))))
    .withColumn("ship_state", F.upper(F.trim(F.col("ship_state"))))
    .withColumn("ship_postal_code", F.trim(F.col("ship_postal_code")))
)

w_order = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc_nulls_last())
orders_dedup = (orders
    .withColumn("_rn", F.row_number().over(w_order))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# -------------------------
# SILVER: ORDER ITEMS
# - enforce types
# - drop bad rows
# - compute gross/net sales
# -------------------------
items = (bi
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
    .withColumn("discount_amount", F.col("discount_amount").cast("decimal(12,2)"))
    .withColumn("tax_amount", F.col("tax_amount").cast("decimal(12,2)"))
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price").isNotNull())
)

items_enriched = (items
    .withColumn("gross_sales", (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)"))
    .withColumn("net_sales", ((F.col("quantity") * F.col("unit_price")) - F.col("discount_amount")).cast("decimal(12,2)"))
)

# -------------------------
# Write Silver tables
# -------------------------
(customers_dedup.write.mode("overwrite").format("delta").saveAsTable("silver_customers"))
(products.write.mode("overwrite").format("delta").saveAsTable("silver_products"))
(orders_dedup.write.mode("overwrite").format("delta").saveAsTable("silver_orders"))
(items_enriched.write.mode("overwrite").format("delta").saveAsTable("silver_order_items"))

print("Silver tables created:")
for t in ["silver_customers","silver_products","silver_orders","silver_order_items"]:
    print(t, spark.table(t).count()) 

mssparkutils.notebook.exit(f'{{"status":"SUCCESS","run_date":"{run_date}","layer":"silver"}}')    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
