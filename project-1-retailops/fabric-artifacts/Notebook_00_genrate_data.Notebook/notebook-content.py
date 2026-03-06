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

# Install python packages
# Welcome to your new notebook
# Type here in the cell editor to add code!
# Install faker once per session if needed
%pip install faker pandas numpy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Install + Import pytthon libraries

%pip install faker pandas numpy

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Full data Generation (creeates the lists and DataFrames)

fake = Faker()
random.seed(42)
np.random.seed(42)

N_CUSTOMERS = 20000
N_PRODUCTS = 3000
N_ORDERS = 150000
AVG_ITEMS_PER_ORDER = 2.4

start_date = datetime(2024, 1, 1)
end_date   = datetime(2026, 2, 1)

def rand_dt():
    delta = end_date - start_date
    return start_date + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

loyalty_tiers = ["Bronze", "Silver", "Gold", "Platinum"]
tier_weights = [0.55, 0.28, 0.14, 0.03]

channels = ["web", "mobile", "store", "marketplace"]
channel_weights = [0.45, 0.35, 0.15, 0.05]

statuses = ["placed", "shipped", "delivered", "cancelled", "refunded"]
status_weights = [0.08, 0.12, 0.70, 0.06, 0.04]

payment_methods = ["card", "paypal", "apple_pay", "google_pay", "klarna"]
payment_weights = [0.62, 0.14, 0.10, 0.08, 0.06]

categories = ["Running", "Cycling", "Training", "Apparel", "Accessories", "Nutrition"]
brands = ["Apex", "NorthPeak", "VeloWorks", "StrideCo", "IronLeaf", "SummitGear"]

# ---- Customers
customers = []
for cid in range(1, N_CUSTOMERS + 1):
    created = rand_dt()
    is_active = 1 if random.random() > 0.06 else 0
    tier = random.choices(loyalty_tiers, weights=tier_weights, k=1)[0]

    email = fake.email() if random.random() > 0.04 else None
    phone = fake.phone_number() if random.random() > 0.12 else None

    street = fake.street_address() if random.random() > 0.08 else None
    city = fake.city() if random.random() > 0.08 else None
    state = fake.state_abbr() if random.random() > 0.12 else None
    postal = fake.postcode() if random.random() > 0.10 else None

    customers.append({
        "customer_id": cid,
        "created_at": created,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": email,
        "phone": phone,
        "street": street,
        "city": city,
        "state": state,
        "postal_code": postal,
        "country": "US",
        "loyalty_tier": tier,
        "is_active": is_active
    })

df_customers = pd.DataFrame(customers)

# ---- Products
products = []
for pid in range(1, N_PRODUCTS + 1):
    eff_from = fake.date_between(start_date="-2y", end_date="today")
    discontinued = 1 if random.random() < 0.08 else 0
    eff_to = None if discontinued == 0 else fake.date_between(start_date=eff_from, end_date="today")

    cost = round(random.uniform(3, 120), 2)
    price = round(cost * random.uniform(1.25, 2.8), 2)

    products.append({
        "product_id": pid,
        "sku": f"SKU-{pid:06d}",
        "product_name": f"{random.choice(['Pro','Elite','Core','Aero','Trail','Endurance'])} {fake.word().title()}",
        "category": random.choice(categories),
        "brand": random.choice(brands),
        "unit_cost": cost,
        "list_price": price,
        "is_discontinued": discontinued,
        "effective_from": eff_from,
        "effective_to": eff_to
    })

df_products = pd.DataFrame(products)

# ---- Orders + Items
customer_ids = df_customers["customer_id"].values
n = len(customer_ids)

zipf = np.random.zipf(a=1.15, size=n).astype(float)
zipf = zipf / zipf.sum()

uniform = np.ones(n) / n

weights = 0.60 * zipf + 0.40 * uniform
weights = weights / weights.sum()

print("weights check:", weights.min(), weights.max(), weights.sum())

orders = []
order_items = []
order_item_id = 1

product_ids_all = df_products["product_id"].values

for oid in range(1, N_ORDERS + 1):
    cust = int(np.random.choice(customer_ids, p=weights))
    odt = rand_dt()
    status = random.choices(statuses, weights=status_weights, k=1)[0]
    channel = random.choices(channels, weights=channel_weights, k=1)[0]
    pay = random.choices(payment_methods, weights=payment_weights, k=1)[0]
    updated_at = odt + timedelta(hours=random.randint(0, 240))

    orders.append({
        "order_id": oid,
        "customer_id": cust,
        "order_datetime": odt,
        "order_status": status,
        "channel": channel,
        "payment_method": pay,
        "ship_city": fake.city() if random.random() > 0.05 else None,
        "ship_state": fake.state_abbr() if random.random() > 0.05 else None,
        "ship_postal_code": fake.postcode() if random.random() > 0.07 else None,
        "ship_country": "US",
        "updated_at": updated_at
    })

    n_items = max(1, int(np.random.poisson(AVG_ITEMS_PER_ORDER)))
    picked = np.random.choice(product_ids_all, size=min(n_items, len(product_ids_all)), replace=False)

    for pid in picked:
        pid = int(pid)
        qty = random.randint(1, 4)
        base_price = float(df_products.loc[df_products["product_id"] == pid, "list_price"].iloc[0])

        discount = round(base_price * qty * random.choice([0, 0, 0.05, 0.10, 0.15]), 2)
        subtotal = base_price * qty - discount
        tax = round(subtotal * 0.0825, 2)

        order_items.append({
            "order_item_id": order_item_id,
            "order_id": oid,
            "product_id": pid,
            "quantity": qty,
            "unit_price": round(base_price, 2),
            "discount_amount": discount,
            "tax_amount": tax,
            "created_at": odt
        })
        order_item_id += 1

df_orders = pd.DataFrame(orders)
df_order_items = pd.DataFrame(order_items)

print("Generated:",
      "customers=", len(df_customers),
      "products=", len(df_products),
      "orders=", len(df_orders),
      "order_items=", len(df_order_items))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run this ngeneration script first to declare the data frames (df) for ussage
df_customers = pd.DataFrame(customers)
df_products = pd.DataFrame(products)
df_orders = pd.DataFrame(orders)
df_order_items = pd.DataFrame(order_items)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ensure folder exists
import os

os.makedirs("/lakehouse/default/Files/raw/retailops", exist_ok=True)

df_customers.to_csv("/lakehouse/default/Files/raw/retailops/customers.csv", index=False)
df_products.to_csv("/lakehouse/default/Files/raw/retailops/products.csv", index=False)
df_orders.to_csv("/lakehouse/default/Files/raw/retailops/orders.csv", index=False)
df_order_items.to_csv("/lakehouse/default/Files/raw/retailops/order_items.csv", index=False)

print(os.listdir("/lakehouse/default/Files/raw/retailops"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os

print("Exists /lakehouse?:", os.path.exists("/lakehouse"))
print("Exists /lakehouse/default?:", os.path.exists("/lakehouse/default"))
print("Exists Files root?:", os.path.exists("/lakehouse/default/Files"))

if os.path.exists("/lakehouse/default/Files"):
    print("Files contents:", os.listdir("/lakehouse/default/Files"))
    print("raw contents:", os.listdir("/lakehouse/default/Files/raw") if os.path.exists("/lakehouse/default/Files/raw") else "no raw/")
    print("retailops contents:", os.listdir("/lakehouse/default/Files/raw/retailops") if os.path.exists("/lakehouse/default/Files/raw/retailops") else "no retailops/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F

BASE = "Files/raw/retailops"

def read_csv(name):
    return (spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{BASE}/{name}.csv"))

batch_id = F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")

def add_meta(df, source_name):
    return (df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(source_name))
            .withColumn("_batch_id", batch_id))

customers = add_meta(read_csv("customers"), "customers.csv")
products  = add_meta(read_csv("products"), "products.csv")
orders    = add_meta(read_csv("orders"), "orders.csv")
items     = add_meta(read_csv("order_items"), "order_items.csv")

(customers.write.mode("overwrite").format("delta").saveAsTable("bronze_customers"))
(products.write.mode("overwrite").format("delta").saveAsTable("bronze_products"))
(orders.write.mode("overwrite").format("delta").saveAsTable("bronze_orders"))
(items.write.mode("overwrite").format("delta").saveAsTable("bronze_order_items"))

print("Bronze tables created:")
for t in ["bronze_customers","bronze_products","bronze_orders","bronze_order_items"]:
    print(t, spark.table(t).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
