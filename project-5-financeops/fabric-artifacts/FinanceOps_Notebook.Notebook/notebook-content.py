# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bb4682e0-07bd-4b13-89a1-fb1eca401b8f",
# META       "default_lakehouse_name": "FinanceOps_Staging_LH",
# META       "default_lakehouse_workspace_id": "33690ddf-e33e-450e-b433-2dfd36785519",
# META       "known_lakehouses": [
# META         {
# META           "id": "bb4682e0-07bd-4b13-89a1-fb1eca401b8f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import pandas as pd
import pyodbc

# Read CSVs
je_df = pd.read_csv("D:\Fabricverse\Personal Porfolio Projects\Project 5\files\journal_entries.csv")
jel_df = pd.read_csv("D:\Fabricverse\Personal Porfolio Projects\Project 5\files\journal_entry_lines.csv")

# Connect to warehouse (use Azure CLI auth)
conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=<your-sql-endpoint>;"
    "Database=FinanceOps_DW;"
    "Authentication=ActiveDirectoryInteractive;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Insert journal entries
for _, row in je_df.iterrows():
    cursor.execute("""
        INSERT INTO raw.journal_entries
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, row.journal_entry_id, row.posting_date, row.description,
         row.source_system, row.created_by, row.status, row.created_at)

conn.commit()
# Repeat for journal_entry_lines

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
