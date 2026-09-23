
import pandas as pd
import mysql.connector
from pathlib import Path
from datetime import datetime

# --------------------------------
# File path
# --------------------------------
clean_file = Path(
    r"D:\electricity-grid-demand-market-analysis\Data\Clean\grid_demand_analysis_ready.csv"
)

# --------------------------------
# Load clean CSV
# --------------------------------
df = pd.read_csv(clean_file)

# Convert timestamp
df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    utc=True
)

# --------------------------------
# MySQL connection
# --------------------------------
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Root123@",
    database="electricity_grid"
)

cursor = connection.cursor()

# --------------------------------
# Insert data
# --------------------------------
insert_query = """
INSERT INTO grid_demand
(
    state,
    demand_mw,
    peak_mw,
    frequency_hz,
    timestamp_utc,
    source,
    source_type,
    source_kind,
    collected_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

collected_at = datetime.now()

records = []

for _, row in df.iterrows():

    timestamp_utc = (
        row["timestamp"]
        .to_pydatetime()
        .replace(tzinfo=None)
    )

    records.append(
        (
            row["state"],
            row["demand_mw"],
            row["peak_mw"],
            row["frequency_hz"],
            timestamp_utc,
            row["source"],
            row["source_type"],
            row["source_kind"],
            collected_at
        )
    )

cursor.executemany(insert_query, records)

connection.commit()

print("Data inserted successfully!")
print("Rows inserted:", cursor.rowcount)

# --------------------------------
# Close connection
# --------------------------------
cursor.close()
connection.close()

print("MySQL connection closed.")
