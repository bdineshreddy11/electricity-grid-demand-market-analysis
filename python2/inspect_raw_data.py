import json
import pandas as pd
from pathlib import Path

# Raw data folder
raw_folder = Path(r"D:\electricity-grid-demand-market-analysis\Data\Raw")

# Get the latest raw JSON file
raw_files = sorted(raw_folder.glob("grid_demand_raw_*.json"))

if not raw_files:
    print("No raw JSON file found.")
    exit()

latest_file = raw_files[-1]

print("Reading file:")
print(latest_file)

# Load raw JSON
with open(latest_file, "r", encoding="utf-8") as file:
    raw_data = json.load(file)

# Extract records
items = raw_data["data"]["items"]

# Convert to DataFrame
df = pd.DataFrame(items)

print("\nData loaded successfully!")

print("\nShape:")
print(df.shape)

print("\nColumns:")
print(df.columns.tolist())

print("\nFirst 10 rows:")
print(df.head(10))

print("\nData types:")
print(df.dtypes)

print("\nMissing values:")
print(df.isnull().sum())

print("\nRecords with missing demand:")
print(df[df["demand_mw"].isna()].to_string(index=False))