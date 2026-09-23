import json
import pandas as pd
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
raw_folder = Path(r"D:\electricity-grid-demand-market-analysis\Data\Raw")
clean_folder = Path(r"D:\electricity-grid-demand-market-analysis\Data\Clean")

clean_folder.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Get latest raw file
# -----------------------------
raw_files = sorted(raw_folder.glob("grid_demand_raw_*.json"))

if not raw_files:
    print("No raw JSON file found.")
    exit()

latest_file = raw_files[-1]

print("Reading:")
print(latest_file)

# -----------------------------
# Load raw JSON
# -----------------------------
with open(latest_file, "r", encoding="utf-8") as file:
    raw_data = json.load(file)

# -----------------------------
# Create DataFrame
# -----------------------------
df = pd.DataFrame(raw_data["data"]["items"])

print("\nOriginal records:", len(df))

# -----------------------------
# Convert data types
# -----------------------------
df["demand_mw"] = pd.to_numeric(df["demand_mw"], errors="coerce")
df["peak_mw"] = pd.to_numeric(df["peak_mw"], errors="coerce")
df["frequency_hz"] = pd.to_numeric(df["frequency_hz"], errors="coerce")

df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    errors="coerce",
    utc=True
)

# -----------------------------
# Remove records without demand
# -----------------------------
df_clean = df.dropna(subset=["demand_mw"]).copy()

# -----------------------------
# Sort records
# -----------------------------
df_clean = df_clean.sort_values(
    by=["timestamp", "state"]
).reset_index(drop=True)

# -----------------------------
# Save clean CSV
# -----------------------------
clean_file = clean_folder / "grid_demand_clean.csv"

df_clean.to_csv(
    clean_file,
    index=False
)

# -----------------------------
# Validation
# -----------------------------
print("\nCleaning completed!")

print("Original records:", len(df))
print("Clean records:", len(df_clean))
print("Removed records:", len(df) - len(df_clean))

print("\nClean columns:")
print(df_clean.columns.tolist())

print("\nClean data types:")
print(df_clean.dtypes)

print("\nMissing values:")
print(df_clean.isnull().sum())

print("\nClean file saved:")
print(clean_file)