import pandas as pd
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
clean_folder = Path(r"D:\electricity-grid-demand-market-analysis\Data\Clean")

input_file = clean_folder / "grid_demand_clean.csv"
output_file = clean_folder / "grid_demand_analysis_ready.csv"

# -----------------------------
# Load clean data
# -----------------------------
df = pd.read_csv(input_file)

print("Clean data loaded successfully!")
print("Original shape:", df.shape)

# -----------------------------
# Convert timestamp
# -----------------------------
df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    utc=True
)

# -----------------------------
# Create date/time columns
# -----------------------------
df["date"] = df["timestamp"].dt.date
df["time"] = df["timestamp"].dt.strftime("%H:%M:%S")
df["hour"] = df["timestamp"].dt.hour
df["day_of_week"] = df["timestamp"].dt.day_name()

# -----------------------------
# Demand vs peak percentage
# -----------------------------
df["demand_vs_peak_pct"] = (
    df["demand_mw"] / df["peak_mw"]
) * 100

# -----------------------------
# Demand status
# -----------------------------
df["demand_status"] = pd.cut(
    df["demand_vs_peak_pct"],
    bins=[0, 70, 85, 95, 100],
    labels=[
        "Low",
        "Normal",
        "High",
        "Very High"
    ],
    include_lowest=True
)

# -----------------------------
# Save analysis-ready data
# -----------------------------
df.to_csv(
    output_file,
    index=False
)

# -----------------------------
# Validation
# -----------------------------
print("\nAnalysis-ready data created!")

print("Final shape:", df.shape)

print("\nNew columns:")
print([
    "date",
    "time",
    "hour",
    "day_of_week",
    "demand_vs_peak_pct",
    "demand_status"
])

print("\nDemand status:")
print(df["demand_status"].value_counts())

print("\nFirst 10 rows:")
print(
    df[
        [
            "state",
            "demand_mw",
            "peak_mw",
            "demand_vs_peak_pct",
            "demand_status",
            "timestamp"
        ]
    ].head(10)
)

print("\nFile saved:")
print(output_file)