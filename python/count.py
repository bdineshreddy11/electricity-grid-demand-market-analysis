
import os
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient


# ==========================================================
# 1. CONFIGURATION
# ==========================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "electricity_grid")

RAW_COLLECTION = "electricity_data"

REPORT_FOLDER = PROJECT_ROOT / "Data" / "Reports"
BACKUP_FOLDER = PROJECT_ROOT / "Data" / "Raw"

REPORT_FOLDER.mkdir(parents=True, exist_ok=True)
BACKUP_FOLDER.mkdir(parents=True, exist_ok=True)

RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

EXPECTED_FIELDS = [
    "state",
    "demand_mw",
    "peak_mw",
    "frequency_hz",
    "timestamp",
    "source",
    "source_type",
    "source_kind"
]


# ==========================================================
# 2. CONNECT TO MONGODB
# ==========================================================

if not MONGO_URI:
    raise ValueError("MONGO_URI is missing in your .env file.")

client = MongoClient(MONGO_URI)

db = client[MONGO_DATABASE]
collection = db[RAW_COLLECTION]

print("\nConnected to MongoDB Atlas successfully.")
print("Database:", MONGO_DATABASE)
print("Collection:", RAW_COLLECTION)


# ==========================================================
# 3. LOAD RAW DOCUMENTS
# ==========================================================

raw_documents = list(collection.find({}))

raw_document_count = len(raw_documents)

print("\n========== RAW DATA ==========")
print("Raw MongoDB documents:", raw_document_count)


# ==========================================================
# 4. BACKUP RAW DOCUMENTS
# ==========================================================

backup_path = (
    BACKUP_FOLDER
    / f"raw_mongodb_backup_{RUN_TIMESTAMP}.json"
)

with open(backup_path, "w", encoding="utf-8") as file:

    json.dump(
        raw_documents,
        file,
        indent=4,
        default=str,
        ensure_ascii=False
    )

print("Raw backup saved:", backup_path)


# ==========================================================
# 5. RECURSIVE EXTRACTION
# ==========================================================

def extract_state_records(obj, path="root"):

    extracted = []

    if isinstance(obj, dict):

        keys = set(obj.keys())

        # Identify a state-level electricity observation
        if (
            "state" in keys
            and keys.intersection({
                "demand_mw",
                "peak_mw",
                "frequency_hz"
            })
        ):

            extracted.append({
                "record": obj,
                "path": path
            })

        else:

            for key, value in obj.items():

                extracted.extend(
                    extract_state_records(
                        value,
                        path=f"{path}.{key}"
                    )
                )

    elif isinstance(obj, list):

        for index, item in enumerate(obj):

            extracted.extend(
                extract_state_records(
                    item,
                    path=f"{path}[{index}]"
                )
            )

    return extracted


# ==========================================================
# 6. EXTRACT OBSERVATIONS FROM ALL DOCUMENTS
# ==========================================================

records = []
extraction_issues = []

for document_number, document in enumerate(
    raw_documents,
    start=1
):

    mongodb_id = str(document.get("_id", ""))

    extracted_items = extract_state_records(document)

    if not extracted_items:

        extraction_issues.append({
            "MongoDB Document Number": document_number,
            "MongoDB ID": mongodb_id,
            "Issue": "No state-level records found"
        })

        continue

    snapshot_timestamp = (
        document.get("timestamp")
        or document.get("collected_at")
        or document.get("created_at")
        or document.get("as_of")
    )

    for item_number, item_info in enumerate(
        extracted_items,
        start=1
    ):

        item = item_info["record"]

        record = {
            field: item.get(field)
            for field in EXPECTED_FIELDS
        }

        record["snapshot_timestamp"] = snapshot_timestamp
        record["mongodb_document_id"] = mongodb_id
        record["mongodb_document_number"] = document_number
        record["item_number"] = item_number
        record["extraction_path"] = item_info["path"]

        records.append(record)


raw_df = pd.DataFrame(records)

raw_observation_count = len(raw_df)

print("\n========== EXTRACTION RESULTS ==========")
print("Raw MongoDB documents:", raw_document_count)
print("Extracted state-level observations:", raw_observation_count)
print("Documents without state records:", len(extraction_issues))


if raw_df.empty:

    client.close()

    raise ValueError(
        "No state-level observations were extracted. "
        "Check the raw MongoDB document structure."
    )


# ==========================================================
# 7. PRESERVE RAW EXTRACTED DATA
# ==========================================================

raw_extracted_df = raw_df.copy()


# ==========================================================
# 8. STANDARDIZE STATE NAMES
# ==========================================================

raw_df["state"] = (
    raw_df["state"]
    .astype("string")
    .str.strip()
)

state_mapping = {
    "Jammu & Kashmir": "Jammu and Kashmir",
    "NCT of Delhi": "Delhi",
    "Orissa": "Odisha",
    "Pondicherry": "Puducherry"
}

raw_df["state"] = raw_df["state"].replace(state_mapping)


# ==========================================================
# 9. CONVERT DATA TYPES
# ==========================================================

numeric_columns = [
    "demand_mw",
    "peak_mw",
    "frequency_hz"
]

for column in numeric_columns:

    raw_df[column] = pd.to_numeric(
        raw_df[column],
        errors="coerce"
    )


# Parse timestamps into UTC-aware datetime values
raw_df["timestamp"] = pd.to_datetime(
    raw_df["timestamp"],
    errors="coerce",
    utc=True
)


# ==========================================================
# 10. COLUMN-LEVEL DATA QUALITY
# ==========================================================

column_quality = []

for column in EXPECTED_FIELDS:

    series = raw_extracted_df[column]

    missing_mask = series.isna()

    if series.dtype == "object":

        blank_mask = (
            series.astype("string")
            .str.strip()
            .eq("")
            .fillna(False)
        )

        missing_mask = missing_mask | blank_mask

    missing_count = int(missing_mask.sum())

    column_quality.append({

        "Column": column,

        "Total Records": raw_observation_count,

        "Missing / Null / Blank": missing_count,

        "Available Values": (
            raw_observation_count - missing_count
        ),

        "Missing Percentage": round(
            missing_count / raw_observation_count * 100,
            2
        ) if raw_observation_count else 0

    })


column_quality_df = pd.DataFrame(column_quality)


# ==========================================================
# 11. IDENTIFY AGGREGATE / REGIONAL RECORDS
# ==========================================================

aggregate_names = [
    "All India",
    "Eastern",
    "Western",
    "Northern",
    "Southern",
    "North Eastern",
    "Northeastern",
    "DVC",
    "UTs of J&K and Ladakh"
]

aggregate_mask = (
    raw_df["state"]
    .astype("string")
    .str.strip()
    .str.casefold()
    .isin([
        name.casefold()
        for name in aggregate_names
    ])
)

aggregate_df = raw_df.loc[aggregate_mask].copy()


# ==========================================================
# 12. IDENTIFY MISSING / INVALID STATE
# ==========================================================

missing_state_mask = (
    raw_df["state"].isna()
    |
    raw_df["state"]
    .astype("string")
    .str.strip()
    .eq("")
    .fillna(False)
)


# ==========================================================
# 13. IDENTIFY MISSING / INVALID DEMAND
# ==========================================================

missing_demand_mask = (
    raw_df["demand_mw"].isna()
    | (raw_df["demand_mw"] < 0)
)


# ==========================================================
# 14. IDENTIFY MISSING TIMESTAMP
# ==========================================================

missing_timestamp_mask = raw_df["timestamp"].isna()


# ==========================================================
# 15. IDENTIFY INVALID FREQUENCY
# ==========================================================

invalid_frequency_mask = (
    raw_df["frequency_hz"].isna()
    | (raw_df["frequency_hz"] <= 0)
)


# ==========================================================
# 16. IDENTIFY EXACT DUPLICATES
# ==========================================================

duplicate_columns = [
    "state",
    "demand_mw",
    "peak_mw",
    "frequency_hz",
    "timestamp",
    "source",
    "source_type",
    "source_kind"
]

exact_duplicate_mask = raw_df.duplicated(
    subset=duplicate_columns,
    keep="first"
)

exact_duplicates_df = raw_df.loc[
    exact_duplicate_mask
].copy()


# ==========================================================
# 17. IDENTIFY REPEATED STATE + TIMESTAMP
# ==========================================================

state_timestamp_duplicate_mask = raw_df.duplicated(
    subset=["state", "timestamp"],
    keep=False
)

state_timestamp_duplicates_df = raw_df.loc[
    state_timestamp_duplicate_mask
].copy()


# ==========================================================
# 18. IDENTIFY POTENTIAL OUTLIERS USING IQR
# ==========================================================

outlier_mask = pd.Series(
    False,
    index=raw_df.index
)

valid_demand = raw_df["demand_mw"].dropna()

if len(valid_demand) >= 4:

    q1 = valid_demand.quantile(0.25)
    q3 = valid_demand.quantile(0.75)

    iqr = q3 - q1

    lower_bound = q1 - (1.5 * iqr)
    upper_bound = q3 + (1.5 * iqr)

    outlier_mask = (
        (raw_df["demand_mw"] < lower_bound)
        | (raw_df["demand_mw"] > upper_bound)
    ).fillna(False)


outlier_df = raw_df.loc[outlier_mask].copy()


# ==========================================================
# 19. ADD DATA QUALITY FLAGS
# ==========================================================

raw_df["issue_missing_state"] = missing_state_mask

raw_df["issue_aggregate_record"] = aggregate_mask

raw_df["issue_missing_demand"] = (
    raw_df["demand_mw"].isna()
)

raw_df["issue_negative_demand"] = (
    raw_df["demand_mw"] < 0
)

raw_df["issue_missing_timestamp"] = (
    raw_df["timestamp"].isna()
)

raw_df["issue_invalid_frequency"] = (
    invalid_frequency_mask
)

raw_df["issue_exact_duplicate"] = (
    exact_duplicate_mask
)

raw_df["issue_state_timestamp_repeat"] = (
    state_timestamp_duplicate_mask
)

raw_df["issue_potential_outlier"] = (
    outlier_mask
)


# ==========================================================
# 20. DEFINE CLEANING RULES
# ==========================================================

valid_state_mask = (
    ~missing_state_mask
    & ~aggregate_mask
)

valid_record_mask = (
    valid_state_mask
    & ~missing_demand_mask
    & ~missing_timestamp_mask
    & ~invalid_frequency_mask
    & ~exact_duplicate_mask
)


# ==========================================================
# 21. CREATE CLEANED DATA
# ==========================================================

cleaned_df = raw_df.loc[
    valid_record_mask
].copy()

cleaned_df["state"] = (
    cleaned_df["state"]
    .astype("string")
    .str.strip()
)

cleaned_df = cleaned_df.sort_values(
    by=["timestamp", "state"],
    ascending=[True, True]
)

cleaned_df = cleaned_df.reset_index(drop=True)


# ==========================================================
# 22. CREATE REMOVED RECORDS WITH REASONS
# ==========================================================

removed_df = raw_df.loc[
    ~valid_record_mask
].copy()

removed_reasons = []

for index in removed_df.index:

    reasons = []

    if missing_state_mask.loc[index]:
        reasons.append("Missing State")

    if aggregate_mask.loc[index]:
        reasons.append("Aggregate / Regional Record")

    if missing_demand_mask.loc[index]:
        reasons.append("Missing / Invalid Demand")

    if missing_timestamp_mask.loc[index]:
        reasons.append("Missing / Invalid Timestamp")

    if invalid_frequency_mask.loc[index]:
        reasons.append("Invalid Frequency")

    if exact_duplicate_mask.loc[index]:
        reasons.append("Exact Duplicate")

    removed_reasons.append("; ".join(reasons))


removed_df["Removal Reason"] = removed_reasons


# ==========================================================
# 23. EXECUTIVE SUMMARY
# ==========================================================

cleaned_record_count = len(cleaned_df)

removed_record_count = (
    raw_observation_count - cleaned_record_count
)

retention_percentage = (
    cleaned_record_count / raw_observation_count * 100
    if raw_observation_count
    else 0
)

removal_percentage = (
    removed_record_count / raw_observation_count * 100
    if raw_observation_count
    else 0
)

executive_summary_df = pd.DataFrame(
    [
        [
            "Report Generated",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ],
        [
            "Raw MongoDB Documents",
            raw_document_count
        ],
        [
            "Extracted Raw Observations",
            raw_observation_count
        ],
        [
            "Cleaned Records Retained",
            cleaned_record_count
        ],
        [
            "Records Removed",
            removed_record_count
        ],
        [
            "Retention Percentage",
            round(retention_percentage, 2)
        ],
        [
            "Removal Percentage",
            round(removal_percentage, 2)
        ],
        [
            "Extraction Issues",
            len(extraction_issues)
        ]
    ],
    columns=["Metric", "Value"]
)


# ==========================================================
# 24. DATA ISSUE SUMMARY
# ==========================================================

issue_summary = {

    "Missing State":
        int(missing_state_mask.sum()),

    "Aggregate / Regional Records":
        int(aggregate_mask.sum()),

    "Missing / Invalid Demand":
        int(missing_demand_mask.sum()),

    "Missing / Invalid Timestamp":
        int(missing_timestamp_mask.sum()),

    "Invalid Frequency":
        int(invalid_frequency_mask.sum()),

    "Exact Duplicate Records":
        int(exact_duplicate_mask.sum()),

    "Repeated State + Timestamp (Review Only)":
        int(state_timestamp_duplicate_mask.sum()),

    "Potential Demand Outliers (Review Only)":
        int(outlier_mask.sum())

}

issue_summary_df = pd.DataFrame(
    list(issue_summary.items()),
    columns=["Issue Type", "Record Count"]
)


# ==========================================================
# 25. EXCEL TIMEZONE FIX
# ==========================================================

def make_excel_compatible(df):

    df = df.copy()

    for column in df.columns:

        # Remove timezone from timezone-aware datetime columns
        if isinstance(df[column].dtype, pd.DatetimeTZDtype):

            df[column] = (
                df[column]
                .dt.tz_convert("UTC")
                .dt.tz_localize(None)
            )

        # Also handle timezone-aware datetime values
        # hidden inside object columns
        elif df[column].dtype == "object":

            def remove_timezone(value):

                if isinstance(value, pd.Timestamp):

                    if value.tz is not None:

                        return (
                            value.tz_convert("UTC")
                            .tz_localize(None)
                        )

                if isinstance(value, datetime):

                    if value.tzinfo is not None:

                        return (
                            value.astimezone()
                            .replace(tzinfo=None)
                        )

                return value

            df[column] = df[column].apply(
                remove_timezone
            )

    return df


# ==========================================================
# 26. EXPORT EXCEL REPORT
# ==========================================================

excel_path = (
    REPORT_FOLDER
    / f"electricity_data_quality_report_{RUN_TIMESTAMP}.xlsx"
)


excel_sheets = {

    "Executive Summary":
        executive_summary_df,

    "Column Quality":
        column_quality_df,

    "Data Issues":
        issue_summary_df,

    "Raw Extracted Data":
        raw_extracted_df,

    "Cleaned Data":
        cleaned_df,

    "Removed Data":
        removed_df,

    "Duplicate Details":
        exact_duplicates_df,

    "State Timestamp Review":
        state_timestamp_duplicates_df,

    "Outlier Details":
        outlier_df,

    "Aggregate Records":
        aggregate_df,

    "Extraction Issues":
        pd.DataFrame(extraction_issues)

}


with pd.ExcelWriter(
    excel_path,
    engine="openpyxl"
) as writer:

    for sheet_name, dataframe in excel_sheets.items():

        excel_safe_df = make_excel_compatible(
            dataframe
        )

        excel_safe_df.to_excel(
            writer,
            sheet_name=sheet_name,
            index=False
        )


print("\nExcel report successfully generated!")
print("Saved at:", excel_path)


# ==========================================================
# 27. EXPORT CLEANED DATA TO CSV
# ==========================================================

csv_path = (
    REPORT_FOLDER
    / f"electricity_cleaned_data_{RUN_TIMESTAMP}.csv"
)

cleaned_df.to_csv(
    csv_path,
    index=False,
    encoding="utf-8"
)

print("Cleaned CSV successfully generated!")
print("Saved at:", csv_path)


# ==========================================================
# 28. FINAL CONSOLE REPORT
# ==========================================================

print("\n========== FINAL DATA QUALITY REPORT ==========")

print("Raw MongoDB Documents:", raw_document_count)

print("Extracted Raw Observations:", raw_observation_count)

print("Cleaned Records Retained:", cleaned_record_count)

print("Records Removed:", removed_record_count)

print(
    "Retention Percentage:",
    round(retention_percentage, 2),
    "%"
)

print(
    "Removal Percentage:",
    round(removal_percentage, 2),
    "%"
)


print("\n========== INDIVIDUAL DATA ISSUES ==========")

for issue, count in issue_summary.items():

    print(f"{issue}: {count}")


print("\n========== OUTPUT FILES ==========")

print("Excel Report:", excel_path)

print("Cleaned CSV:", csv_path)

print("Raw Backup:", backup_path)


print("\n========== PROCESS COMPLETED SUCCESSFULLY ==========")


# ==========================================================
# 29. CLOSE MONGODB CONNECTION
# ==========================================================

client.close()

print("\nMongoDB connection closed.")
