import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv
import numpy as np
import pandas as pd
from pymongo import MongoClient

# =========================================================
# 1. ENVIRONMENT & MONGODB ATLAS CONNECTION
# =========================================================

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    raise ValueError("MONGO_URI not found in .env file.")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
client.admin.command("ping")

db = client["electricity_grid"]

raw_collection = db["electricity_data"]
clean_collection = db["electricity_clean"]
normalized_collection = db["electricity_normalized"]
audit_collection = db["pipeline_audit_log"]
rejected_collection = db["rejected_electricity_data"]

EXPORT_DIR = Path("exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
CONSOLIDATED_EXCEL_PATH = EXPORT_DIR / "electricity_cleaning_audit.xlsx"


# =========================================================
# 2. STAGE 4 & 5: DATA CONVERSION & TEXT NORMALIZATION
# =========================================================

STATE_NAME_MAPPINGS = {
    "DADAR & NAGAR HAVELI": "Dadra and Nagar Haveli and Daman and Diu",
    "DAMAN & DIU": "Dadra and Nagar Haveli and Daman and Diu",
    "DADRA & NAGAR HAVELI": "Dadra and Nagar Haveli and Daman and Diu",
    "DADRA AND NAGAR HAVELI AND DAMAN AND DIU": "Dadra and Nagar Haveli and Daman and Diu",
    "ANDAMAN & NICOBAR ISLANDS": "Andaman and Nicobar Islands",
    "JAMMU & KASHMIR": "Jammu and Kashmir",
    "UTS OF J&K AND LADAKH": "Jammu and Kashmir and Ladakh",
    "DVC": "Damodar Valley Corporation",
    "NORTH EASTERN": "North Eastern Region",
    "NORTHERN": "Northern Region",
    "EASTERN": "Eastern Region",
    "WESTERN": "Western Region",
    "SOUTHERN": "Southern Region",
}


def normalize_text(text: str) -> str:
    """Stage 5: Text and Categorical Normalization."""
    if text is None:
        return ""
    cleaned = re.sub(r"\s+", " ", str(text).strip())
    upper_cleaned = cleaned.upper()
    return STATE_NAME_MAPPINGS.get(upper_cleaned, cleaned.title())


def to_number(value):
    """Stage 4: Data Type Casting to Float."""
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return float(value)
    except (ValueError, TypeError):
        return None


def to_datetime(value):
    """Stage 4: Temporal Standardization to UTC datetime."""
    if value is None or value == "":
        return None

    if isinstance(value, dict) and "$date" in value:
        value = value["$date"]

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        except (ValueError, OverflowError, OSError):
            return None

    try:
        cleaned_str = str(value).replace("Z", "+00:00").strip()
        result = datetime.fromisoformat(cleaned_str)
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError):
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d"):
        try:
            result = datetime.strptime(str(value).strip(), fmt)
            if result.tzinfo is None:
                result = result.replace(tzinfo=timezone.utc)
            return result.astimezone(timezone.utc)
        except (ValueError, TypeError):
            continue

    return None


# =========================================================
# 3. STAGE 1: STATISTICAL PROFILING
# =========================================================

def profile_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 1: Initial Profiling & Statistical Inspection."""
    profiles = []
    total_rows = len(df)

    for col in df.columns:
        col_data = df[col]
        null_count = int(col_data.isna().sum())
        null_pct = round((null_count / total_rows) * 100, 2) if total_rows > 0 else 0
        unique_count = int(col_data.nunique(dropna=True))

        numeric_series = pd.to_numeric(col_data, errors="coerce")
        valid_numeric = numeric_series.dropna()

        if not valid_numeric.empty:
            mean_val = round(float(valid_numeric.mean()), 2)
            std_val = round(float(valid_numeric.std()), 2) if len(valid_numeric) > 1 else 0.0
            min_val = round(float(valid_numeric.min()), 2)
            max_val = round(float(valid_numeric.max()), 2)
            skew_val = round(float(valid_numeric.skew()), 2) if len(valid_numeric) > 2 else 0.0
        else:
            mean_val = std_val = min_val = max_val = skew_val = None

        profiles.append({
            "Column Name": col,
            "Inferred Type": str(col_data.dtype),
            "Total Rows": total_rows,
            "Null Count": null_count,
            "Null Percentage (%)": null_pct,
            "Unique Count": unique_count,
            "Min": min_val,
            "Max": max_val,
            "Mean": mean_val,
            "Std Dev": std_val,
            "Skewness": skew_val
        })

    return pd.DataFrame(profiles)


# =========================================================
# 4. STAGE 6: OUTLIER BOUNDARY COMPUTATION (IQR)
# =========================================================

def calculate_state_iqr_thresholds(unwound_data):
    """Stage 6: Pre-compute IQR boundaries for demand_mw grouped by state."""
    state_demands = {}
    for row in unwound_data:
        st = normalize_text(row.get("state"))
        val = to_number(row.get("demand_mw"))
        if st and val is not None and val > 0:
            state_demands.setdefault(st, []).append(val)

    iqr_bounds = {}
    for st, vals in state_demands.items():
        if len(vals) >= 8:
            q25, q75 = np.percentile(vals, [25, 75])
            iqr = q75 - q25
            lower_bound = max(0.0, q25 - (1.5 * iqr))
            upper_bound = q75 + (1.5 * iqr)
            iqr_bounds[st] = (lower_bound, upper_bound)
        else:
            iqr_bounds[st] = (0.0, 100000.0)

    return iqr_bounds


# =========================================================
# 5. CORE EXECUTION & SINGLE EXCEL EXPORT
# =========================================================

def execute_cleaning_and_export():
    run_id = str(uuid4())
    run_time = datetime.now(timezone.utc)

    print("\n[1/8] Fetching raw electricity snapshots from MongoDB Atlas...")
    raw_docs = list(raw_collection.find({}))

    if not raw_docs:
        raise ValueError(f"No documents found in raw collection '{raw_collection.name}'.")

    total_snapshots = len(raw_docs)
    print(f"Loaded {total_snapshots:,} snapshot documents.")

    # Stage 7: Schema & Structural Alignment (Unwinding raw nested structures)
    raw_flattened_rows = []
    unwound_records = []

    for doc in raw_docs:
        doc_id = str(doc.get("_id"))
        collected_at = doc.get("collected_at")
        api_as_of = doc.get("api_as_of")
        all_india_info = doc.get("all_india") or {}
        states_list = doc.get("states") or []

        raw_flattened_rows.append({
            "raw_document_id": doc_id,
            "collected_at": str(collected_at),
            "api_as_of": str(api_as_of),
            "all_india_demand_mw": all_india_info.get("demand_mw"),
            "all_india_peak_mw": all_india_info.get("peak_mw"),
            "states_count": len(states_list),
            "coverage_live_states": (doc.get("coverage") or {}).get("live_states")
        })

        for state_item in states_list:
            if isinstance(state_item, dict):
                unwound_records.append({
                    "raw_document_id": doc_id,
                    "collected_at": collected_at,
                    "state": state_item.get("state"),
                    "demand_mw": state_item.get("demand_mw"),
                    "peak_mw": state_item.get("peak_mw"),
                    "frequency_hz": state_item.get("frequency_hz"),
                    "source": state_item.get("source"),
                    "source_type": state_item.get("source_type"),
                    "source_kind": state_item.get("source_kind"),
                    "timestamp": state_item.get("timestamp")
                })

    raw_unwound_df = pd.DataFrame(unwound_records)
    total_unwound = len(raw_unwound_df)
    print(f"[2/8] Extracted {total_unwound:,} individual state measurements.")

    # Stage 1: Data Profiling
    print("[3/8] Running initial statistical data profiling...")
    profiling_df = profile_dataframe(raw_unwound_df)

    # Stage 6: Calculate statistical outlier bounds
    print("[4/8] Calculating IQR outlier limits across states...")
    iqr_bounds = calculate_state_iqr_thresholds(unwound_records)

    # Stages 2, 3, 4, 5, 6: Validation, Cleaning, Outliers & Deduplication
    print("[5/8] Applying validation, deduplication, and cleaning rules...")

    cleaned_records = []
    rejected_records = []
    seen_signatures = set()
    reason_counter = Counter()

    for item in unwound_records:
        reasons = []

        # Stage 5: Text normalization
        raw_state = item.get("state")
        state_name = normalize_text(raw_state)

        # Stage 2: Missing state check
        if not state_name or state_name.lower() in ("none", "null", ""):
            reasons.append("missing_state_name")

        # Stage 4: Timestamp standardization
        dt_collected = to_datetime(item.get("collected_at"))
        if dt_collected is None:
            reasons.append("invalid_collected_at")

        # Stage 2 & 4: Numeric validation
        raw_demand = item.get("demand_mw")
        demand_num = to_number(raw_demand)

        if raw_demand is None or raw_demand == "":
            reasons.append("missing_demand")
        elif demand_num is None or demand_num <= 0:
            reasons.append("invalid_or_negative_demand")
        else:
            # Stage 6: Statistical outlier boundary test
            bounds = iqr_bounds.get(state_name)
            if bounds:
                low_b, high_b = bounds
                if demand_num > high_b or demand_num < low_b:
                    reasons.append(f"demand_iqr_outlier (>{high_b:.1f}MW)")

        # Stage 6: Grid frequency physical sanity test (47.5 Hz to 52.0 Hz)
        freq_num = to_number(item.get("frequency_hz"))
        if freq_num is not None and (freq_num < 47.5 or freq_num > 52.0):
            reasons.append(f"frequency_out_of_bounds ({freq_num}Hz)")

        # Stage 3: Compound deduplication
        source_name = str(item.get("source") or "unknown").strip().lower()
        signature = (state_name, str(dt_collected), source_name)
        if signature in seen_signatures:
            reasons.append("duplicate_record")
        else:
            seen_signatures.add(signature)

        # Stage 8: Data Validation & Routing
        if reasons:
            for r in reasons:
                category = r.split(" ")[0]
                reason_counter[category] += 1

            rejected_records.append({
                "source_document_id": item["raw_document_id"],
                "raw_state": raw_state,
                "normalized_state": state_name,
                "demand_mw": raw_demand,
                "collected_at": str(item.get("collected_at")),
                "rejection_reasons": "; ".join(reasons)
            })
        else:
            peak_num = to_number(item.get("peak_mw"))
            dt_timestamp = to_datetime(item.get("timestamp"))

            cleaned_records.append({
                "collected_at": dt_collected.strftime("%Y-%m-%d %H:%M:%S"),
                "state": state_name,
                "demand_mw": round(demand_num, 2),
                "peak_mw": round(peak_num, 2) if peak_num is not None else None,
                "frequency_hz": round(freq_num, 4) if freq_num is not None else 50.0000,
                "source": source_name,
                "source_type": str(item.get("source_type") or "modeled").strip().lower(),
                "source_kind": str(item.get("source_kind") or "modeled").strip().lower(),
                "timestamp": dt_timestamp.strftime("%Y-%m-%d %H:%M:%S") if dt_timestamp else None,
                "source_record_id": item["raw_document_id"]
            })

    total_clean = len(cleaned_records)
    total_rejected = len(rejected_records)

    # Stage 8: Reconciliation assertion
    assert total_unwound == total_clean + total_rejected, "Reconciliation mismatch between raw, clean, and rejected!"

    print(f"[6/8] Verification complete: {total_clean:,} accepted | {total_rejected:,} rejected.")

    # =====================================================
    # 6. EXPORT ALL PROCESS DATA TO A SINGLE EXCEL FILE
    # =====================================================
    print(f"\n[7/8] Generating consolidated single Excel workbook at '{CONSOLIDATED_EXCEL_PATH}'...")

    summary_df = pd.DataFrame([
        {"Metric": "Pipeline Run ID", "Value": run_id},
        {"Metric": "Execution Timestamp (UTC)", "Value": str(run_time)},
        {"Metric": "Raw Snapshots Processed", "Value": total_snapshots},
        {"Metric": "Total State Measurements Checked", "Value": total_unwound},
        {"Metric": "Clean Validated Records", "Value": total_clean},
        {"Metric": "Invalid Records Rejected", "Value": total_rejected},
        {"Metric": "Data Retention Rate (%)", "Value": round((total_clean / total_unwound) * 100, 2) if total_unwound else 0.0},
        {"Metric": "Deduplicated Unique Signatures", "Value": len(seen_signatures)}
    ])

    reasons_df = pd.DataFrame(
        [{"Rejection Category": k, "Total Occurrences": v} for k, v in sorted(reason_counter.items(), key=lambda x: x[1], reverse=True)]
    )
    if reasons_df.empty:
        reasons_df = pd.DataFrame([{"Rejection Category": "None", "Total Occurrences": 0}])

    raw_summary_df = pd.DataFrame(raw_flattened_rows)
    clean_df = pd.DataFrame(cleaned_records)
    rejected_df = pd.DataFrame(rejected_records[:10000])

    with pd.ExcelWriter(CONSOLIDATED_EXCEL_PATH, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Audit Summary", index=False)
        reasons_df.to_excel(writer, sheet_name="Rejection Analytics", index=False)
        profiling_df.to_excel(writer, sheet_name="Initial Profiling", index=False)
        clean_df.to_excel(writer, sheet_name="Clean Data", index=False)
        raw_summary_df.to_excel(writer, sheet_name="Raw Data Summary", index=False)
        if not rejected_df.empty:
            rejected_df.to_excel(writer, sheet_name="Rejected Records Log", index=False)

    print(f"Excel workbook generated: {CONSOLIDATED_EXCEL_PATH}")

    # =====================================================
    # 7. LOG RUN AUDIT TO MONGODB
    # =====================================================
    print("[8/8] Persisting audit summary to MongoDB Atlas...")
    audit_collection.insert_one({
        "run_id": run_id,
        "run_time": run_time,
        "total_snapshots": total_snapshots,
        "total_state_measurements": total_unwound,
        "clean_records": total_clean,
        "rejected_records": total_rejected,
        "rejection_counts": dict(reason_counter),
        "status": "completed"
    })

    print("\nData cleaning audit pipeline finished successfully.")


if __name__ == "__main__":
    try:
        execute_cleaning_and_export()
    finally:
        client.close()