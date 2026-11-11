import os
import sys
from math import isfinite
from dotenv import load_dotenv
from pymongo import MongoClient, InsertOne
from pymongo.errors import PyMongoError, BulkWriteError

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    raise ValueError("MONGO_URI is not set in environment or .env")

BATCH_SIZE = 1000

client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=10000
)

def to_number(value):
    try:
        if value is None or value == "":
            return None
        number = float(value)
        return number if isfinite(number) else None
    except (ValueError, TypeError):
        return None

def write_batch(collection, operations):
    """
    Executes bulk write safely by ignoring duplicate key conflicts (code 11000)
    and returning (inserted_count, skipped_count).
    """
    if not operations:
        return 0, 0
    try:
        res = collection.bulk_write(operations, ordered=False)
        inserted = res.inserted_count
        skipped = len(operations) - inserted
        return inserted, skipped
    except BulkWriteError as bwe:
        inserted = bwe.details.get("nInserted", 0)
        write_errors = bwe.details.get("writeErrors", [])
        
        # Count write errors caused by existing unique index duplicates
        dup_errors = sum(1 for err in write_errors if err.get("code") == 11000)
        skipped = dup_errors
        
        # Re-raise if there are fatal errors other than duplicate key violations
        other_errors = [err for err in write_errors if err.get("code") != 11000]
        if other_errors:
            raise bwe
            
        return inserted, skipped

try:
    client.admin.command("ping")

    db = client["electricity_grid"]
    raw_collection = db["electricity_data"]
    clean_collection = db["electricity_clean"]

    # Create unique compound index to prevent duplicate records
    clean_collection.create_index(
        [
            ("collected_at", 1),
            ("state", 1)
        ],
        unique=True
    )

    raw_documents = 0
    total_state_records = 0
    missing_demand = 0
    invalid_demand = 0
    clean_records_inserted = 0
    duplicates_skipped = 0

    operations = []

    print("Starting optimized data cleaning...")

    cursor = raw_collection.find(
        {},
        {
            "collected_at": 1,
            "states": 1
        }
    ).batch_size(BATCH_SIZE)

    for document in cursor:
        raw_documents += 1

        collected_at = document.get("collected_at")
        states = document.get("states") or []

        for state in states:
            total_state_records += 1

            raw_demand = state.get("demand_mw")

            if raw_demand is None or raw_demand == "":
                missing_demand += 1
                continue

            demand = to_number(raw_demand)

            if demand is None:
                invalid_demand += 1
                continue

            clean_record = {
                "collected_at": collected_at,
                "state": state.get("state"),
                "demand_mw": demand,
                "peak_mw": to_number(state.get("peak_mw")),
                "frequency_hz": to_number(state.get("frequency_hz")),
                "source": state.get("source"),
                "source_type": state.get("source_type"),
                "source_kind": state.get("source_kind")
            }

            operations.append(InsertOne(clean_record))

            if len(operations) >= BATCH_SIZE:
                ins, skp = write_batch(clean_collection, operations)
                clean_records_inserted += ins
                duplicates_skipped += skp
                operations.clear()

        if raw_documents % 500 == 0:
            print(f"Processed {raw_documents} raw documents...")

    # Flush remaining operations
    if operations:
        ins, skp = write_batch(clean_collection, operations)
        clean_records_inserted += ins
        duplicates_skipped += skp
        operations.clear()

    print("\n========== DATA CLEANING REPORT ==========")
    print("Raw documents read       :", raw_documents)
    print("State records checked   :", total_state_records)
    print("Missing demand removed  :", missing_demand)
    print("Invalid demand removed  :", invalid_demand)
    print("Clean records inserted  :", clean_records_inserted)
    print("Duplicates skipped      :", duplicates_skipped)
    print(
        "Final clean records     :",
        clean_collection.count_documents({})
    )
    print("==========================================")

except PyMongoError as error:
    print(f"MongoDB operation failed: {error}", file=sys.stderr)
    sys.exit(1)

finally:
    client.close()