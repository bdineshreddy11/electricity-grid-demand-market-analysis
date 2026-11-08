
import os
from math import isfinite

from dotenv import load_dotenv
from pymongo import MongoClient, InsertOne
from pymongo.errors import PyMongoError

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    raise ValueError("MONGO_URI is not set")

BATCH_SIZE = 1000

client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=10000
)

try:
    client.admin.command("ping")

    db = client["electricity_grid"]
    raw_collection = db["electricity_data"]
    clean_collection = db["electricity_clean"]

    # Create a unique index to prevent duplicate clean records.
    clean_collection.create_index(
        [
            ("collected_at", 1),
            ("state", 1)
        ],
        unique=True
    )

    def to_number(value):
        try:
            if value is None or value == "":
                return None

            number = float(value)

            return number if isfinite(number) else None

        except (ValueError, TypeError):
            return None

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
                "frequency_hz": to_number(
                    state.get("frequency_hz")
                ),
                "source": state.get("source"),
                "source_type": state.get("source_type"),
                "source_kind": state.get("source_kind")
            }

            # Skip records already present in the clean collection.
            operations.append(
                InsertOne(clean_record)
            )

            if len(operations) >= BATCH_SIZE:
                result = clean_collection.bulk_write(
                    operations,
                    ordered=False
                )

                clean_records_inserted += result.inserted_count
                duplicates_skipped += (
                    len(operations) - result.inserted_count
                )

                operations.clear()

        if raw_documents % 500 == 0:
            print(f"Processed {raw_documents} raw documents...")

    # Insert the remaining records.
    if operations:
        result = clean_collection.bulk_write(
            operations,
            ordered=False
        )

        clean_records_inserted += result.inserted_count
        duplicates_skipped += (
            len(operations) - result.inserted_count
        )

    print("\n========== DATA CLEANING REPORT ==========")
    print("Raw documents read      :", raw_documents)
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
    print("MongoDB operation failed:", error)
    raise

finally:
    client.close()
