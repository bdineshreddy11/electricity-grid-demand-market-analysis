
import os
from math import isfinite

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError


# Load environment variables
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    raise ValueError("MONGO_URI is not set")


# MongoDB connection
client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=10000
)

try:
    client.admin.command("ping")

    db = client["electricity_grid"]
    raw_collection = db["electricity_data"]
    clean_collection = db["electricity_clean"]

    BATCH_SIZE = 20

    def to_number(value):
        try:
            if value is None or value == "":
                return None

            number = float(value)

            if not isfinite(number):
                return None

            return number

        except (ValueError, TypeError):
            return None

    # Counters
    raw_documents = 0
    total_state_records = 0
    missing_demand = 0
    invalid_demand = 0
    duplicates_skipped = 0
    clean_records_inserted = 0

    # Read raw documents in small batches
    last_id = None

    print("Starting MongoDB data cleaning...")

    while True:
        query = (
            {}
            if last_id is None
            else {"_id": {"$gt": last_id}}
        )

        batch = list(
            raw_collection.find(query)
            .sort("_id", 1)
            .limit(BATCH_SIZE)
        )

        if not batch:
            break

        # Save the final ID before processing the batch
        last_id = batch[-1]["_id"]

        for document in batch:
            raw_documents += 1

            collected_at = document.get("collected_at")
            states = document.get("states") or []

            for state in states:
                total_state_records += 1

                state_name = state.get("state")
                raw_demand = state.get("demand_mw")

                # Check missing demand
                if raw_demand is None or raw_demand == "":
                    missing_demand += 1
                    continue

                # Convert demand to a valid number
                demand = to_number(raw_demand)

                if demand is None:
                    invalid_demand += 1
                    continue

                # Create clean record
                clean_record = {
                    "collected_at": collected_at,
                    "state": state_name,
                    "demand_mw": demand,
                    "peak_mw": to_number(
                        state.get("peak_mw")
                    ),
                    "frequency_hz": to_number(
                        state.get("frequency_hz")
                    ),
                    "source": state.get("source"),
                    "source_type": state.get("source_type"),
                    "source_kind": state.get("source_kind")
                }

                try:
                    clean_collection.insert_one(clean_record)
                    clean_records_inserted += 1

                except DuplicateKeyError:
                    duplicates_skipped += 1

        print(
            f"Processed {raw_documents} raw documents..."
        )

    # Final report
    final_clean_count = clean_collection.count_documents({})

    print("\n==============================")
    print("     DATA CLEANING REPORT")
    print("==============================")
    print("Raw documents read       :", raw_documents)
    print("Total state records      :", total_state_records)
    print("Missing demand removed   :", missing_demand)
    print("Invalid demand removed   :", invalid_demand)
    print("Duplicates skipped       :", duplicates_skipped)
    print("Clean records inserted   :", clean_records_inserted)
    print("Final clean records      :", final_clean_count)
    print("==============================")

except PyMongoError as error:
    print("MongoDB operation failed:", error)
    raise

finally:
    client.close()
