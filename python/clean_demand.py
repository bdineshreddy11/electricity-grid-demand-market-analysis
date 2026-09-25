import os
from pymongo import MongoClient
from dotenv import load_dotenv
from pymongo.errors import DuplicateKeyError

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI)
db = client["electricity_grid"]

raw_collection = db["electricity_data"]
clean_collection = db["electricity_clean"]


def to_number(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


# Counters
raw_documents = 0
total_state_records = 0
missing_demand = 0
invalid_demand = 0
duplicates_skipped = 0
clean_records_inserted = 0


documents = raw_collection.find()


for document in documents:

    raw_documents += 1

    collected_at = document.get("collected_at")

    states = document.get("states", [])

    for state in states:

        total_state_records += 1

        state_name = state.get("state")

        raw_demand = state.get("demand_mw")

        # 1. Missing demand
        if raw_demand is None or raw_demand == "":
            missing_demand += 1
            continue

        # 2. Convert demand to number
        demand = to_number(raw_demand)

        # 3. Invalid demand
        if demand is None:
            invalid_demand += 1
            continue

        # 4. Create clean record
        clean_record = {
            "collected_at": collected_at,
            "state": state_name,
            "demand_mw": demand,
            "peak_mw": to_number(state.get("peak_mw")),
            "frequency_hz": to_number(state.get("frequency_hz")),
            "source": state.get("source"),
            "source_type": state.get("source_type"),
            "source_kind": state.get("source_kind")
        }

        # 5. Insert into clean collection
        try:

            clean_collection.insert_one(clean_record)

            clean_records_inserted += 1

        except DuplicateKeyError:

            duplicates_skipped += 1


# Final clean count
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


client.close()