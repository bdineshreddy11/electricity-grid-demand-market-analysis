import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, PyMongoError

# --------------------------------------------------
# 1. LOAD ENVIRONMENT VARIABLES
# --------------------------------------------------

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    raise ValueError("MONGO_URI is not set in .env")

DATABASE_NAME = "electricity_grid"
SOURCE_COLLECTION = "electricity_clean"
TARGET_COLLECTION = "electricity_normalized"

BATCH_SIZE = 2000  # Number of operations sent per network request


# --------------------------------------------------
# 2. DATA CONVERSION FUNCTIONS
# --------------------------------------------------

def to_number(value):
    """Convert numeric strings, doubles, or ints into clean float."""
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
    """Convert timestamp, ISO string, datetime, or MongoDB $date dict into UTC datetime."""
    if value is None or value == "":
        return None

    if isinstance(value, dict):
        if "$date" in value:
            value = value["$date"]
        else:
            return None

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

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d"
    ):
        try:
            result = datetime.strptime(str(value).strip(), fmt)
            if result.tzinfo is None:
                result = result.replace(tzinfo=timezone.utc)
            return result.astimezone(timezone.utc)
        except (ValueError, TypeError):
            continue

    return None


# --------------------------------------------------
# 3. PREPARE A NORMALIZED RECORD
# --------------------------------------------------

OPTIONAL_METADATA_FIELDS = [
    "api_as_of",
    "all_india",
    "iex_dam",
    "carbon_intensity",
    "coverage",
    "meta"
]


def normalize_record(state_record, parent_document=None):
    parent_document = parent_document or {}

    state_name = state_record.get("state")
    if not state_name:
        return None

    state_name = str(state_name).strip()

    collected_at = (
        state_record.get("collected_at")
        or parent_document.get("collected_at")
        or state_record.get("timestamp")
        or parent_document.get("timestamp")
    )

    normalized_collected_at = to_datetime(collected_at)
    if normalized_collected_at is None:
        return None

    demand_mw = to_number(state_record.get("demand_mw"))
    if demand_mw is None:
        return None

    source = (
        state_record.get("source")
        or parent_document.get("source")
        or "unknown"
    )

    normalized = {
        "collected_at": normalized_collected_at,
        "state": state_name,
        "demand_mw": demand_mw,
        "peak_mw": to_number(state_record.get("peak_mw")),
        "frequency_hz": to_number(state_record.get("frequency_hz")),
        "source": str(source).strip(),
        "source_type": state_record.get("source_type"),
        "source_kind": state_record.get("source_kind")
    }

    api_timestamp = state_record.get("timestamp")
    if api_timestamp:
        normalized_api_time = to_datetime(api_timestamp)
        if normalized_api_time:
            normalized["timestamp"] = normalized_api_time

    if state_record.get("_id") is not None:
        normalized["source_record_id"] = str(state_record["_id"])
    elif parent_document.get("_id") is not None:
        normalized["source_record_id"] = str(parent_document["_id"])

    return normalized


# --------------------------------------------------
# 4. EXTRACT RECORDS
# --------------------------------------------------

def extract_records(document):
    states = document.get("states")

    if isinstance(states, list):
        for state_record in states:
            if isinstance(state_record, dict):
                yield normalize_record(state_record, document)
    else:
        yield normalize_record(document)


# --------------------------------------------------
# 5. FAST BATCH EXECUTION
# --------------------------------------------------

def main():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)

    try:
        client.admin.command("ping")

        db = client[DATABASE_NAME]
        source_collection = db[SOURCE_COLLECTION]
        target_collection = db[TARGET_COLLECTION]

        target_collection.create_index(
            [
                ("state", ASCENDING),
                ("collected_at", ASCENDING),
                ("source", ASCENDING)
            ],
            unique=True,
            name="unique_state_collection_time_source"
        )

        total_source_docs = source_collection.count_documents({})
        print(f"\nStarting batch normalization...")
        print(f"Total documents to process: {total_source_docs:,}")
        print(f"Batch size: {BATCH_SIZE} operations\n")

        total_read = 0
        total_normalized = 0
        total_upserted = 0
        total_modified = 0

        operations = []

        # Cursor with batch_size to keep streaming fast
        cursor = source_collection.find({}, batch_size=500)

        for document in cursor:
            total_read += 1

            for record in extract_records(document):
                if record is None:
                    continue

                total_normalized += 1

                filter_query = {
                    "state": record["state"],
                    "collected_at": record["collected_at"],
                    "source": record["source"]
                }

                operations.append(
                    UpdateOne(filter_query, {"$set": record}, upsert=True)
                )

                # Flush batch to MongoDB when limit reached
                if len(operations) >= BATCH_SIZE:
                    try:
                        res = target_collection.bulk_write(operations, ordered=False)
                        total_upserted += res.upserted_count
                        total_modified += res.modified_count
                    except BulkWriteError as bwe:
                        total_upserted += bwe.details.get("nUpserted", 0)
                        total_modified += bwe.details.get("nModified", 0)
                    operations.clear()

            # Live progress log every 1,000 documents
            if total_read % 1000 == 0:
                print(f"Processed {total_read:,}/{total_source_docs:,} documents | Normalized: {total_normalized:,} records")

        # Flush any remaining records
        if operations:
            try:
                res = target_collection.bulk_write(operations, ordered=False)
                total_upserted += res.upserted_count
                total_modified += res.modified_count
            except BulkWriteError as bwe:
                total_upserted += bwe.details.get("nUpserted", 0)
                total_modified += bwe.details.get("nModified", 0)
            operations.clear()

        print("\n================ NORMALIZATION COMPLETED ================")
        print(f"Total clean docs read       : {total_read:,}")
        print(f"Total state records built   : {total_normalized:,}")
        print(f"New records inserted (upsert): {total_upserted:,}")
        print(f"Existing records updated    : {total_modified:,}")
        print(f"Final collection count      : {target_collection.count_documents({}):,}")
        print("=========================================================")

    except PyMongoError as error:
        print("MongoDB error encountered:", error)
        raise

    finally:
        client.close()


if __name__ == "__main__":
    main()