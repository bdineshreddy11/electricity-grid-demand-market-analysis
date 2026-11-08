
import os
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient
import mysql.connector


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

BATCH_SIZE = 500


# ==========================================
# CONVERT MONGODB VALUES
# ==========================================

def convert_text(value):
    if value is None:
        return None

    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)

    return str(value).strip()


def convert_number(value):
    if value is None:
        return None

    if isinstance(value, dict):
        for key in (
            "$numberDouble",
            "$numberInt",
            "$numberLong",
            "$numberDecimal"
        ):
            if key in value:
                value = value[key]
                break
        else:
            raise ValueError(
                f"Unexpected numeric structure: {value}"
            )

    if isinstance(value, (list, bool, dict)):
        raise ValueError(
            f"Invalid numeric value: {value}"
        )

    return float(value)


def convert_timestamp(value):
    if value is None:
        return None

    if isinstance(value, dict):

        if "$date" in value:
            value = value["$date"]

        elif "date" in value:
            value = value["date"]

        elif "timestamp" in value:
            value = value["timestamp"]

        elif "value" in value:
            value = value["value"]

        else:
            raise ValueError(
                f"Unrecognized timestamp: {value}"
            )

        if isinstance(value, dict):

            if "$numberLong" in value:
                value = int(value["$numberLong"])

            elif "$numberDouble" in value:
                value = float(value["$numberDouble"])

            else:
                raise ValueError(
                    f"Invalid date structure: {value}"
                )

    if isinstance(value, (int, float)):
        value = datetime.fromtimestamp(
            value / 1000,
            tz=timezone.utc
        )

    elif isinstance(value, str):
        value = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

    if not isinstance(value, datetime):
        raise ValueError(
            f"Invalid timestamp: {value!r}"
        )

    if value.tzinfo is not None:
        value = value.astimezone(
            timezone.utc
        ).replace(tzinfo=None)

    return value


# ==========================================
# MAIN SYNCHRONIZATION
# ==========================================

mongo_client = None
mysql_connection = None
mysql_cursor = None

try:

    if not MONGO_URI:
        raise ValueError(
            "MONGO_URI is missing in .env"
        )

    mongo_client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000
    )

    mongo_client.admin.command("ping")

    mongo_db = mongo_client["electricity_grid"]

    mongo_collection = mongo_db["electricity_clean"]

    mysql_connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )

    mysql_cursor = mysql_connection.cursor()

    print("\n================================")
    print(" MONGODB TO MYSQL SYNCHRONIZATION")
    print("================================\n")

    total_found = mongo_collection.count_documents({})

    print("MongoDB clean records:", total_found)

    insert_query = """
        INSERT INTO grid_demand
        (
            state,
            demand_mw,
            peak_mw,
            frequency_hz,
            source,
            source_type,
            source_kind,
            collected_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    check_query = """
        SELECT id
        FROM grid_demand
        WHERE state = %s
          AND collected_at = %s
        LIMIT 1
    """

    inserted = 0
    skipped = 0
    failed = 0

    error_samples = []

    batch_count = 0

    # Use a cursor instead of loading all records into memory.
    for record in mongo_collection.find(
        {},
        batch_size=BATCH_SIZE
    ):

        try:

            state = convert_text(
                record.get("state")
            )

            collected_at = convert_timestamp(
                record.get("collected_at")
            )

            # Required field validation.
            if not state or collected_at is None:
                skipped += 1

                if len(error_samples) < 10:
                    error_samples.append(
                        f"Skipped MongoDB ID {record.get('_id')}: "
                        "Missing state or collected_at"
                    )

                continue

            values = (
                state,
                convert_number(record.get("demand_mw")),
                convert_number(record.get("peak_mw")),
                convert_number(record.get("frequency_hz")),
                convert_text(record.get("source")),
                convert_text(record.get("source_type")),
                convert_text(record.get("source_kind")),
                collected_at
            )

            # Check whether record already exists.
            mysql_cursor.execute(
                check_query,
                (state, collected_at)
            )

            if mysql_cursor.fetchone():
                skipped += 1
                continue

            # Insert new record.
            mysql_cursor.execute(
                insert_query,
                values
            )

            inserted += 1
            batch_count += 1

            # Commit every 500 inserted records.
            if batch_count >= BATCH_SIZE:
                mysql_connection.commit()
                batch_count = 0

        except Exception as error:

            failed += 1

            if len(error_samples) < 10:
                error_samples.append(
                    f"MongoDB ID {record.get('_id')}: {error}"
                )

    # Commit remaining records.
    mysql_connection.commit()

    # Get final MySQL count.
    mysql_cursor.execute(
        "SELECT COUNT(*) FROM grid_demand"
    )

    mysql_total = mysql_cursor.fetchone()[0]

    print("\n================================")
    print("          SYNC REPORT")
    print("================================")

    print("MongoDB records found :", total_found)
    print("New records inserted  :", inserted)
    print("Records skipped       :", skipped)
    print("Failed records        :", failed)
    print("MySQL total records   :", mysql_total)

    if error_samples:

        print("\nRecords requiring investigation:")

        for error in error_samples:
            print("-", error)

    print("\n================================")
    print(" SYNCHRONIZATION COMPLETED")
    print("================================\n")


except Exception:

    if (
        mysql_connection is not None
        and mysql_connection.is_connected()
    ):
        mysql_connection.rollback()

    raise


finally:

    if mysql_cursor is not None:
        mysql_cursor.close()

    if (
        mysql_connection is not None
        and mysql_connection.is_connected()
    ):
        mysql_connection.close()

    if mongo_client is not None:
        mongo_client.close()
