import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error as MySQLError
from pymongo import MongoClient

# --------------------------------------------------
# 1. LOAD CONFIGURATION
# --------------------------------------------------
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = "electricity_grid"
MONGO_COLLECTION = "electricity_normalized"

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "electricity_grid")

BATCH_SIZE = 5000

if not MONGO_URI:
    raise ValueError("MONGO_URI is missing from .env")
if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD is missing from .env")


# --------------------------------------------------
# 2. DATA FORMATTING HELPER
# --------------------------------------------------
def sanitize_datetime(val):
    """Ensure value converts cleanly to MySQL DATETIME format."""
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    return None


# --------------------------------------------------
# 3. EXTRACTION AND INGESTION
# --------------------------------------------------
def sync_mongo_to_mysql():
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
    mysql_conn = None

    try:
        # Check Mongo connection
        mongo_client.admin.command("ping")
        mongo_db = mongo_client[MONGO_DATABASE]
        mongo_col = mongo_db[MONGO_COLLECTION]

        # Connect to MySQL
        mysql_conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = mysql_conn.cursor()

        insert_sql = """
            INSERT INTO electricity_normalized (
                collected_at, state, demand_mw, peak_mw, frequency_hz,
                source, source_type, source_kind, timestamp, source_record_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                demand_mw = VALUES(demand_mw),
                peak_mw = VALUES(peak_mw),
                frequency_hz = VALUES(frequency_hz),
                source_type = VALUES(source_type),
                source_kind = VALUES(source_kind);
        """

        total_docs = mongo_col.count_documents({})
        print(f"Transferring {total_docs:,} records from MongoDB to MySQL...")

        records_batch = []
        total_synced = 0

        cursor_mongo = mongo_col.find({}, batch_size=2000)

        for doc in cursor_mongo:
            row = (
                sanitize_datetime(doc.get("collected_at")),
                doc.get("state"),
                doc.get("demand_mw"),
                doc.get("peak_mw"),
                doc.get("frequency_hz"),
                doc.get("source"),
                doc.get("source_type"),
                doc.get("source_kind"),
                sanitize_datetime(doc.get("timestamp")),
                doc.get("source_record_id")
            )
            records_batch.append(row)

            if len(records_batch) >= BATCH_SIZE:
                cursor.executemany(insert_sql, records_batch)
                mysql_conn.commit()
                total_synced += len(records_batch)
                print(f"Synced {total_synced:,}/{total_docs:,} rows...")
                records_batch.clear()

        # Flush remaining records
        if records_batch:
            cursor.executemany(insert_sql, records_batch)
            mysql_conn.commit()
            total_synced += len(records_batch)

        print("\n================ DATA SYNC COMPLETE ================")
        print(f"Total rows processed and verified in MySQL: {total_synced:,}")
        print("=====================================================")

    except MySQLError as err:
        print("MySQL Error:", err)
        if mysql_conn:
            mysql_conn.rollback()
        raise
    except Exception as err:
        print("Sync Error:", err)
        raise
    finally:
        if mysql_conn and mysql_conn.is_connected():
            cursor.close()
            mysql_conn.close()
        mongo_client.close()


if __name__ == "__main__":
    sync_mongo_to_mysql()