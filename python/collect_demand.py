import os
import sys
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError


# Load environment variables
load_dotenv()

API_KEY = os.getenv("ENERGY_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")

API_URL = "https://api.energymap.in/developer/v1/grid/demand/latest"


# Check required environment variables
if not API_KEY:
    raise ValueError("ENERGY_API_KEY is not set in environment or .env")

if not MONGO_URI:
    raise ValueError("MONGO_URI is not set in environment or .env")


def collect_data():

    # -------------------------
    # 1. Get data from API
    # -------------------------
    print("Fetching live electricity data...")

    headers = {
        "X-API-Key": API_KEY
    }

    try:
        response = requests.get(
            API_URL,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()

    except requests.RequestException as e:
        print(f"API request failed: {e}", file=sys.stderr)
        sys.exit(1)

    api_response = response.json()

    if "data" not in api_response:
        print("Invalid API response: 'data' key missing.", file=sys.stderr)
        sys.exit(1)

    data = api_response["data"]

    # -------------------------
    # 2. Connect to MongoDB
    # -------------------------
    print("Connecting to MongoDB...")

    try:
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=10000
        )
        client.admin.command("ping")

        db = client["electricity_grid"]
        collection = db["electricity_data"]

    except PyMongoError as e:
        print(f"MongoDB connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    # -------------------------
    # 3. Collection timestamp & document
    # -------------------------
    collected_at = datetime.now(timezone.utc)

    document = {
        "collected_at": collected_at,
        "api_as_of": data.get("as_of"),
        "all_india": data.get("all_india"),
        "iex_dam": data.get("iex_dam"),
        "carbon_intensity": data.get("carbon_intensity"),
        "coverage": data.get("coverage"),
        "states": data.get("items"),
        "meta": api_response.get("meta")
    }

    # -------------------------
    # 4. Insert snapshot
    # -------------------------
    try:
        result = collection.insert_one(document)

        print("Data collected successfully!")
        print("Collected at:", collected_at)
        print(
            "All India demand:",
            (data.get("all_india") or {}).get("demand_mw"),
            "MW"
        )
        print("MongoDB document ID:", result.inserted_id)

    except PyMongoError as e:
        print(f"MongoDB insert failed: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        client.close()


if __name__ == "__main__":
    collect_data()
