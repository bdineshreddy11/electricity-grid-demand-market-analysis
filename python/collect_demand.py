import os
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
    raise ValueError("ENERGY_API_KEY is not set in .env")

if not MONGO_URI:
    raise ValueError("MONGO_URI is not set in .env")


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
        print("API request failed:", e)
        return

    api_response = response.json()

    if "data" not in api_response:
        print("Invalid API response.")
        return

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
        print("MongoDB connection failed:", e)
        return


    # -------------------------
    # 3. Collection timestamp
    # -------------------------
    collected_at = datetime.now(timezone.utc)


    # -------------------------
    # 4. Create document
    # -------------------------
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
    # 5. Insert snapshot
    # -------------------------
    try:
        result = collection.insert_one(document)

        print("Data collected successfully!")
        print("Collected at:", collected_at)
        print(
            "All India demand:",
            data.get("all_india", {}).get("demand_mw"),
            "MW"
        )
        print("MongoDB document ID:", result.inserted_id)

    except PyMongoError as e:
        print("MongoDB insert failed:", e)

    finally:
        client.close()


# -------------------------
# Run collector
# -------------------------
if __name__ == "__main__":
    collect_data()