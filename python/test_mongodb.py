
from pymongo import MongoClient

MONGO_URI = "mongodb+srv://electricity_admin:electricity_admin123@electricity-grid-cluste.wgrtqh4.mongodb.net/?appName=electricity-grid-cluster"

client = MongoClient(MONGO_URI)

db = client["electricity_grid"]
collection = db["electricity_data"]

test_record = {
    "source": "test",
    "status": "connected",
    "message": "MongoDB Atlas connection successful"
}

result = collection.insert_one(test_record)

print("MongoDB connection successful!")
print("Inserted document ID:", result.inserted_id)

client.close()