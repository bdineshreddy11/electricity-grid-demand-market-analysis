import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load API key
load_dotenv()
API_KEY = os.getenv("ENERGY_ATLAS_API_KEY")

# API
url = "https://api.energymap.in/developer/v1/grid/demand/latest"
headers = {
    "X-API-Key": API_KEY
}

# Raw data folder
raw_folder = r"D:\electricity-grid-demand-market-analysis\Data\Raw"

# Create folder if it does not exist
os.makedirs(raw_folder, exist_ok=True)

# API request
response = requests.get(url, headers=headers, timeout=90)

print("Status Code:", response.status_code)

if response.status_code == 200:

    raw_data = response.json()

    # Create timestamped raw JSON filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(
        raw_folder,
        f"grid_demand_raw_{timestamp}.json"
    )

    # Save completely unchanged API response
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(raw_data, file, indent=4)

    print("\nAPI connection successful!")
    print("Raw data saved successfully.")
    print("File:", file_path)

else:
    print("\nAPI request failed.")
    print(response.text)