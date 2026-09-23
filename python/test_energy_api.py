import os
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("ENERGY_API_KEY")

url = "https://api.energymap.in/developer/v1/grid/demand/latest"

headers = {
    "X-API-Key": api_key
}

response = requests.get(url, headers=headers, timeout=30)

print("Status code:", response.status_code)

if response.status_code == 200:
    data = response.json()

    print("API connection successful!")
    print(data)

else:
    print("API request failed")
    print(response.text)