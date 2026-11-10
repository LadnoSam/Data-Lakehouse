import time
import os
import requests
from pymongo import MongoClient
from datetime import datetime, timezone

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))  

CG_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=ethereum&vs_currencies=usd"
    "&include_24hr_change=true"
    "&include_market_cap=true"
    "&include_24hr_vol=true"
)

client = MongoClient(MONGO_URL)
db = client["crypto"]
collection = db["ethereum_prices"]

def fetch_and_store():
    try:
        resp = requests.get(CG_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()["ethereum"]

        doc = {
            "timestamp": datetime.now(timezone.utc),
            "price_usd": data["usd"],
            "change_24h": data.get("usd_24h_change"),
            "market_cap": data.get("usd_market_cap"),
            "volume_24h": data.get("usd_24h_vol"),
        }

        collection.insert_one(doc)
        print(f"[{doc['timestamp']}] Inserted: {doc}")

    except Exception as e:
        print(f"Error fetching/storing data: {e}")


if __name__ == "__main__":
    while True:
        fetch_and_store()
        time.sleep(POLL_INTERVAL)
