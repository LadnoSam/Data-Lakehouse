from fastapi import FastAPI, Query
from pymongo import MongoClient
import os
from datetime import datetime, timezone

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")

app = FastAPI(title="ETH Data API")

client = MongoClient(MONGO_URL)
db = client["crypto"]
collection = db["ethereum_prices"]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/eth/latest")
def latest():
    doc = collection.find_one(sort=[("timestamp", -1)])
    if not doc:
        return {}
    doc["_id"] = str(doc["_id"])
    doc["timestamp"] = doc["timestamp"].isoformat()
    return doc


@app.get("/eth/history")
def history(limit: int = Query(default=200, le=2000)):
    docs = list(
        collection.find().sort("timestamp", -1).limit(limit)
    )
    docs.reverse()  
    result = []
    for d in docs:
        result.append({
            "timestamp": d["timestamp"].isoformat(),
            "price_usd": d["price_usd"],
            "change_24h": d.get("change_24h"),
            "market_cap": d.get("market_cap"),
            "volume_24h": d.get("volume_24h"),
        })
    return result
