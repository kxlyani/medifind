"""
Search Service  (public API, horizontally scalable)
---------------------------------------------------
Users call this. It is STATELESS: it keeps no data of its own, so Kubernetes
can run 2, 5 or 10 identical copies behind one Service (load balancer) and
add more automatically when CPU usage is high (HPA).

Request flow for GET /search:
  1. Look in Redis for "search:<medicine>"        -> cache HIT  (fast)
  2. If missing, ask Inventory Service over REST   -> cache MISS
     and store the answer in Redis for CACHE_TTL seconds.
  3. Compute distance from the user to each pharmacy and sort nearest-first.

The cache stores results per MEDICINE, not per user location, so every user
searching "Dolo 650" shares one cache entry, no matter where they are.

Distributed Systems ideas shown here:
  * Synchronous inter-service communication (REST to Inventory)
  * Service discovery by name ("inventory-service", "redis")
  * Caching + eventual consistency (stale for at most CACHE_TTL seconds,
    usually less because the consumer deletes the entry on every update)
  * Graceful degradation: if Redis is down we still answer from Inventory
  * Load balancing: "served_by" shows which replica answered
"""
import json
import math
import os
import socket

import httpx
import redis
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

INVENTORY_URL = os.getenv("INVENTORY_URL", "http://inventory-service:8001")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "30"))  # seconds
POD_NAME = socket.gethostname()

app = FastAPI(title="MediFind - Search Service")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

client = httpx.Client(base_url=INVENTORY_URL, timeout=3.0)
cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_timeout=0.5, socket_connect_timeout=0.5)


def cache_key(medicine: str) -> str:
    """Must match the key format used by the Inventory Consumer."""
    return f"search:{medicine.strip().lower()}"


def distance_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Haversine formula: straight-line distance between two points on Earth."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return round(2 * r * math.asin(math.sqrt(a)), 2)


def call_inventory(path: str, params: dict | None = None):
    try:
        resp = client.get(path, params=params)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError as e:
        # Inventory being down must not crash the Search Service.
        raise HTTPException(status_code=503, detail=f"Inventory service unavailable: {e}")


def cache_get(key: str):
    try:
        raw = cache.get(key)
        return (json.loads(raw) if raw else None), True
    except redis.RedisError:
        return None, False  # Redis down -> behave as a miss


def cache_set(key: str, value) -> None:
    try:
        cache.setex(key, CACHE_TTL, json.dumps(value))
    except redis.RedisError:
        pass


@app.get("/health")
def health():
    return {"status": "ok", "pod": POD_NAME}


@app.get("/medicines")
def medicines():
    return call_inventory("/medicines")


@app.get("/areas")
def areas():
    """Distinct pharmacy areas with coordinates, used for the frontend's location dropdown."""
    seen, out = set(), []
    for p in call_inventory("/pharmacies"):
        if p["area"] not in seen:
            seen.add(p["area"])
            out.append({"area": p["area"], "lat": p["lat"], "lng": p["lng"]})
    return sorted(out, key=lambda a: a["area"])


@app.get("/search")
def search(
    medicine: str = Query(..., min_length=2),
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    limit: int = Query(10, ge=1, le=50),
):
    key = cache_key(medicine)
    cached, redis_ok = cache_get(key)

    if cached is not None:
        source, data = "cache", cached
    else:
        data = call_inventory("/stock", {"medicine": medicine})
        cache_set(key, data)
        source = "db" if redis_ok else "db (cache unavailable)"

    results = []
    for r in data["results"]:
        item = dict(r)
        item["distance_km"] = distance_km(lat, lng, r["lat"], r["lng"])
        results.append(item)
    results.sort(key=lambda r: r["distance_km"])

    return {
        "medicine": medicine,
        "count": len(results),
        "source": source,                     # "cache" or "db"
        "served_by": POD_NAME,                # which Search replica answered
        "inventory_pod": data.get("served_by"),
        "results": results[:limit],
    }
