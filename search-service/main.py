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
import itertools
import json
import math
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

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


# ---------------------------------------------------------------- pod registry
# Every Search pod writes a heartbeat into a shared Redis hash every 2 s and
# counts the searches it answers in 5-second buckets. GET /pods reads them back,
# so the System view can show which pods are alive (autoscaling) and how the
# requests are spread across them (load balancing).
PODS_KEY = "search:pods"
BUCKET_S = 5          # count requests in 5-second buckets
WINDOW_S = 60         # show requests from the last minute
POD_TIMEOUT_S = 8     # no heartbeat for 8 s = pod is gone


def heartbeat_loop():
    while True:
        try:
            cache.hset(PODS_KEY, POD_NAME, time.time())
        except redis.RedisError:
            pass
        time.sleep(2)


def count_request():
    key = f"search:served:{POD_NAME}:{int(time.time() // BUCKET_S)}"
    try:
        pipe = cache.pipeline()
        pipe.incr(key)
        pipe.expire(key, WINDOW_S + BUCKET_S * 2)
        pipe.execute()
    except redis.RedisError:
        pass


threading.Thread(target=heartbeat_loop, daemon=True).start()


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


@app.get("/pods")
def pods():
    """Search pods that are alive right now, with the searches each answered in the last minute."""
    now = time.time()
    try:
        beats = {k.decode(): float(v) for k, v in cache.hgetall(PODS_KEY).items()}
    except redis.RedisError:
        raise HTTPException(status_code=503, detail="Redis unavailable, pod list unknown")
    current = int(now // BUCKET_S)
    buckets = range(current - WINDOW_S // BUCKET_S + 1, current + 1)
    alive = []
    for pod, last in sorted(beats.items()):
        if now - last > POD_TIMEOUT_S:
            try:
                cache.hdel(PODS_KEY, pod)        # forget pods that stopped sending heartbeats
            except redis.RedisError:
                pass
            continue
        try:
            counts = cache.mget([f"search:served:{pod}:{b}" for b in buckets])
        except redis.RedisError:
            counts = []
        alive.append({"pod": pod, "searches_last_minute": sum(int(c) for c in counts if c),
                      "last_heartbeat_s": round(now - last, 1)})
    return {"pods": alive, "window_s": WINDOW_S, "asked": POD_NAME}


@app.get("/pharmacies")
def pharmacies():
    return call_inventory("/pharmacies")


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


def lookup_stock(medicine: str) -> tuple[dict, str]:
    """Pharmacies that have this medicine: from Redis if cached, otherwise from Inventory."""
    key = cache_key(medicine)
    cached, redis_ok = cache_get(key)
    if cached is not None:
        return cached, "cache"
    data = call_inventory("/stock", {"medicine": medicine})
    cache_set(key, data)
    return data, ("db" if redis_ok else "db (cache unavailable)")


@app.get("/search")
def search(
    medicine: str = Query(..., min_length=2),
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    limit: int = Query(10, ge=1, le=50),
):
    count_request()
    data, source = lookup_stock(medicine)

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


# ------------------------------------------------------------------ prescription search
# SCATTER-GATHER: a prescription has several medicines. Instead of looking them
# up one after another, we send all the lookups at the same time (scatter),
# wait for every answer, then merge them into one result (gather). The total
# time is roughly the SLOWEST lookup, not the SUM of all of them.
MAX_MEDICINES = 8


def route_km(lat: float, lng: float, stops: list[dict]) -> float:
    """Distance of going from the user to each stop in order."""
    total, here = 0.0, (lat, lng)
    for s in stops:
        total += distance_km(here[0], here[1], s["lat"], s["lng"])
        here = (s["lat"], s["lng"])
    return round(total, 2)


def fewest_stops(wanted: list[str], pharmacies: list[dict], lat: float, lng: float) -> dict:
    """No single pharmacy has everything: find the shortest trip with as few stops as possible."""
    need = set(wanted)
    best = None
    # Two stops is the usual answer, so try every pair and both visiting orders.
    for a, b in itertools.combinations(pharmacies, 2):
        if need <= set(a["has"]) | set(b["has"]):
            for order in ((a, b), (b, a)):
                km = route_km(lat, lng, list(order))
                if best is None or km < best[0]:
                    best = (km, list(order))
    if best:
        stops = best[1]
    else:
        # Rare: needs 3 or more stops. Greedy: keep picking the pharmacy that
        # covers the most of what's still missing (nearest first on ties).
        stops, left, here = [], set(need), (lat, lng)
        while left:
            pick = max(pharmacies, key=lambda p: (len(left & set(p["has"])),
                                                 -distance_km(here[0], here[1], p["lat"], p["lng"])))
            if not left & set(pick["has"]):
                break
            stops.append(pick)
            left -= set(pick["has"])
            here = (pick["lat"], pick["lng"])
    # Say which medicines to buy at which stop (each medicine at its first stop that has it).
    plan, left = [], list(wanted)
    for stop in stops:
        here_items = [m for m in left if m in stop["has"]]
        left = [m for m in left if m not in here_items]
        plan.append({**{k: stop[k] for k in ("pharmacy_id", "pharmacy", "area", "lat", "lng", "distance_km")},
                     "buy": [{"medicine": stop["names"][m], "quantity": stop["has"][m]} for m in here_items]})
    return {"stops": plan, "route_km": route_km(lat, lng, stops)}


@app.get("/prescription")
def prescription(
    medicines: str = Query(..., min_length=2, description="comma-separated medicine names"),
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
):
    names, seen = [], set()
    for raw in medicines.split(","):
        name = raw.strip()
        if len(name) >= 2 and name.lower() not in seen:
            seen.add(name.lower())
            names.append(name)
    if not names:
        raise HTTPException(422, "Add at least one medicine")
    if len(names) > MAX_MEDICINES:
        raise HTTPException(422, f"At most {MAX_MEDICINES} medicines at a time")
    count_request()

    def timed_lookup(name: str) -> dict:
        t0 = time.perf_counter()
        data, source = lookup_stock(name)
        return {"medicine": name, "data": data, "source": source,
                "ms": round((time.perf_counter() - t0) * 1000, 1)}

    # SCATTER: one lookup per medicine, all at once
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=len(names)) as workers:
        answers = list(workers.map(timed_lookup, names))
    parallel_ms = round((time.perf_counter() - started) * 1000, 1)

    # GATHER: merge everything into one view per pharmacy
    pharmacies: dict[int, dict] = {}
    for ans in answers:
        key = ans["medicine"].lower()
        for r in ans["data"]["results"]:
            p = pharmacies.setdefault(r["pharmacy_id"], {
                "pharmacy_id": r["pharmacy_id"], "pharmacy": r["pharmacy"], "area": r["area"],
                "lat": r["lat"], "lng": r["lng"], "distance_km": distance_km(lat, lng, r["lat"], r["lng"]),
                "has": {}, "names": {}})
            p["has"][key] = r["quantity"]
            p["names"][key] = r["medicine"]           # the spelling stored in the database

    wanted = [a["medicine"].lower() for a in answers if a["data"]["results"]]
    missing = [a["medicine"] for a in answers if not a["data"]["results"]]
    everyone = sorted(pharmacies.values(), key=lambda p: p["distance_km"])

    def public(p: dict) -> dict:
        return {**{k: p[k] for k in ("pharmacy_id", "pharmacy", "area", "lat", "lng", "distance_km")},
                "items": [{"medicine": p["names"][m], "quantity": p["has"][m]} for m in wanted if m in p["has"]]}

    all_in_one = [public(p) for p in everyone if wanted and set(wanted) <= set(p["has"])][:5]
    plan = fewest_stops(wanted, everyone, lat, lng) if wanted and not all_in_one else None

    return {
        "medicines": names,
        "lookups": [{"medicine": a["medicine"], "source": a["source"], "ms": a["ms"],
                     "pharmacies": len(a["data"]["results"])} for a in answers],
        "parallel_ms": parallel_ms,
        "one_by_one_ms": round(sum(a["ms"] for a in answers), 1),
        "all_in_one": all_in_one,
        "plan": plan,
        "missing": missing,
        "served_by": POD_NAME,
    }
