"""
Inventory Service
-----------------
Owns the PostgreSQL database (no other service queries the DB directly).

Endpoints
  GET  /health, /pharmacies, /medicines, /stock?medicine=X   (read stock)
  POST /reserve     reserve a medicine at a pharmacy  (uses MUTUAL EXCLUSION)
  GET  /cluster     status of the coordinator cluster (leader, beacons, locks)

The background consumer that saves stock-update events lives in consumer.py.

--- Why /reserve needs a distributed lock --------------------------------------
Reserving is a READ-then-WRITE:   read quantity -> check enough -> write quantity-1
If two requests run this at the same time (on two different Inventory pods),
both can read "1 left", both succeed, and the last strip is sold twice.
That is a RACE CONDITION; the read-check-write is the CRITICAL SECTION.

To protect it, we ask the coordinator cluster for a lock named
"stock:<pharmacy>:<medicine>". Only one request in the whole system can hold it,
so only one runs the critical section at a time (mutual exclusion).

For the demo, ?use_lock=false turns the lock off so you can SEE the race
(overbooking). CRITICAL_SECTION_DELAY_MS adds a small pause inside the critical
section, like real processing time, which makes the race easy to reproduce.
"""
import os
import socket
import threading
import time
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
import redis
from fastapi import FastAPI, HTTPException, Query
from psycopg2.pool import ThreadedConnectionPool
from pydantic import BaseModel, Field

from coordination import CoordinatorClient, LockUnavailable

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "medifind")
DB_USER = os.getenv("DB_USER", "medifind")
DB_PASSWORD = os.getenv("DB_PASSWORD", "medifind")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CRITICAL_SECTION_DELAY_MS = int(os.getenv("CRITICAL_SECTION_DELAY_MS", "50"))
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "20"))

POD_NAME = socket.gethostname()  # in Kubernetes this is the pod name

app = FastAPI(title="MediFind - Inventory Service")
pool: ThreadedConnectionPool | None = None
pool_slots = threading.BoundedSemaphore(DB_POOL_SIZE)  # wait for a free connection instead of failing
cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_timeout=0.5, socket_connect_timeout=0.5)
coordinator = CoordinatorClient()


def connect_with_retry(retries: int = 15, delay: float = 2.0) -> ThreadedConnectionPool:
    """The DB may start after this service, so keep trying for a while."""
    for attempt in range(1, retries + 1):
        try:
            return ThreadedConnectionPool(
                1, DB_POOL_SIZE,
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASSWORD,
            )
        except psycopg2.OperationalError as e:
            print(f"[inventory] DB not ready (attempt {attempt}/{retries}): {e}", flush=True)
            time.sleep(delay)
    raise RuntimeError("Could not connect to PostgreSQL")


@contextmanager
def db_conn():
    """Borrow a connection from the (thread-safe) pool and always give it back."""
    with pool_slots:
        conn = pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            pool.putconn(conn)


def query(sql: str, params: tuple = ()) -> list[dict]:
    with db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if cur.description else []
    return [dict(r) for r in rows]


def invalidate_cache(medicine: str):
    """Same key format as the Search Service."""
    try:
        cache.delete(f"search:{medicine.strip().lower()}")
    except redis.RedisError:
        pass  # cached entries also expire on their own


@app.on_event("startup")
def startup():
    global pool
    pool = connect_with_retry()
    print(f"[inventory] connected to DB, running on {POD_NAME}", flush=True)


# ------------------------------------------------------------------ read API
@app.get("/health")
def health():
    try:
        query("SELECT 1")
        return {"status": "ok", "pod": POD_NAME}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB unavailable: {e}")


@app.get("/pharmacies")
def list_pharmacies():
    return query("SELECT id, name, area, lat, lng FROM pharmacies ORDER BY id")


@app.get("/medicines")
def list_medicines():
    rows = query("SELECT DISTINCT medicine_name FROM stock ORDER BY medicine_name")
    return [r["medicine_name"] for r in rows]


@app.get("/stock")
def pharmacies_with_stock(medicine: str = Query(..., min_length=2)):
    """All pharmacies that currently have this medicine (quantity > 0)."""
    rows = query(
        """
        SELECT p.id AS pharmacy_id, p.name AS pharmacy, p.area, p.lat, p.lng,
               s.medicine_name AS medicine, s.quantity, s.updated_at
        FROM stock s
        JOIN pharmacies p ON p.id = s.pharmacy_id
        WHERE LOWER(s.medicine_name) = LOWER(%s) AND s.quantity > 0
        """,
        (medicine.strip(),),
    )
    for r in rows:
        r["updated_at"] = r["updated_at"].isoformat()
    return {"medicine": medicine, "served_by": POD_NAME, "results": rows}


# ------------------------------------------------------------------ reservations
class ReservationRequest(BaseModel):
    pharmacy_id: int = Field(..., gt=0)
    medicine: str = Field(..., min_length=2, max_length=100)
    quantity: int = Field(1, ge=1, le=5)
    customer: str = Field("guest", max_length=50)


def reserve_critical_section(req: ReservationRequest) -> dict:
    """READ -> CHECK -> WRITE. Must never run concurrently for the same item."""
    with db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 1. READ current stock
            cur.execute(
                "SELECT medicine_name, quantity FROM stock "
                "WHERE pharmacy_id = %s AND LOWER(medicine_name) = LOWER(%s)",
                (req.pharmacy_id, req.medicine.strip()))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "This pharmacy does not stock that medicine")
            available = row["quantity"]

            # 2. CHECK
            if available < req.quantity:
                raise HTTPException(409, f"Not enough stock: only {available} left")

            # Simulated processing time (payment check, etc.). This is where
            # concurrent requests overlap if there is no lock.
            time.sleep(CRITICAL_SECTION_DELAY_MS / 1000)

            # 3. WRITE new stock + record the reservation
            remaining = available - req.quantity
            cur.execute(
                "UPDATE stock SET quantity = %s, updated_at = NOW() "
                "WHERE pharmacy_id = %s AND medicine_name = %s",
                (remaining, req.pharmacy_id, row["medicine_name"]))
            cur.execute(
                "INSERT INTO reservations (pharmacy_id, medicine_name, quantity, customer, reserved_by) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (req.pharmacy_id, row["medicine_name"], req.quantity, req.customer, POD_NAME))
            reservation_id = cur.fetchone()["id"]
    invalidate_cache(row["medicine_name"])
    return {"reservation_id": reservation_id, "medicine": row["medicine_name"],
            "pharmacy_id": req.pharmacy_id, "quantity": req.quantity, "remaining": remaining}


@app.post("/reserve")
def reserve(req: ReservationRequest, use_lock: bool = True):
    resource = f"stock:{req.pharmacy_id}:{req.medicine.strip().lower()}"
    if not use_lock:
        # DEMO ONLY: no mutual exclusion -> race conditions are possible
        result = reserve_critical_section(req)
        result.update(lock={"used": False}, served_by=POD_NAME)
        return result
    try:
        with coordinator.lock(resource) as held:
            result = reserve_critical_section(req)
    except LockUnavailable as e:
        raise HTTPException(503, f"Could not get lock: {e}")
    result.update(lock={"used": True, "resource": resource, "granted_by_node": held["leader_id"]},
                  served_by=POD_NAME)
    return result


@app.get("/cluster")
def cluster():
    """Live view of the coordinator cluster, shown in the frontend."""
    return {"nodes": coordinator.cluster_status()}
