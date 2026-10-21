"""
Inventory Service
-----------------
Owns the PostgreSQL database (no other service queries the DB directly).

Endpoints
  GET  /health, /pharmacies, /medicines, /stock?medicine=X   (read stock)
  POST /reserve     reserve a medicine at a pharmacy  (uses MUTUAL EXCLUSION)
  GET  /cluster     status of the coordinator cluster (leader, beacons, locks)
  GET  /activity    live feed of what the system just did (stock updates, reservations)
  GET  /pharmacies/{id}/stock          every medicine at one pharmacy (pharmacy portal)
  GET  /pharmacies/{id}/reservations   recent reservations at one pharmacy

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

import activity
import events
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
RESERVATION_MINUTES = int(os.getenv("RESERVATION_MINUTES", "30"))   # how long a strip is held

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


MIGRATIONS = [
    # Added for "My reservations": who reserved, its state, and when the hold ends.
    "ALTER TABLE reservations ADD COLUMN IF NOT EXISTS user_id VARCHAR(40)",
    "ALTER TABLE reservations ADD COLUMN IF NOT EXISTS status VARCHAR(12) NOT NULL DEFAULT 'active'",
    "ALTER TABLE reservations ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP",
    "ALTER TABLE reservations ADD COLUMN IF NOT EXISTS closed_at TIMESTAMP",
    "UPDATE reservations SET expires_at = created_at + INTERVAL '30 minutes' WHERE expires_at IS NULL",
    "CREATE INDEX IF NOT EXISTS idx_reservations_user ON reservations (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_reservations_active ON reservations (status, expires_at)",
]


def migrate():
    """Bring an existing database up to date. Several Inventory pods start at once,
    so a PostgreSQL advisory lock makes them take turns (only one changes the schema)."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(4242)")
            try:
                for sql in MIGRATIONS:
                    cur.execute(sql)
                conn.commit()
            finally:
                cur.execute("SELECT pg_advisory_unlock(4242)")


@app.on_event("startup")
def startup():
    global pool
    pool = connect_with_retry()
    migrate()
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


@app.get("/pharmacies/{pharmacy_id}/stock")
def pharmacy_stock(pharmacy_id: int):
    """Every medicine one pharmacy carries, including sold-out ones (pharmacy portal)."""
    rows = query(
        "SELECT medicine_name AS medicine, quantity, updated_at FROM stock "
        "WHERE pharmacy_id = %s ORDER BY medicine_name", (pharmacy_id,))
    for r in rows:
        r["updated_at"] = r["updated_at"].isoformat()
    return {"pharmacy_id": pharmacy_id, "served_by": POD_NAME, "stock": rows}


@app.get("/pharmacies/{pharmacy_id}/reservations")
def pharmacy_reservations(pharmacy_id: int, limit: int = Query(12, ge=1, le=50)):
    rows = query(
        "SELECT id, medicine_name AS medicine, quantity, customer, reserved_by, status, "
        "created_at, expires_at, closed_at FROM reservations WHERE pharmacy_id = %s "
        "ORDER BY (status = 'active') DESC, id DESC LIMIT %s", (pharmacy_id, limit))
    return {"pharmacy_id": pharmacy_id, "reservations": [iso_dates(r) for r in rows]}


def iso_dates(row: dict) -> dict:
    for k in ("created_at", "expires_at", "closed_at", "updated_at"):
        if row.get(k) is not None:
            row[k] = row[k].isoformat()
    return row


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
    user_id: str | None = Field(None, max_length=40)    # this browser's id, for "My reservations"


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
                "INSERT INTO reservations (pharmacy_id, medicine_name, quantity, customer, reserved_by, "
                "user_id, status, expires_at) VALUES (%s, %s, %s, %s, %s, %s, 'active', "
                "NOW() + make_interval(mins => %s)) RETURNING id, expires_at",
                (req.pharmacy_id, row["medicine_name"], req.quantity, req.customer, POD_NAME,
                 req.user_id, RESERVATION_MINUTES))
            inserted = cur.fetchone()
            reservation_id, expires_at = inserted["id"], inserted["expires_at"]
            cur.execute("SELECT name FROM pharmacies WHERE id = %s", (req.pharmacy_id,))
            pharmacy = cur.fetchone()["name"]
    invalidate_cache(row["medicine_name"])
    return {"reservation_id": reservation_id, "medicine": row["medicine_name"], "pharmacy": pharmacy,
            "pharmacy_id": req.pharmacy_id, "quantity": req.quantity, "remaining": remaining,
            "expires_at": expires_at.isoformat(), "hold_minutes": RESERVATION_MINUTES}


@app.post("/reserve")
def reserve(req: ReservationRequest, use_lock: bool = True):
    resource = f"stock:{req.pharmacy_id}:{req.medicine.strip().lower()}"
    if not use_lock:
        # DEMO ONLY: no mutual exclusion -> race conditions are possible
        result = reserve_critical_section(req)
        result.update(lock={"used": False}, served_by=POD_NAME)
        activity.record(cache, "reserved-unsafe",
                        f"Reserved {req.quantity} {result['medicine']} at {result['pharmacy']} WITHOUT a lock",
                        f"Handled by {POD_NAME}. {result['remaining']} left.", pod=POD_NAME)
        return result
    try:
        with coordinator.lock(resource) as held:
            result = reserve_critical_section(req)
    except LockUnavailable as e:
        raise HTTPException(503, f"Could not get lock: {e}")
    result.update(lock={"used": True, "resource": resource, "granted_by_node": held["leader_id"]},
                  served_by=POD_NAME)
    activity.record(cache, "reserved", f"Reserved {req.quantity} {result['medicine']} at {result['pharmacy']}",
                    f"Handled by {POD_NAME} with a lock from coordinator node {held['leader_id']}. "
                    f"{result['remaining']} left.", pod=POD_NAME)
    return result


# ------------------------------------------------------------------ my reservations
@app.get("/reservations")
def my_reservations(user_id: str = Query(..., min_length=4, max_length=40)):
    """Everything this browser has reserved: active first, then the most recent closed ones."""
    rows = query(
        "SELECT r.id, r.pharmacy_id, p.name AS pharmacy, p.area, r.medicine_name AS medicine, r.quantity, "
        "r.customer, r.status, r.created_at, r.expires_at, r.closed_at "
        "FROM reservations r JOIN pharmacies p ON p.id = r.pharmacy_id WHERE r.user_id = %s "
        "ORDER BY (r.status = 'active') DESC, r.id DESC LIMIT 30", (user_id,))
    return {"reservations": [iso_dates(r) for r in rows], "hold_minutes": RESERVATION_MINUTES,
            "server_time": time.time()}


def close_reservation(reservation_id: int, new_status: str, actor: str, user_id: str | None = None) -> dict:
    """Move an ACTIVE reservation to cancelled / expired / collected.

    Cancelled and expired reservations give their strips BACK to the pharmacy's
    stock, so this changes the same stock row that /reserve changes. It therefore
    takes the same distributed lock: a cancel and a new reservation for the same
    item can never interleave.
    """
    found = query("SELECT pharmacy_id, medicine_name, quantity, user_id, status FROM reservations WHERE id = %s",
                  (reservation_id,))
    if not found:
        raise HTTPException(404, "No such reservation")
    r = found[0]
    if user_id is not None and r["user_id"] != user_id:
        raise HTTPException(403, "That reservation belongs to someone else")
    if r["status"] != "active":
        raise HTTPException(409, f"Reservation is already {r['status']}")

    resource = f"stock:{r['pharmacy_id']}:{r['medicine_name'].strip().lower()}"
    try:
        with coordinator.lock(resource) as held:
            with db_conn() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    # Re-check inside the lock: another pod may have closed it meanwhile.
                    cur.execute("UPDATE reservations SET status = %s, closed_at = NOW() "
                                "WHERE id = %s AND status = 'active' RETURNING id", (new_status, reservation_id))
                    if not cur.fetchone():
                        raise HTTPException(409, "Reservation was already closed")
                    remaining = None
                    if new_status in ("cancelled", "expired"):
                        cur.execute("UPDATE stock SET quantity = quantity + %s, updated_at = NOW() "
                                    "WHERE pharmacy_id = %s AND medicine_name = %s RETURNING quantity",
                                    (r["quantity"], r["pharmacy_id"], r["medicine_name"]))
                        remaining = cur.fetchone()["quantity"]
                    cur.execute("SELECT id, name, area, lat, lng FROM pharmacies WHERE id = %s", (r["pharmacy_id"],))
                    pharmacy = dict(cur.fetchone())
    except LockUnavailable as e:
        raise HTTPException(503, f"Could not get lock: {e}")

    if remaining is not None:
        invalidate_cache(r["medicine_name"])
        if remaining == r["quantity"]:
            # Stock went from 0 back to > 0: tell subscribers (publish-subscribe).
            events.publish_standalone(events.restock_event(pharmacy, r["medicine_name"], remaining, POD_NAME))
    returned = f" {r['quantity']} back in stock ({remaining} now)." if remaining is not None else ""
    activity.record(cache, new_status, f"{new_status.capitalize()} reservation R-{reservation_id}: "
                    f"{r['medicine_name']} at {pharmacy['name']}",
                    f"{actor}. Handled by {POD_NAME} with a lock from coordinator node {held['leader_id']}.{returned}",
                    pod=POD_NAME)
    return {"reservation_id": reservation_id, "status": new_status, "stock_now": remaining}


@app.post("/reservations/{reservation_id}/cancel")
def cancel_reservation(reservation_id: int, user_id: str = Query(..., min_length=4, max_length=40)):
    return close_reservation(reservation_id, "cancelled", "Cancelled by the customer", user_id)


@app.post("/reservations/{reservation_id}/collect")
def collect_reservation(reservation_id: int):
    """The pharmacy hands the medicine over (pharmacy portal)."""
    return close_reservation(reservation_id, "collected", "Collected at the counter")


@app.post("/jobs/expire-reservations")
def expire_reservations(scheduled_by: int = 0):
    """Release holds that ran out. Called every 15 s by the coordinator LEADER only,
    so exactly one place in the whole system schedules this job (section 24)."""
    due = query("SELECT id FROM reservations WHERE status = 'active' AND expires_at < NOW() ORDER BY id LIMIT 50")
    expired = 0
    for row in due:
        try:
            close_reservation(row["id"], "expired", f"Hold time ran out. Job scheduled by coordinator node {scheduled_by}")
            expired += 1
        except HTTPException:
            pass   # closed by someone else in the meantime
    return {"expired": expired, "checked": len(due), "run_by": POD_NAME}


@app.get("/activity")
def recent_activity(limit: int = Query(40, ge=1, le=60)):
    """Newest first. Written by every consumer and Inventory pod into one shared Redis list."""
    return {"events": activity.recent(cache, limit)}


@app.get("/cluster")
def cluster():
    """Live view of the coordinator cluster, shown in the frontend."""
    return {"nodes": coordinator.cluster_status()}
