"""
Notification Service  ("Notify me when it's back")
--------------------------------------------------
A patient whose medicine is sold out everywhere can ask to be told when it
comes back. This service:

  1. Stores those requests (subscriptions) in its OWN tables.
  2. SUBSCRIBES to the "stock.events" fanout exchange. Every time a medicine
     goes from sold out to available at any pharmacy, it receives a
     "restocked" event, finds the patients waiting for that medicine, and
     creates a notification for each.
  3. Lets the patient's browser fetch its notifications.

Distributed Systems ideas shown here:
  * PUBLISH-SUBSCRIBE: the Inventory side publishes "restocked" without knowing
    this service exists. We bind our own queue to the exchange, so we get a
    copy of every event. More services could subscribe the same way.
  * Loose coupling: if this service is down, restock events wait in its queue
    (durable) and are processed when it comes back; nobody else is affected.
  * Database per service: subscriptions/notifications belong to this service
    only; no other service reads or writes them.

API
  POST   /subscriptions                 {user_id, medicine, area?, lat?, lng?}
  GET    /subscriptions?user_id=...
  DELETE /subscriptions/{id}?user_id=...
  GET    /notifications?user_id=...&unread_only=true
  POST   /notifications/read?user_id=...
"""
import json
import math
import os
import socket
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import pika
import psycopg2
import psycopg2.extras
import redis
from fastapi import FastAPI, HTTPException, Query
from psycopg2.pool import ThreadedConnectionPool
from pydantic import BaseModel, Field

DB = dict(host=os.getenv("DB_HOST", "postgres"), port=int(os.getenv("DB_PORT", "5432")),
          dbname=os.getenv("DB_NAME", "medifind"), user=os.getenv("DB_USER", "medifind"),
          password=os.getenv("DB_PASSWORD", "medifind"))
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
EXCHANGE = os.getenv("STOCK_EVENTS_EXCHANGE", "stock.events")
QUEUE = os.getenv("NOTIFY_QUEUE", "notifications.restock")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
POD_NAME = socket.gethostname()

app = FastAPI(title="MediFind - Notification Service")
pool: ThreadedConnectionPool | None = None
cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_timeout=0.5, socket_connect_timeout=0.5)
subscriber_state = {"connected": False, "events_received": 0, "notifications_created": 0}

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS subscriptions (
        id          SERIAL PRIMARY KEY,
        user_id     VARCHAR(40)  NOT NULL,
        medicine    VARCHAR(100) NOT NULL,
        area        VARCHAR(100),
        lat         DOUBLE PRECISION,
        lng         DOUBLE PRECISION,
        status      VARCHAR(12)  NOT NULL DEFAULT 'waiting',    -- waiting / notified / cancelled
        created_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
        notified_at TIMESTAMP)""",
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_waiting ON subscriptions (LOWER(medicine)) WHERE status = 'waiting'",
    """CREATE TABLE IF NOT EXISTS notifications (
        id              SERIAL PRIMARY KEY,
        user_id         VARCHAR(40)  NOT NULL,
        subscription_id INT REFERENCES subscriptions(id) ON DELETE CASCADE,
        medicine        VARCHAR(100) NOT NULL,
        pharmacy_id     INT,
        pharmacy        VARCHAR(100),
        area            VARCHAR(100),
        quantity        INT,
        distance_km     DOUBLE PRECISION,
        is_read         BOOLEAN      NOT NULL DEFAULT FALSE,
        created_at      TIMESTAMP    NOT NULL DEFAULT NOW())""",
    "CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications (user_id, is_read)",
]


# ------------------------------------------------------------------ database
def connect_with_retry() -> ThreadedConnectionPool:
    while True:
        try:
            return ThreadedConnectionPool(1, 10, **DB)
        except psycopg2.OperationalError as e:
            print(f"[notify] waiting for PostgreSQL: {e}", flush=True)
            time.sleep(2)


@contextmanager
def db_conn():
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
    return [iso(dict(r)) for r in rows]


def iso(row: dict) -> dict:
    for k in ("created_at", "notified_at"):
        if row.get(k) is not None:
            row[k] = row[k].isoformat()
    return row


def distance_km(lat1, lng1, lat2, lng2):
    if None in (lat1, lng1, lat2, lng2):
        return None
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin(math.radians(lat2 - lat1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lng2 - lng1) / 2) ** 2)
    return round(2 * 6371 * math.asin(math.sqrt(a)), 2)


def record_activity(text: str, detail: str):
    """Same shared Redis list the Inventory pods write to (System view feed)."""
    entry = {"time": datetime.now(timezone.utc).isoformat(), "kind": "notified", "text": text,
             "detail": detail, "pod": POD_NAME}
    try:
        pipe = cache.pipeline()
        pipe.lpush("activity", json.dumps(entry))
        pipe.ltrim("activity", 0, 59)
        pipe.execute()
    except redis.RedisError:
        pass


# ------------------------------------------------------------------ the subscriber
def handle_restock(event: dict) -> int:
    """Turn one restocked event into notifications for everyone waiting. Returns how many."""
    with db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # FOR UPDATE SKIP LOCKED: if two copies of this service run, they never
            # notify the same subscription twice.
            cur.execute("SELECT id, user_id, lat, lng FROM subscriptions "
                        "WHERE status = 'waiting' AND LOWER(medicine) = LOWER(%s) FOR UPDATE SKIP LOCKED",
                        (event["medicine"],))
            waiting = cur.fetchall()
            for sub in waiting:
                cur.execute(
                    "INSERT INTO notifications (user_id, subscription_id, medicine, pharmacy_id, pharmacy, area, "
                    "quantity, distance_km) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (sub["user_id"], sub["id"], event["medicine"], event["pharmacy_id"], event["pharmacy"],
                     event["area"], event["quantity"],
                     distance_km(sub["lat"], sub["lng"], event.get("lat"), event.get("lng"))))
                cur.execute("UPDATE subscriptions SET status = 'notified', notified_at = NOW() WHERE id = %s",
                            (sub["id"],))
    return len(waiting)


def on_message(ch, method, properties, body):
    try:
        event = json.loads(body)
        if event.get("type") != "restocked":
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
    except (ValueError, TypeError):
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    try:
        n = handle_restock(event)
    except psycopg2.Error as e:
        print(f"[notify] DB error, will retry the event: {e}", flush=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        time.sleep(1)
        return
    ch.basic_ack(delivery_tag=method.delivery_tag)
    subscriber_state["events_received"] += 1
    subscriber_state["notifications_created"] += n
    print(f"[notify {POD_NAME}] {event['medicine']} back at {event['pharmacy']}: {n} notified", flush=True)
    if n:
        record_activity(f"Told {n} {'person' if n == 1 else 'people'} that {event['medicine']} is back "
                        f"at {event['pharmacy']}",
                        f"Restock event received from the stock.events exchange by {POD_NAME}.")


def subscriber_loop():
    """Runs in a background thread for the life of the service."""
    while True:
        try:
            creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            conn = pika.BlockingConnection(pika.ConnectionParameters(
                host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=creds, heartbeat=30))
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)
            ch.queue_declare(queue=QUEUE, durable=True)        # our own queue...
            ch.queue_bind(queue=QUEUE, exchange=EXCHANGE)      # ...gets a copy of every event
            ch.basic_qos(prefetch_count=10)
            ch.basic_consume(queue=QUEUE, on_message_callback=on_message)
            subscriber_state["connected"] = True
            print(f"[notify {POD_NAME}] subscribed to '{EXCHANGE}' via queue '{QUEUE}'", flush=True)
            ch.start_consuming()
        except Exception as e:
            subscriber_state["connected"] = False
            print(f"[notify] RabbitMQ unavailable, retrying: {e}", flush=True)
            time.sleep(2)


@app.on_event("startup")
def startup():
    global pool
    pool = connect_with_retry()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(4343)")      # only one pod creates tables at a time
            try:
                for sql in SCHEMA:
                    cur.execute(sql)
                conn.commit()
            finally:
                cur.execute("SELECT pg_advisory_unlock(4343)")
    threading.Thread(target=subscriber_loop, daemon=True).start()
    print(f"[notify] ready on {POD_NAME}", flush=True)


# ------------------------------------------------------------------ API
class Subscription(BaseModel):
    user_id: str = Field(..., min_length=4, max_length=40)
    medicine: str = Field(..., min_length=2, max_length=100)
    area: str | None = Field(None, max_length=100)
    lat: float | None = Field(None, ge=-90, le=90)
    lng: float | None = Field(None, ge=-180, le=180)


@app.get("/health")
def health():
    return {"status": "ok", "pod": POD_NAME, **subscriber_state}


@app.post("/subscriptions", status_code=201)
def subscribe(sub: Subscription):
    medicine = sub.medicine.strip()
    existing = query("SELECT * FROM subscriptions WHERE user_id = %s AND LOWER(medicine) = LOWER(%s) "
                     "AND status = 'waiting'", (sub.user_id, medicine))
    if existing:
        return existing[0]          # asking twice is harmless (idempotent)
    return query("INSERT INTO subscriptions (user_id, medicine, area, lat, lng) VALUES (%s, %s, %s, %s, %s) "
                 "RETURNING *", (sub.user_id, medicine, sub.area, sub.lat, sub.lng))[0]


@app.get("/subscriptions")
def list_subscriptions(user_id: str = Query(..., min_length=4, max_length=40)):
    return {"subscriptions": query(
        "SELECT * FROM subscriptions WHERE user_id = %s AND status <> 'cancelled' "
        "ORDER BY (status = 'waiting') DESC, id DESC LIMIT 20", (user_id,))}


@app.delete("/subscriptions/{sub_id}")
def unsubscribe(sub_id: int, user_id: str = Query(..., min_length=4, max_length=40)):
    rows = query("UPDATE subscriptions SET status = 'cancelled' WHERE id = %s AND user_id = %s "
                 "AND status = 'waiting' RETURNING id", (sub_id, user_id))
    if not rows:
        raise HTTPException(404, "No waiting alert with that id")
    return {"cancelled": sub_id}


@app.get("/notifications")
def list_notifications(user_id: str = Query(..., min_length=4, max_length=40), unread_only: bool = False):
    sql = "SELECT * FROM notifications WHERE user_id = %s"
    if unread_only:
        sql += " AND NOT is_read"
    return {"notifications": query(sql + " ORDER BY id DESC LIMIT 20", (user_id,))}


@app.post("/notifications/read")
def mark_read(user_id: str = Query(..., min_length=4, max_length=40)):
    rows = query("UPDATE notifications SET is_read = TRUE WHERE user_id = %s AND NOT is_read RETURNING id",
                 (user_id,))
    return {"marked_read": len(rows)}
