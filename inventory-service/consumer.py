"""
Inventory Consumer  (the CONSUMER side)
---------------------------------------
Reads stock-update events from RabbitMQ and saves them to PostgreSQL.
It uses the same Docker image as the Inventory API but runs this file instead,
so it can be scaled separately (e.g. 1 API pod, 3 consumer pods).

Distributed Systems ideas shown here:
  * Competing consumers: run several copies; RabbitMQ hands each message to
    exactly ONE of them, so work is shared automatically.
  * At-least-once delivery: we ACK a message only AFTER it is saved. If this
    process crashes mid-way, RabbitMQ re-delivers the message to another consumer.
  * Idempotency: an event SETS the quantity (not "add 5"), so processing the
    same event twice gives the same result.
  * Ordering with timestamps: with several consumers, an older event could be
    processed after a newer one. We only apply an event if it is newer than
    what is stored ("last write wins").
  * Cache invalidation: after saving, we delete that medicine's cached search
    result in Redis, so users see fresh data.
"""
import json
import os
import socket
import time
from datetime import datetime, timezone

import pika
import psycopg2
import redis

import activity
import events

DB = dict(
    host=os.getenv("DB_HOST", "postgres"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME", "medifind"),
    user=os.getenv("DB_USER", "medifind"),
    password=os.getenv("DB_PASSWORD", "medifind"),
)
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "stock_updates")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
# Optional artificial slowness (milliseconds per message). Useful in the demo
# to make the queue fill up so you can show scaling consumers.
PROCESS_DELAY_MS = int(os.getenv("PROCESS_DELAY_MS", "0"))
POD_NAME = socket.gethostname()

UPSERT_SQL = """
INSERT INTO stock (pharmacy_id, medicine_name, quantity, updated_at)
VALUES (%s, %s, %s, %s)
ON CONFLICT (pharmacy_id, medicine_name) DO UPDATE
    SET quantity = EXCLUDED.quantity,
        updated_at = EXCLUDED.updated_at
    WHERE stock.updated_at <= EXCLUDED.updated_at   -- ignore out-of-date events
"""


def cache_key(medicine: str) -> str:
    """Must match the key format used by the Search Service."""
    return f"search:{medicine.strip().lower()}"


def wait_for(name, fn, delay=2.0):
    """Keep retrying until a dependency (DB / RabbitMQ) is reachable."""
    while True:
        try:
            return fn()
        except Exception as e:
            print(f"[consumer] waiting for {name}: {e}", flush=True)
            time.sleep(delay)


class Consumer:
    def __init__(self):
        self.db = wait_for("PostgreSQL", lambda: psycopg2.connect(**DB))
        self.cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                                 socket_timeout=1, socket_connect_timeout=1)
        self.processed = 0
        self.pharmacies: dict[int, dict] = {}     # small cache: id -> name, area, lat, lng

    def canonical_name(self, cur, medicine: str) -> str:
        """Reuse the existing spelling so 'dolo 650' and 'Dolo 650' stay one row."""
        cur.execute("SELECT medicine_name FROM stock WHERE LOWER(medicine_name) = LOWER(%s) LIMIT 1",
                    (medicine,))
        row = cur.fetchone()
        return row[0] if row else medicine

    def save(self, event: dict) -> tuple[str, bool, int | None]:
        """Returns (medicine name as stored, whether the update was applied, previous quantity)."""
        ts = datetime.fromisoformat(event["timestamp"]).replace(tzinfo=None)
        with self.db.cursor() as cur:
            name = self.canonical_name(cur, event["medicine"])
            cur.execute("SELECT quantity FROM stock WHERE pharmacy_id = %s AND medicine_name = %s",
                        (event["pharmacy_id"], name))
            row = cur.fetchone()
            previous = row[0] if row else None
            cur.execute(UPSERT_SQL, (event["pharmacy_id"], name, event["quantity"], ts))
            applied = cur.rowcount == 1
        self.db.commit()
        return name, applied, previous

    def pharmacy(self, pharmacy_id: int) -> dict:
        if pharmacy_id not in self.pharmacies:
            with self.db.cursor() as cur:
                cur.execute("SELECT id, name, area, lat, lng FROM pharmacies WHERE id = %s", (pharmacy_id,))
                row = cur.fetchone()
            self.db.commit()
            self.pharmacies[pharmacy_id] = (
                dict(zip(("id", "name", "area", "lat", "lng"), row)) if row
                else {"id": pharmacy_id, "name": f"Pharmacy {pharmacy_id}", "area": "", "lat": 0, "lng": 0})
        return self.pharmacies[pharmacy_id]

    def invalidate_cache(self, medicine: str):
        try:
            self.cache.delete(cache_key(medicine))
        except redis.RedisError as e:
            # Not fatal: cached entries also expire on their own (TTL).
            print(f"[consumer] could not clear cache (will expire by TTL): {e}", flush=True)

    def on_message(self, ch, method, properties, body):
        # 1. Parse and validate. A broken message can never succeed, so we
        #    reject it WITHOUT requeue (otherwise it would loop forever).
        try:
            event = json.loads(body)
            for field in ("pharmacy_id", "medicine", "quantity", "timestamp"):
                if field not in event:
                    raise ValueError(f"missing field '{field}'")
        except (ValueError, json.JSONDecodeError) as e:
            print(f"[consumer] dropping bad message: {e}", flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            activity.record(self.cache, "rejected", "Rejected a broken message",
                            f"{e}. Dropped by {POD_NAME} so it can't block the queue.")
            return

        if PROCESS_DELAY_MS:
            time.sleep(PROCESS_DELAY_MS / 1000)

        # 2. Save to the database.
        try:
            name, applied, previous = self.save(event)
        except psycopg2.errors.ForeignKeyViolation:
            self.db.rollback()
            print(f"[consumer] unknown pharmacy {event['pharmacy_id']}, dropping event", flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            activity.record(self.cache, "rejected", f"Rejected an update for unknown pharmacy {event['pharmacy_id']}",
                            f"Dropped by {POD_NAME}.")
            return
        except psycopg2.Error as e:
            # Database problem: put the message BACK in the queue and reconnect.
            print(f"[consumer] DB error, requeueing message: {e}", flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            try:
                self.db.close()
            except Exception:
                pass
            self.db = wait_for("PostgreSQL", lambda: psycopg2.connect(**DB))
            return

        # 3. Clear the cache. If the medicine was sold out here and is now back,
        #    PUBLISH a "restocked" event for anyone subscribed (publish-subscribe).
        #    Then ACK. ACK last = at-least-once delivery.
        pharmacy = self.pharmacy(event["pharmacy_id"])
        restocked = applied and event["quantity"] > 0 and not previous
        if applied:
            self.invalidate_cache(name)
        if restocked:
            events.publish(ch, events.restock_event(pharmacy, name, event["quantity"], POD_NAME))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        self.processed += 1
        status = "applied" if applied else "skipped (older than stored data)"
        print(f"[consumer {POD_NAME}] #{self.processed} pharmacy={event['pharmacy_id']} "
              f"{event['medicine']} -> {event['quantity']} {status}", flush=True)

        # 4. Record it for the live activity feed (how long since the pharmacy sent it).
        sent = datetime.fromisoformat(event["timestamp"])
        delay_ms = max(0, round((datetime.now(timezone.utc) - sent).total_seconds() * 1000))
        if applied:
            back = " Back in stock, so a restock event was published." if restocked else ""
            activity.record(self.cache, "update", f"{pharmacy['name']} set {name} to {event['quantity']}",
                            f"Saved by {POD_NAME}, {delay_ms} ms after the pharmacy sent it.{back}",
                            pod=POD_NAME, delay_ms=delay_ms)
        else:
            activity.record(self.cache, "skipped", f"Ignored an out-of-date update for {name} at {pharmacy['name']}",
                            f"A newer update was already saved. Checked by {POD_NAME}.", pod=POD_NAME)

    def run(self):
        while True:
            try:
                creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
                conn = pika.BlockingConnection(pika.ConnectionParameters(
                    host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=creds, heartbeat=30))
                ch = conn.channel()
                ch.queue_declare(queue=QUEUE_NAME, durable=True)
                events.declare_exchange(ch)          # where "restocked" events are published
                # prefetch: each consumer holds at most 5 un-ACKed messages,
                # so work spreads fairly across all consumer pods.
                ch.basic_qos(prefetch_count=5)
                ch.basic_consume(queue=QUEUE_NAME, on_message_callback=self.on_message)
                print(f"[consumer {POD_NAME}] listening on queue '{QUEUE_NAME}'", flush=True)
                ch.start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                print(f"[consumer] RabbitMQ connection lost/unavailable, retrying: {e}", flush=True)
                time.sleep(2)


if __name__ == "__main__":
    Consumer().run()
