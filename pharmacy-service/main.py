"""
Pharmacy Service  (the PRODUCER side)
-------------------------------------
Pharmacies call this API whenever their stock changes.

This service does NOT write to the database. It only turns each update into an
EVENT and publishes it to a RabbitMQ queue, then answers immediately with
"202 Accepted". The Inventory Consumer saves it later.

Why? (Distributed Systems idea: asynchronous, decoupled communication)
  * Pharmacies get a fast response even if the database is slow or down.
  * If the Inventory side crashes, updates wait safely in the queue.
  * Producers and consumers can be scaled independently.
"""
import json
import os
import socket
import threading
import time
import uuid
from datetime import datetime, timezone

import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "stock_updates")
POD_NAME = socket.gethostname()

app = FastAPI(title="MediFind - Pharmacy Service")


class StockUpdate(BaseModel):
    """What a pharmacy sends. Pydantic rejects bad input with HTTP 422."""
    pharmacy_id: int = Field(..., gt=0)
    medicine: str = Field(..., min_length=2, max_length=100)
    quantity: int = Field(..., ge=0, le=100000)


class Publisher:
    """
    Keeps ONE connection to RabbitMQ and reconnects if it drops.
    pika's BlockingConnection is not thread-safe, and FastAPI runs normal
    (non-async) endpoints in a thread pool, so a lock protects it.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._conn = None
        self._channel = None
        self.published = 0

    def _connect(self):
        creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=creds,
            heartbeat=30, blocked_connection_timeout=10,
        )
        self._conn = pika.BlockingConnection(params)
        self._channel = self._conn.channel()
        # durable=True -> the queue itself survives a RabbitMQ restart
        self._channel.queue_declare(queue=QUEUE_NAME, durable=True)
        # publisher confirms -> RabbitMQ tells us when it has safely stored the message
        self._channel.confirm_delivery()

    def connected(self) -> bool:
        return self._conn is not None and self._conn.is_open

    def publish(self, event: dict):
        body = json.dumps(event).encode()
        with self._lock:
            for attempt in (1, 2):  # one automatic reconnect on failure
                try:
                    if not self.connected():
                        self._connect()
                    self._channel.basic_publish(
                        exchange="",
                        routing_key=QUEUE_NAME,
                        body=body,
                        # delivery_mode=2 -> message is saved to disk (persistent)
                        properties=pika.BasicProperties(
                            delivery_mode=2, content_type="application/json",
                            message_id=event["event_id"],
                        ),
                    )
                    self.published += 1
                    return
                except Exception as e:
                    self._conn = None
                    if attempt == 2:
                        raise RuntimeError(f"RabbitMQ unavailable: {type(e).__name__} {e}".strip())
                    time.sleep(0.5)


publisher = Publisher()


@app.on_event("startup")
def startup():
    # Try to connect early, but don't crash if RabbitMQ is still starting:
    # publish() will connect on the first request.
    for _ in range(10):
        try:
            with publisher._lock:
                publisher._connect()
            print(f"[pharmacy] connected to RabbitMQ, running on {POD_NAME}")
            return
        except Exception as e:
            print(f"[pharmacy] RabbitMQ not ready yet: {e}")
            time.sleep(2)


@app.get("/health")
def health():
    return {"status": "ok", "pod": POD_NAME, "rabbitmq_connected": publisher.connected(),
            "events_published": publisher.published}


@app.post("/stock", status_code=202)
def update_stock(update: StockUpdate):
    event = {
        "event_id": str(uuid.uuid4()),       # unique ID for tracing
        "type": "stock_updated",
        "pharmacy_id": update.pharmacy_id,
        "medicine": update.medicine.strip(),
        "quantity": update.quantity,         # the NEW absolute quantity (not +/-)
        "timestamp": datetime.now(timezone.utc).isoformat(),  # used for ordering
        "published_by": POD_NAME,
    }
    try:
        publisher.publish(event)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    return {"status": "queued", "event_id": event["event_id"], "published_by": POD_NAME}
