"""
"Restocked" events: PUBLISH-SUBSCRIBE.

When a medicine goes from sold out (0) to available at a pharmacy, we publish a
"restocked" event to a RabbitMQ FANOUT EXCHANGE called "stock.events".

  * A queue (used for stock updates) delivers each message to ONE consumer.
  * A fanout exchange copies each message to EVERY queue bound to it.

So the publisher doesn't know or care who is listening. Today the Notification
Service subscribes (to alert waiting patients); tomorrow an analytics or SMS
service could bind its own queue and receive the same events, with no change here.

Published by:
  * consumer.py   when a pharmacy's stock update takes a medicine from 0 to > 0
  * main.py       when a cancelled/expired reservation returns the last strip
"""
import json
import os
from datetime import datetime, timezone

import pika

EXCHANGE = os.getenv("STOCK_EVENTS_EXCHANGE", "stock.events")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")


def declare_exchange(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)


def restock_event(pharmacy: dict, medicine: str, quantity: int, source: str) -> dict:
    return {"type": "restocked", "pharmacy_id": pharmacy["id"], "pharmacy": pharmacy["name"],
            "area": pharmacy["area"], "lat": pharmacy["lat"], "lng": pharmacy["lng"],
            "medicine": medicine, "quantity": quantity, "source": source,
            "time": datetime.now(timezone.utc).isoformat()}


def publish(channel, event: dict):
    """Publish on an existing channel (the consumer already has one)."""
    channel.basic_publish(exchange=EXCHANGE, routing_key="", body=json.dumps(event).encode(),
                          properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))


def publish_standalone(event: dict) -> bool:
    """Open a short connection, publish one event, close. Used for rare events from the API."""
    try:
        creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        conn = pika.BlockingConnection(pika.ConnectionParameters(
            host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=creds,
            socket_timeout=3, blocked_connection_timeout=3))
        try:
            ch = conn.channel()
            declare_exchange(ch)
            publish(ch, event)
        finally:
            conn.close()
        return True
    except Exception as e:
        print(f"[events] could not publish restock event: {e}", flush=True)
        return False
