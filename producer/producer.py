"""
producer/producer.py
--------------------
Kafka Order Producer

Generates random order messages, serializes them with Avro (fastavro),
and publishes them to the `orders` Kafka topic.

Usage:
    python producer.py [--count N] [--delay SECONDS] [--failure-mode MODE]

Arguments:
    --count         Number of messages to send (default: 20, 0 = infinite)
    --delay         Seconds between messages (default: 1.0)
    --failure-mode  Inject failures: off | temporary | permanent | random
                    (default: off)

Failure modes (for demo / testing):
    off        - Normal operation, all messages succeed
    temporary  - Simulates transient errors; consumer will retry and succeed
    permanent  - Simulates unrecoverable errors; messages land in DLQ
    random     - Randomly picks between temporary and permanent (~50/50)

Environment variable (alternative to --failure-mode):
    FAILURE_MODE=temporary|permanent|random|off
"""

import argparse
import io
import os
import random
import time
import uuid

import fastavro
from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schema", "order.avsc")

PRODUCTS = [
    "Laptop", "Mouse", "Keyboard", "Monitor", "Headphones",
    "Webcam", "USB Hub", "SSD Drive", "Notebook", "Pen Drive",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_schema(path: str):
    """Load and parse the Avro schema from a .avsc file."""
    return fastavro.schema.load_schema(path)


def serialize_order(schema, record: dict) -> bytes:
    """Serialize an order record to Avro bytes using schemaless encoding."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


def make_order(index: int) -> dict:
    """Generate a random order record."""
    return {
        "orderId": str(1000 + index),
        "product": random.choice(PRODUCTS),
        "price":   round(random.uniform(5.0, 500.0), 2),
    }


def delivery_report(err, msg):
    """Callback invoked by confluent-kafka after each produce() call."""
    if err is not None:
        print(f"  [DELIVERY FAILURE] topic={msg.topic()} "
              f"partition={msg.partition()} error={err}")
    else:
        print(f"  [DELIVERED] topic={msg.topic()} "
              f"partition={msg.partition()} offset={msg.offset()}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Kafka Order Producer")
    parser.add_argument("--count",        type=int,   default=20,
                        help="Messages to send (0 = infinite)")
    parser.add_argument("--delay",        type=float, default=1.0,
                        help="Seconds between messages")
    parser.add_argument("--failure-mode", type=str,   default=None,
                        choices=["off", "temporary", "permanent", "random"],
                        help="Failure injection mode for demo purposes")
    args = parser.parse_args()

    # Resolve failure mode: CLI arg > env var > default "off"
    failure_mode = (
        args.failure_mode
        or os.environ.get("FAILURE_MODE", "off").lower()
    )

    print("=" * 60)
    print(f"  Kafka Order Producer")
    print(f"  Bootstrap servers : {BOOTSTRAP_SERVERS}")
    print(f"  Topic             : {TOPIC}")
    print(f"  Message count     : {args.count if args.count > 0 else 'infinite'}")
    print(f"  Delay             : {args.delay}s")
    print(f"  Failure mode      : {failure_mode}")
    print("=" * 60)

    schema = load_schema(SCHEMA_PATH)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id":         "order-producer",
    })

    sent = 0
    index = 0

    try:
        while True:
            if args.count > 0 and sent >= args.count:
                break

            index += 1
            order = make_order(index)

            # Attach the failure mode as a message header so the consumer
            # knows how to behave when processing this specific message.
            headers = [("failure_mode", failure_mode.encode())]

            avro_bytes = serialize_order(schema, order)

            print(f"\n[PRODUCING] orderId={order['orderId']} "
                  f"product={order['product']} price=${order['price']:.2f} "
                  f"failure_mode={failure_mode}")

            producer.produce(
                topic=TOPIC,
                value=avro_bytes,
                headers=headers,
                callback=delivery_report,
            )
            producer.poll(0)   # trigger callbacks for any completed deliveries

            sent += 1
            time.sleep(args.delay)

    except KeyboardInterrupt:
        print("\n[PRODUCER] Interrupted by user.")
    finally:
        print(f"\n[PRODUCER] Flushing remaining messages...")
        producer.flush()
        print(f"[PRODUCER] Done. Total messages sent: {sent}")


if __name__ == "__main__":
    main()
