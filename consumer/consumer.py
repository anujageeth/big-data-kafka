"""
consumer/consumer.py
--------------------
Kafka Order Consumer with Real-Time Aggregation, Retry Logic, and DLQ

Subscribes to the `orders` topic, deserializes Avro messages, and:
  - Maintains a running average of order prices
  - Retries failed messages up to MAX_RETRIES times with exponential backoff
  - Sends permanently failed messages to the `orders-dlq` Dead Letter Queue

Usage:
    python consumer.py [--group GROUP_ID]

Arguments:
    --group     Kafka consumer group ID (default: order-consumers)

Failure behaviour (controlled by the `failure_mode` header on each message):
    off        - Process normally, no errors injected
    temporary  - Raises a TemporaryError on attempts 1 and 2; succeeds on 3rd
    permanent  - Always raises a PermanentError; exhausts retries -> DLQ
    random     - Randomly raises TemporaryError or PermanentError (~50/50)

Design decisions:
    - Only SUCCESSFULLY processed messages count toward the running average.
      DLQ-ed messages are excluded so the aggregate reflects real throughput.
    - DLQ messages are serialized as JSON (not Avro) for simplicity; the
      envelope carries the original Avro bytes + error metadata.
    - Retry backoff: 1s after attempt 1, 2s after attempt 2 (exponential).
"""

import argparse
import io
import json
import os
import random
import time
from datetime import datetime, timezone

import fastavro
from confluent_kafka import Consumer, KafkaError, Producer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_ORDERS      = "orders"
TOPIC_DLQ         = "orders-dlq"
SCHEMA_PATH       = os.path.join(os.path.dirname(__file__), "..", "schema", "order.avsc")

MAX_RETRIES  = 3          # total attempts before sending to DLQ
BACKOFF_BASE = 1          # seconds; wait = BACKOFF_BASE * 2^(attempt-1)

# ---------------------------------------------------------------------------
# Custom exception types
# ---------------------------------------------------------------------------

class TemporaryError(Exception):
    """Represents a transient, retryable failure (e.g., downstream timeout)."""

class PermanentError(Exception):
    """Represents an unrecoverable failure (e.g., invalid business rule)."""

# ---------------------------------------------------------------------------
# Failure simulation
# ---------------------------------------------------------------------------

def simulate_failure(failure_mode: str, attempt: int):
    """
    Inject a failure based on the failure_mode string.

    temporary  -> fail on attempts 1 & 2, succeed on attempt 3+
    permanent  -> always fail (exhausts retries -> DLQ)
    random     -> coin-flip between temporary and permanent behaviour
    off        -> no failure injected
    """
    if failure_mode == "off":
        return   # no failure

    if failure_mode == "temporary":
        if attempt <= 2:
            raise TemporaryError(
                f"Simulated transient error (attempt {attempt}/3)"
            )
        # attempt 3 succeeds — fall through

    elif failure_mode == "permanent":
        raise PermanentError("Simulated permanent error — cannot recover")

    elif failure_mode == "random":
        choice = random.choice(["temporary", "permanent"])
        if choice == "temporary" and attempt <= 2:
            raise TemporaryError(
                f"Simulated random transient error (attempt {attempt}/3)"
            )
        elif choice == "permanent":
            raise PermanentError("Simulated random permanent error")

# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_order(record: dict):
    """
    Business logic for a successfully deserialized order.
    In a real system this might persist to a DB, call a pricing service, etc.
    Here it simply returns the record (the aggregation happens in the caller).
    """
    # Placeholder: extend this with real processing logic as needed
    return record


def process_with_retry(record: dict, failure_mode: str) -> tuple[bool, Exception | None]:
    """
    Attempt to process an order up to MAX_RETRIES times.

    Returns:
        (True, None)          on success
        (False, last_exception) when all retries are exhausted
    """
    last_exc = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            simulate_failure(failure_mode, attempt)
            process_order(record)
            return True, None

        except TemporaryError as e:
            last_exc = e
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            print(f"    [RETRY] Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                print(f"    [RETRY] Backing off for {wait}s...")
                time.sleep(wait)
            else:
                print(f"    [RETRY] Max retries reached. Sending to DLQ.")

        except PermanentError as e:
            last_exc = e
            print(f"    [DLQ]   Permanent error on attempt {attempt}: {e}")
            print(f"    [DLQ]   Sending to DLQ immediately (no more retries).")
            break   # no point retrying a permanent failure

    return False, last_exc

# ---------------------------------------------------------------------------
# Dead Letter Queue
# ---------------------------------------------------------------------------

def send_to_dlq(dlq_producer: Producer, original_avro_bytes: bytes,
                record: dict, error: Exception, retry_count: int):
    """
    Publish a failed message to the orders-dlq topic.
    The payload is JSON-encoded for easy inspection and debugging.

    DLQ envelope fields:
        original_record  - the deserialized order dict
        error_reason     - string representation of the exception
        error_type       - class name of the exception (Temporary/Permanent)
        retry_count      - number of retries attempted
        failed_at        - UTC ISO-8601 timestamp
    """
    dlq_payload = json.dumps({
        "original_record": record,
        "error_reason":    str(error),
        "error_type":      type(error).__name__,
        "retry_count":     retry_count,
        "failed_at":       datetime.now(timezone.utc).isoformat(),
    }).encode("utf-8")

    dlq_producer.produce(topic=TOPIC_DLQ, value=dlq_payload)
    dlq_producer.poll(0)
    print(f"    [DLQ]   Message orderId={record.get('orderId')} "
          f"written to {TOPIC_DLQ}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Kafka Order Consumer")
    parser.add_argument("--group", type=str, default="order-consumers",
                        help="Kafka consumer group ID")
    args = parser.parse_args()

    print("=" * 60)
    print(f"  Kafka Order Consumer")
    print(f"  Bootstrap servers : {BOOTSTRAP_SERVERS}")
    print(f"  Topic             : {TOPIC_ORDERS}")
    print(f"  DLQ topic         : {TOPIC_DLQ}")
    print(f"  Consumer group    : {args.group}")
    print(f"  Max retries       : {MAX_RETRIES}")
    print(f"  Backoff base      : {BACKOFF_BASE}s (exponential)")
    print("=" * 60)
    print("  NOTE: Only successfully processed messages count toward")
    print("        the running average. DLQ messages are excluded.\n")

    schema = fastavro.schema.load_schema(SCHEMA_PATH)

    consumer = Consumer({
        "bootstrap.servers":  BOOTSTRAP_SERVERS,
        "group.id":           args.group,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_ORDERS])

    # Internal producer used exclusively for sending messages to the DLQ
    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    # Running aggregation state
    total_price = 0.0
    count       = 0
    dlq_count   = 0

    print("[CONSUMER] Waiting for messages... (Ctrl+C to stop)\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue   # no message within the timeout window

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition — not an error, just informational
                    print(f"[INFO] Reached end of partition "
                          f"{msg.topic()}[{msg.partition()}]")
                else:
                    print(f"[ERROR] Kafka error: {msg.error()}")
                continue

            # ----------------------------------------------------------------
            # Deserialize the Avro payload
            # ----------------------------------------------------------------
            try:
                buf    = io.BytesIO(msg.value())
                record = fastavro.schemaless_reader(buf, schema)
            except Exception as e:
                print(f"[ERROR] Failed to deserialize message: {e} — skipping")
                continue

            # ----------------------------------------------------------------
            # Extract the failure_mode header (set by producer for demo control)
            # ----------------------------------------------------------------
            failure_mode = "off"
            if msg.headers():
                for key, value in msg.headers():
                    if key == "failure_mode" and value:
                        failure_mode = value.decode("utf-8")

            print(f"\n[RECEIVED] orderId={record['orderId']} "
                  f"product={record['product']} "
                  f"price=${record['price']:.2f} "
                  f"failure_mode={failure_mode}")

            # ----------------------------------------------------------------
            # Process with retry logic
            # ----------------------------------------------------------------
            success, last_error = process_with_retry(record, failure_mode)

            if success:
                # Update running average only for successful messages
                total_price += record["price"]
                count       += 1
                running_avg  = total_price / count
                print(f"  [OK]    orderId={record['orderId']} "
                      f"price=${record['price']:.2f} | "
                      f"running_avg=${running_avg:.2f} (n={count})")
            else:
                # Exhausted retries — send to DLQ
                dlq_count += 1
                send_to_dlq(
                    dlq_producer,
                    original_avro_bytes=msg.value(),
                    record=record,
                    error=last_error,
                    retry_count=MAX_RETRIES,
                )
                print(f"  [STATS] Successful={count} | DLQ={dlq_count} | "
                      f"running_avg=$"
                      f"{'N/A' if count == 0 else f'{total_price/count:.2f}'}")

    except KeyboardInterrupt:
        print("\n[CONSUMER] Interrupted by user.")
    finally:
        print(f"\n[CONSUMER] Closing consumer...")
        print(f"[CONSUMER] Final stats: "
              f"processed={count} | dlq={dlq_count} | "
              f"running_avg=${'N/A' if count == 0 else f'{total_price/count:.2f}'}")
        consumer.close()
        dlq_producer.flush()
        print("[CONSUMER] Done.")


if __name__ == "__main__":
    main()
