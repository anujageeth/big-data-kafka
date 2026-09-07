"""
dlq/dlq_inspector.py
--------------------
Dead Letter Queue (DLQ) Inspector

Reads and pretty-prints all messages from the `orders-dlq` topic.
Useful during the live demo to show which messages failed and why.

Usage:
    python dlq_inspector.py [--from-beginning]

Arguments:
    --from-beginning   Read all DLQ messages from the start (default: True)
    --follow           Keep polling after catching up (like `tail -f`)

The DLQ message format is JSON with these fields:
    original_record  - the deserialized order dict
    error_reason     - human-readable error message
    error_type       - TemporaryError | PermanentError
    retry_count      - number of retries attempted before giving up
    failed_at        - UTC ISO-8601 timestamp
"""

import argparse
import json
import time

from confluent_kafka import Consumer, KafkaError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_DLQ         = "orders-dlq"

# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def print_divider():
    print("-" * 60)

def print_dlq_message(index: int, payload: dict):
    """Pretty-print a single DLQ envelope."""
    record = payload.get("original_record", {})
    print_divider()
    print(f"  DLQ Message #{index}")
    print(f"  Order ID    : {record.get('orderId', 'N/A')}")
    print(f"  Product     : {record.get('product', 'N/A')}")
    print(f"  Price       : ${record.get('price', 0):.2f}")
    print(f"  Error type  : {payload.get('error_type', 'N/A')}")
    print(f"  Error reason: {payload.get('error_reason', 'N/A')}")
    print(f"  Retries     : {payload.get('retry_count', 'N/A')}")
    print(f"  Failed at   : {payload.get('failed_at', 'N/A')}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="DLQ Inspector")
    parser.add_argument("--from-beginning", action="store_true", default=True,
                        help="Read all DLQ messages from the beginning")
    parser.add_argument("--follow", action="store_true", default=False,
                        help="Keep polling for new DLQ messages (Ctrl+C to stop)")
    args = parser.parse_args()

    offset_reset = "earliest" if args.from_beginning else "latest"

    print("=" * 60)
    print(f"  Dead Letter Queue Inspector")
    print(f"  Bootstrap servers : {BOOTSTRAP_SERVERS}")
    print(f"  DLQ topic         : {TOPIC_DLQ}")
    print(f"  Offset reset      : {offset_reset}")
    print(f"  Follow mode       : {args.follow}")
    print("=" * 60 + "\n")

    consumer = Consumer({
        "bootstrap.servers":  BOOTSTRAP_SERVERS,
        "group.id":           "dlq-inspector",
        "auto.offset.reset":  offset_reset,
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_DLQ])

    count         = 0
    idle_polls    = 0
    MAX_IDLE      = 5  # stop after 5 consecutive empty polls if not following

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                idle_polls += 1
                if not args.follow and idle_polls >= MAX_IDLE:
                    break    # caught up — exit unless --follow is set
                continue

            idle_polls = 0   # reset idle counter on any message

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    if not args.follow:
                        break
                else:
                    print(f"[ERROR] {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"[ERROR] Could not decode DLQ message: {e}")
                continue

            count += 1
            print_dlq_message(count, payload)

    except KeyboardInterrupt:
        print("\n[INSPECTOR] Interrupted.")
    finally:
        print_divider()
        if count == 0:
            print("  No messages found in the DLQ.")
        else:
            print(f"  Total DLQ messages: {count}")
        print_divider()
        consumer.close()
        print("[INSPECTOR] Done.")


if __name__ == "__main__":
    main()
