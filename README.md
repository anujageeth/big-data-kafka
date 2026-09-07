# Kafka Order Processing System

A Kafka-based producer/consumer pipeline for order messages, built as part of the **Big Data Analytics — Chapter 3 Assignment**.

---

## Architecture

```
Producer                     Kafka Broker                Consumer
--------                     ------------                --------
make_order()                                             poll()
serialize (Avro)  ──────►  topic: orders  ──────────►   deserialize (Avro)
                                                         process_with_retry()
                                ▼                              │
                         topic: orders-dlq  ◄─── (DLQ) ───────┘
                                │
                         dlq_inspector.py
```

**Key flow:**
1. **Producer** generates random order messages and publishes them to the `orders` topic, serialized with Avro.
2. **Consumer** reads from `orders`, attempts to process each message up to 3 times with exponential backoff.
3. If all retries fail, the message is published to the `orders-dlq` topic as a JSON envelope.
4. **DLQ Inspector** is a standalone consumer that reads and pretty-prints `orders-dlq` contents.

---

## Project Structure

```
big-data-kafka/
├── docker-compose.yml       # Kafka (KRaft), Schema Registry, Kafka UI
├── requirements.txt         # Python dependencies
├── .gitignore
├── schema/
│   └── order.avsc           # Avro schema definition
├── producer/
│   └── producer.py          # Order producer with failure simulation
├── consumer/
│   └── consumer.py          # Consumer with retry + DLQ + running average
└── dlq/
    └── dlq_inspector.py     # Standalone DLQ reader/inspector
```

---

## Setup & Running

### 1. Start Kafka

```bash
docker compose up -d
```

Verify containers are running:
```bash
docker ps
```

Open Kafka UI at **http://localhost:8080** to monitor topics and messages.

### 2. Create Topics

```bash
docker exec -it kafka kafka-topics --create --topic orders \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --create --topic orders-dlq \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Install Python Dependencies

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

pip install -r requirements.txt
```

### 4. Run the Consumer (Terminal 1)

```bash
python consumer/consumer.py
```

With a custom group ID:
```bash
python consumer/consumer.py --group my-group
```

### 5. Run the Producer (Terminal 2)

Normal mode (20 messages, 1s apart):
```bash
python producer/producer.py
```

Custom count and delay:
```bash
python producer/producer.py --count 50 --delay 0.5
```

### 6. Demo with Failure Modes

Temporary failures (retries then succeeds):
```bash
python producer/producer.py --failure-mode temporary --count 5
```

Permanent failures (retries exhaust → DLQ):
```bash
python producer/producer.py --failure-mode permanent --count 5
```

Random mix:
```bash
python producer/producer.py --failure-mode random --count 10
```

### 7. Inspect the DLQ

```bash
python dlq/dlq_inspector.py
```

Keep following new DLQ messages in real time:
```bash
python dlq/dlq_inspector.py --follow
```

### 8. Stop Kafka

```bash
docker compose down
# Add -v to also wipe stored topic data:
docker compose down -v
```

---

## Avro Schema (`schema/order.avsc`)

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.bigdata.kafka",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price",   "type": "float"}
  ]
}
```

| Field   | Type   | Description                               |
|---------|--------|-------------------------------------------|
| orderId | string | Unique order identifier (e.g., "1001")    |
| product | string | Name of purchased item (e.g., "Laptop")   |
| price   | float  | Price of the product (randomized, $5–500) |

---

## Design Decisions

### Serialization
- **Avro with schemaless encoding** (`fastavro.schemaless_writer` / `schemaless_reader`).
- The schema is loaded from `schema/order.avsc` and embedded in the code — no external Schema Registry lookup required. This simplifies the setup for a course project.

### Retry Strategy
- **Max retries:** 3 (configurable via `MAX_RETRIES` constant)
- **Backoff:** exponential — 1s after attempt 1, 2s after attempt 2
- **Temporary failures** are retried up to `MAX_RETRIES` times
- **Permanent failures** immediately skip remaining retries and go to DLQ

### Failure Classification
| Error Type      | Definition                          | Behaviour            |
|-----------------|-------------------------------------|----------------------|
| TemporaryError  | Transient — can succeed on retry    | Retry with backoff   |
| PermanentError  | Unrecoverable — retrying is useless | Immediate DLQ        |

### Running Average
- **Only successfully processed messages** count toward the running average.
- DLQ messages are explicitly excluded. This ensures the aggregate reflects real, valid order throughput, not failed noise.

### DLQ Message Format
- DLQ messages are **JSON-encoded** (not Avro).
- Each envelope contains: `original_record`, `error_reason`, `error_type`, `retry_count`, `failed_at`.
- Choice rationale: JSON is human-readable and trivially inspectable without a schema; Avro would add overhead for what is essentially a diagnostic/ops payload.

### Failure Simulation (for Demo)
- Controlled via `--failure-mode` CLI argument or `FAILURE_MODE` environment variable.
- Modes: `off` | `temporary` | `permanent` | `random`
- The producer attaches the mode as a Kafka **message header** (`failure_mode`), so the consumer knows exactly how to behave — enabling fully deterministic, repeatable demos without relying on random chance.

---

## Known Limitations

- **Single partition, replication factor 1** — not suitable for production; fine for a course demo.
- **No message persistence beyond container lifetime** — `docker compose down -v` wipes all topic data.
- **Schema versioning not implemented** — the schema is embedded directly; an incompatible schema change would require redeploying both producer and consumer.
- **No SSL/authentication** — `PLAINTEXT` listeners only, suitable for a local dev/course environment.
- **In-memory aggregation** — the running average is lost if the consumer restarts; a production system would persist state to a store (e.g., Redis, Kafka Streams state store).

---

## Live Demo Checklist

1. `docker compose up -d` → confirm all 3 containers healthy
2. Open **http://localhost:8080** → show empty `orders` and `orders-dlq` topics
3. Terminal 1: `python consumer/consumer.py` → idle, waiting for messages
4. Terminal 2: `python producer/producer.py` → watch running average update live
5. Terminal 2: `python producer/producer.py --failure-mode temporary --count 5` → show retry logs in consumer terminal
6. Terminal 2: `python producer/producer.py --failure-mode permanent --count 5` → show retries exhaust → DLQ
7. Terminal 3: `python dlq/dlq_inspector.py` → pretty-print DLQ messages with error metadata
8. Summarise: schema, retry strategy, DLQ strategy, running average exclusion rule
