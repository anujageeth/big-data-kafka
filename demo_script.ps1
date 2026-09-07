# =============================================================
#  LIVE DEMO SCRIPT — Kafka Order Processing System
#  Big Data Analytics — Chapter 3 Assignment
# =============================================================
#
#  Run each step in sequence. Commands shown for Windows PowerShell.
#  Open separate terminals where indicated.
#
# =============================================================

# --------------------------------------------------
# PRE-CHECK (run once before demo starts)
# --------------------------------------------------

# Confirm Docker is running
docker ps

# Confirm Kafka UI is accessible
#   → Open browser: http://localhost:8080
#   → You should see cluster "local" with 0 or existing topics

# --------------------------------------------------
# STEP 1 — Start Kafka (if not already running)
# --------------------------------------------------

docker compose up -d

# Wait ~15 seconds for Kafka to be ready, then verify:
docker ps
# Expected: kafka, schema-registry, kafka-ui  →  STATUS: Up

# --------------------------------------------------
# STEP 2 — Create Topics (run once)
# --------------------------------------------------

docker exec -it kafka kafka-topics --create `
  --topic orders `
  --bootstrap-server localhost:9092 `
  --partitions 1 `
  --replication-factor 1

docker exec -it kafka kafka-topics --create `
  --topic orders-dlq `
  --bootstrap-server localhost:9092 `
  --partitions 1 `
  --replication-factor 1

# Verify topics exist:
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# --------------------------------------------------
# STEP 3 — Validate the Avro Schema
# --------------------------------------------------

# In your project directory:
python schema/test_schema.py
# Expected output: 4x [PASS] — Schema is valid.

# --------------------------------------------------
# STEP 4 — Normal Operation (TERMINAL 1 + TERMINAL 2)
# --------------------------------------------------

# TERMINAL 1: Start the consumer (leave running)
python consumer/consumer.py

# TERMINAL 2: Send 10 messages, 1.5s apart — normal mode
python producer/producer.py --count 10 --delay 1.5

# Demo talking points:
#   - Producer serializes with Avro and publishes to `orders`
#   - Consumer deserializes and updates running average in real time
#   - Show Kafka UI: Topics → orders → Messages tab (message count rising)

# --------------------------------------------------
# STEP 5 — Retry Logic Demo (temporary failures)
# --------------------------------------------------

# TERMINAL 2: Send 5 messages in temporary-failure mode
python producer/producer.py --failure-mode temporary --count 5 --delay 2.0

# Demo talking points (watch TERMINAL 1):
#   - Attempt 1 fails → "Simulated transient error (attempt 1/3)"
#   - Backs off 1s → Attempt 2 fails → backs off 2s
#   - Attempt 3 succeeds → message counted in running average
#   - This simulates a downstream service that recovers after a brief outage

# --------------------------------------------------
# STEP 6 — DLQ Demo (permanent failures)
# --------------------------------------------------

# TERMINAL 2: Send 5 messages in permanent-failure mode
python producer/producer.py --failure-mode permanent --count 5 --delay 2.0

# Demo talking points (watch TERMINAL 1):
#   - All 3 attempts fail immediately (PermanentError — no point retrying)
#   - Message is sent to orders-dlq with error metadata
#   - Running average is NOT updated (DLQ messages excluded)

# TERMINAL 3: Inspect DLQ contents
python dlq/dlq_inspector.py

# Show in Kafka UI: Topics → orders-dlq → Messages tab

# --------------------------------------------------
# STEP 7 — Random Mix Demo (optional)
# --------------------------------------------------

# TERMINAL 2:
python producer/producer.py --failure-mode random --count 10 --delay 1.5

# Mix of successes, retried-then-succeeded, and DLQ messages

# --------------------------------------------------
# STEP 8 — Shutdown
# --------------------------------------------------

# Stop Kafka (keep data):
docker compose down

# Stop Kafka + wipe all topic data:
docker compose down -v
