"""
schema/test_schema.py
---------------------
Avro Schema Validation Test

Verifies that the order.avsc schema is valid by doing a round-trip:
serialize a sample record → deserialize it → compare to original.

Run this BEFORE wiring the schema into Kafka to confirm it works:
    python schema/test_schema.py
"""

import io
import os

import fastavro

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "order.avsc")

# Sample records to validate against the schema
SAMPLE_RECORDS = [
    {"orderId": "1001", "product": "Laptop",      "price": 999.99},
    {"orderId": "1002", "product": "Mouse",        "price": 25.50},
    {"orderId": "1003", "product": "USB Hub",      "price": 5.0},
    {"orderId": "1004", "product": "Monitor",      "price": 499.0},
]


def round_trip(schema, record: dict) -> dict:
    """Serialize then deserialize a record and return the result."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    buf.seek(0)
    return fastavro.schemaless_reader(buf, schema)


def main():
    print("=" * 50)
    print("  Avro Schema Round-Trip Validation")
    print(f"  Schema: {SCHEMA_PATH}")
    print("=" * 50)

    schema = fastavro.schema.load_schema(SCHEMA_PATH)
    print(f"  Schema loaded OK: {schema['name']}\n")

    all_passed = True
    for record in SAMPLE_RECORDS:
        result   = round_trip(schema, record)
        passed   = result == record
        status   = "PASS" if passed else "FAIL"
        all_passed = all_passed and passed
        print(f"  [{status}] orderId={record['orderId']} "
              f"product={record['product']} "
              f"price={record['price']}")
        if not passed:
            print(f"         Expected : {record}")
            print(f"         Got      : {result}")

    print()
    if all_passed:
        print("  All tests passed. Schema is valid.")
    else:
        print("  Some tests FAILED. Check the schema definition.")
    print("=" * 50)


if __name__ == "__main__":
    main()
