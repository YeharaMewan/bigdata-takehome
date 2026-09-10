"""
Dead Letter Queue inspector.

Reads `orders.DLQ` and prints each failed message together with the diagnostic
headers the consumer attached, decoding the original Avro payload where possible.

DECISION    : A tiny read-only tool rather than a `kafka-console-consumer` call.
ALTERNATIVES: `docker exec kafka kafka-console-consumer ...` or Confluent
              Control Center / AKHQ (a web UI).
WHY OPTIMAL : The console consumer prints Avro as unreadable binary and cannot
              show headers nicely; a web UI is a heavyweight extra container.
              This script decodes the payload AND the headers, which is exactly
              what you need to point at during the live demo.

Run:  python dlq_inspector.py            # one-shot report, then exit
      python dlq_inspector.py --follow   # keep watching
"""

import argparse
import sys

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import common


def decode_headers(msg) -> dict:
    """Kafka headers arrive as a list of (key, bytes) tuples."""
    out = {}
    for key, value in (msg.headers() or []):
        try:
            out[key] = value.decode("utf-8") if value is not None else ""
        except Exception:
            out[key] = repr(value)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect the orders.DLQ topic")
    parser.add_argument("--follow", action="store_true",
                        help="keep watching instead of exiting when idle")
    parser.add_argument("--idle-timeout", type=float, default=5.0,
                        help="seconds of silence before exiting (default: 5)")
    args = parser.parse_args()

    try:
        schema_str = common.load_schema_str()
        sr_client = SchemaRegistryClient({"url": common.SCHEMA_REGISTRY_URL})
        avro_deserializer = AvroDeserializer(sr_client, schema_str)
    except Exception as exc:
        print(common.friendly_startup_error(exc))
        return 1

    # A distinct group id, and we never commit: the inspector is a read-only
    # observer and must not disturb the real consumer's offsets.
    consumer = Consumer({
        "bootstrap.servers": common.BOOTSTRAP_SERVERS,
        "group.id": "dlq-inspector",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([common.TOPIC_DLQ])

    print("=" * 78)
    print(f"DLQ INSPECTOR - topic '{common.TOPIC_DLQ}' @ {common.BOOTSTRAP_SERVERS}")
    print("=" * 78)

    found = 0
    idle = 0.0
    poll_timeout = 1.0

    try:
        while True:
            msg = consumer.poll(poll_timeout)

            if msg is None:
                idle += poll_timeout
                if not args.follow and idle >= args.idle_timeout:
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[KAFKA-ERROR] {msg.error()}")
                continue

            idle = 0.0
            found += 1
            hdr = decode_headers(msg)

            # The DLQ value is the untouched original payload, so the same Avro
            # deserializer works - unless the payload was corrupt to begin with.
            try:
                order = avro_deserializer(
                    msg.value(),
                    SerializationContext(common.TOPIC_ORDERS, MessageField.VALUE),
                )
                payload = (f"orderId={order['orderId']}  "
                           f"product={order['product']}  "
                           f"price={float(order['price']):.2f}")
            except Exception as exc:
                payload = f"<undecodable: {exc}>  raw={msg.value()[:40]!r}"

            print(f"\n--- DLQ message #{found} "
                  f"(dlq offset {msg.offset()}) ".ljust(78, "-"))
            print(f"  payload      : {payload}")
            print(f"  error type   : {hdr.get('x-error-type', '?')}")
            print(f"  error message: {hdr.get('x-error-message', '?')}")
            print(f"  attempts     : {hdr.get('x-attempts', '?')}")
            print(f"  origin       : {hdr.get('x-original-topic', '?')}"
                  f"[{hdr.get('x-original-partition', '?')}]"
                  f"@{hdr.get('x-original-offset', '?')}")
            print(f"  failed at    : {hdr.get('x-failed-at', '?')}")

    except KeyboardInterrupt:
        print("\n[DLQ-INSPECTOR] interrupted by user")
    finally:
        consumer.close()

    print("\n" + "-" * 78)
    print(f"[DLQ-INSPECTOR] {found} failed message(s) in {common.TOPIC_DLQ}")
    print("-" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
