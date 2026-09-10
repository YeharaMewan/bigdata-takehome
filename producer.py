"""
Avro order producer.

Generates randomized order messages, serializes them with Avro (schema managed by
Confluent Schema Registry) and publishes them to the `orders` topic.

Run:  python producer.py --count 20 --interval 0.5
"""

import argparse
import random
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

import common


# -----------------------------------------------------------------------------
# Message construction
# -----------------------------------------------------------------------------
def build_order(order_id: int, poison_rate: float) -> dict:
    """
    Build one order matching order.avsc: {orderId: string, product: string, price: float}.

    A fraction of the orders (`poison_rate`) use a "poison" product name so the
    consumer's retry and DLQ paths can be demonstrated.
    """
    if random.random() < poison_rate:
        product = random.choice(common.POISON_PRODUCTS)
    else:
        product = random.choice(common.NORMAL_PRODUCTS)

    price = round(random.uniform(10.0, 500.0), 2)

    # BAD_ITEM carries an invalid price so it violates a business rule that no
    # amount of retrying can fix -> the consumer classifies it as PERMANENT.
    if product == common.PRODUCT_BAD:
        price = -1.0

    # NOTE: orderId is a *string* per the schema, even though we count with ints.
    return {"orderId": str(order_id), "product": product, "price": price}


def delivery_report(err, msg) -> None:
    """
    Called once per message when the broker acknowledges it (or gives up).

    DECISION    : Use an async delivery callback instead of a blocking send.
    ALTERNATIVES: producer.flush() after every single message (synchronous).
    WHY OPTIMAL : librdkafka batches messages in the background; flushing per
                  message would destroy throughput. The callback still gives us
                  per-message confirmation for the demo.
    """
    if err is not None:
        print(f"  [DELIVERY-FAILED] {err}")
    else:
        print(
            f"  [ACKED] partition={msg.partition()} offset={msg.offset()}"
        )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Avro order producer")
    parser.add_argument("--count", type=int, default=20,
                        help="how many orders to send (default: 20)")
    parser.add_argument("--interval", type=float, default=0.5,
                        help="seconds between orders (default: 0.5)")
    parser.add_argument("--poison-rate", type=float, default=0.25,
                        help="fraction of orders that trigger retry/DLQ paths "
                             "(0.0 = clean run, default: 0.25)")
    parser.add_argument("--start-id", type=int, default=1001,
                        help="first orderId (default: 1001)")
    args = parser.parse_args()

    # --- Avro + Schema Registry ----------------------------------------------
    # DECISION    : AvroSerializer backed by Schema Registry.
    # ALTERNATIVES: fastavro with the schema embedded in every message; JSON.
    # WHY OPTIMAL : The registry stores the schema once and the wire format
    #               carries only a 4-byte schema id, so messages stay small AND
    #               the consumer is guaranteed to read a compatible schema.
    #               On first send the schema is auto-registered under the
    #               subject "orders-value".
    try:
        schema_str = common.load_schema_str()
        sr_client = SchemaRegistryClient({"url": common.SCHEMA_REGISTRY_URL})
        avro_serializer = AvroSerializer(sr_client, schema_str)
    except Exception as exc:
        print(common.friendly_startup_error(exc))
        return 1

    key_serializer = StringSerializer("utf_8")

    # --- Producer configuration ----------------------------------------------
    # acks=all + enable.idempotence: the broker confirms the write on every
    # in-sync replica and librdkafka de-duplicates its own internal retries, so
    # a transient network blip cannot silently duplicate or lose an order.
    producer = Producer({
        "bootstrap.servers": common.BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,          # tiny batching window - better throughput
    })

    print("=" * 72)
    print(f"PRODUCER -> topic '{common.TOPIC_ORDERS}' @ {common.BOOTSTRAP_SERVERS}")
    print(f"  orders={args.count}  interval={args.interval}s  "
          f"poison_rate={args.poison_rate}")
    print("=" * 72)

    sent = 0
    try:
        for i in range(args.count):
            order = build_order(args.start_id + i, args.poison_rate)

            # The key is the orderId: Kafka hashes it to pick a partition, so all
            # events for one order always land on the same partition (ordering).
            producer.produce(
                topic=common.TOPIC_ORDERS,
                key=key_serializer(
                    order["orderId"],
                    SerializationContext(common.TOPIC_ORDERS, MessageField.KEY),
                ),
                value=avro_serializer(
                    order,
                    SerializationContext(common.TOPIC_ORDERS, MessageField.VALUE),
                ),
                on_delivery=delivery_report,
            )
            sent += 1
            print(f"[SENT] orderId={order['orderId']:<6} "
                  f"product={order['product']:<12} price={order['price']:>8.2f}")

            # poll(0) serves the delivery callbacks without blocking.
            producer.poll(0)
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n[PRODUCER] interrupted by user")
    except Exception as exc:
        print(common.friendly_startup_error(exc))
        return 1
    finally:
        # flush() blocks until every buffered message has been acknowledged.
        remaining = producer.flush(timeout=10)
        if remaining:
            print(f"[WARN] {remaining} message(s) were not delivered")

    print("-" * 72)
    print(f"[PRODUCER] done. {sent} order(s) sent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
