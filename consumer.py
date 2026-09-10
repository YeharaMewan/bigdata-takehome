"""
Avro order consumer.

Implements the three reliability/analytics requirements of the assignment:

  1. REAL-TIME AGGREGATION - a running average of order prices, updated per message.
  2. RETRY LOGIC           - bounded retries with exponential backoff for
                             temporary (transient) failures.
  3. DEAD LETTER QUEUE     - permanently failed messages are forwarded to
                             `orders.DLQ` together with diagnostic headers.

Run:  python consumer.py
"""

import argparse
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import common


# =============================================================================
# Error taxonomy
#
# DECISION    : Model failures as two distinct exception types.
# ALTERNATIVES: One generic Exception plus retry-everything; or inspecting error
#               strings at the call site.
# WHY OPTIMAL : The retry/DLQ decision is a *policy* question ("can retrying
#               possibly help?"). Encoding that in the type system means the
#               dispatch logic is one isinstance() check instead of a growing
#               pile of string matching, and new failure modes just pick a base
#               class. Retrying a PermanentError wastes time and delays the
#               whole partition.
# =============================================================================
class TransientError(Exception):
    """Temporary failure - retrying may succeed (timeout, service unavailable)."""


class PermanentError(Exception):
    """Permanent failure - retrying can never succeed (invalid/corrupt data)."""


# =============================================================================
# Real-time aggregation
# =============================================================================
class RunningAverage:
    """
    Incremental mean of every successfully processed price.

    DECISION    : Welford-style incremental update, O(1) time and O(1) memory.
    ALTERNATIVES: Keep every price in a list and recompute sum/len (O(n) memory);
                  push the aggregation into Kafka Streams or ksqlDB.
    WHY OPTIMAL : The stream is unbounded, so storing every price would grow
                  without limit. Welford's update also avoids the precision loss
                  that a naive running `total` suffers once the sum gets large.
                  Kafka Streams/ksqlDB would add a whole extra service for an
                  aggregate that fits in two variables.
    """

    def __init__(self) -> None:
        self.count = 0
        self.total = 0.0
        self.mean = 0.0

    def add(self, value: float) -> float:
        self.count += 1
        self.total += value
        self.mean += (value - self.mean) / self.count
        return self.mean


# =============================================================================
# Simulated business logic (with deterministic fault injection)
# =============================================================================
# Tracks how many times each flaky order has been attempted, so that FLAKY_ITEM
# can *recover* after a couple of retries - proving the retry loop works rather
# than just proving that failures reach the DLQ.
_flaky_attempts: dict[str, int] = {}

FLAKY_RECOVERS_AFTER = 2  # fails twice, succeeds on the 3rd attempt


def process_order(order: dict) -> None:
    """
    Stand-in for real downstream work (DB write, payment call, ...).

    Raises PermanentError or TransientError to exercise the two failure paths.
    """
    product = order["product"]
    price = order["price"]

    # --- Permanent: data that violates a business rule -----------------------
    if price <= 0 or product == common.PRODUCT_BAD:
        raise PermanentError(f"invalid price {price} for product '{product}'")

    # --- Transient that never recovers --------------------------------------
    if product == common.PRODUCT_ALWAYS_FAIL:
        raise TransientError("downstream service unavailable")

    # --- Transient that recovers after a few attempts ------------------------
    if product == common.PRODUCT_FLAKY:
        seen = _flaky_attempts.get(order["orderId"], 0)
        _flaky_attempts[order["orderId"]] = seen + 1
        if seen < FLAKY_RECOVERS_AFTER:
            raise TransientError(f"temporary glitch (attempt {seen + 1})")

    # Falling through == success.


# =============================================================================
# Retry policy
# =============================================================================
def process_with_retry(order: dict, max_retries: int, base_backoff: float):
    """
    Attempt `process_order` up to (max_retries + 1) times.

    Returns (succeeded, error, attempts).

    DECISION    : Blocking in-consumer retry with exponential backoff.
    ALTERNATIVES: (a) Retry-topic pattern - republish to orders.retry.5s /
                  orders.retry.30s topics consumed after a delay (this is what
                  Spring Kafka's @RetryableTopic automates).
                  (b) Rely on Kafka's producer-side retries.
    WHY OPTIMAL : (b) only covers *publish* failures, not consumer-side
                  processing, so it cannot satisfy the requirement at all.
                  (a) is the production-grade answer because it never blocks the
                  partition - but it needs extra topics, extra consumers and
                  makes the live demo much harder to narrate. For this
                  assignment's scale the blocking loop is simpler, fully
                  self-contained and visibly demonstrates recovery.
    TRADE-OFF   : While we sleep, this partition makes no progress. Total backoff
                  here is 0.5+1+2 = 3.5s, far below Kafka's max.poll.interval.ms
                  (5 min), so the consumer is never kicked out of the group. If
                  the backoffs grew large, the retry-topic pattern would become
                  the correct choice.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            process_order(order)
            return True, None, attempt

        except PermanentError as exc:
            # Short-circuit: retrying invalid data can never help.
            return False, exc, attempt

        except TransientError as exc:
            if attempt > max_retries:
                return False, exc, attempt
            backoff = base_backoff * (2 ** (attempt - 1))  # 0.5s -> 1s -> 2s
            print(f"        [RETRY {attempt}/{max_retries}] {exc} "
                  f"- backing off {backoff:.1f}s")
            time.sleep(backoff)


# =============================================================================
# Dead Letter Queue
# =============================================================================
def send_to_dlq(dlq_producer: Producer, msg, error: Exception,
                attempts: int, error_kind: str) -> None:
    """
    Forward a failed message to `orders.DLQ`.

    DECISION    : Publish the ORIGINAL raw bytes and put diagnostics in headers.
    ALTERNATIVES: Re-serialize the decoded order into a richer "failure" Avro
                  record; or log-and-skip; or write failures to a database.
    WHY OPTIMAL : Forwarding raw bytes works even when the failure *was* the
                  deserialization step (there is no decoded object to re-encode),
                  and it keeps the record byte-for-byte replayable - you can
                  later pipe the DLQ straight back into `orders` once the bug is
                  fixed. Headers carry the diagnostics without polluting the
                  business schema, so order.avsc stays exactly as specified.
    """
    headers = [
        ("x-original-topic", msg.topic().encode("utf-8")),
        ("x-original-partition", str(msg.partition()).encode("utf-8")),
        ("x-original-offset", str(msg.offset()).encode("utf-8")),
        ("x-error-type", error_kind.encode("utf-8")),
        ("x-error-message", str(error)[:500].encode("utf-8")),
        ("x-attempts", str(attempts).encode("utf-8")),
        ("x-failed-at", datetime.now(timezone.utc).isoformat().encode("utf-8")),
    ]

    # Carry forward any replay provenance (x-replay-count, x-replayed-at) set by
    # dlq_replay.py. Without this a message that fails again after a replay would
    # come back with a fresh header set, its replay counter reset, and could be
    # replayed forever - an infinite DLQ -> orders -> DLQ loop.
    for key, value in (msg.headers() or []):
        if key.startswith("x-replay") and value is not None:
            headers.append((key, value))

    dlq_producer.produce(
        topic=common.TOPIC_DLQ,
        key=msg.key(),
        value=msg.value(),   # untouched original payload
        headers=headers,
    )
    # Block until the DLQ write is acknowledged. This MUST happen before we
    # commit the source offset, otherwise a crash in between would lose the
    # message entirely.
    dlq_producer.flush(timeout=10)


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    parser = argparse.ArgumentParser(description="Avro order consumer")
    parser.add_argument("--max-retries", type=int, default=3,
                        help="retries after the first attempt (default: 3)")
    parser.add_argument("--backoff", type=float, default=0.5,
                        help="base backoff in seconds, doubled each retry "
                             "(default: 0.5)")
    parser.add_argument("--group", default=common.CONSUMER_GROUP,
                        help="consumer group id")
    parser.add_argument("--from-beginning", action="store_true",
                        help="read the topic from the start")
    parser.add_argument("--idle-timeout", type=float, default=0.0,
                        help="exit after N seconds with no new messages, "
                             "printing the final report (0 = run until Ctrl+C)")
    args = parser.parse_args()

    try:
        schema_str = common.load_schema_str()
        sr_client = SchemaRegistryClient({"url": common.SCHEMA_REGISTRY_URL})
        avro_deserializer = AvroDeserializer(sr_client, schema_str)
    except Exception as exc:
        print(common.friendly_startup_error(exc))
        return 1

    # --- Consumer configuration ----------------------------------------------
    # DECISION    : enable.auto.commit = False (manual commits).
    # ALTERNATIVES: Leave auto-commit on (the default, every 5s).
    # WHY OPTIMAL : Auto-commit can acknowledge an offset while the message is
    #               still being retried. A crash at that moment would silently
    #               drop the order - it is neither processed nor in the DLQ.
    #               Committing by hand, only after success-or-DLQ, gives us
    #               at-least-once delivery with no silent data loss.
    consumer = Consumer({
        "bootstrap.servers": common.BOOTSTRAP_SERVERS,
        "group.id": args.group,
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": False,
    })

    # A separate plain Producer for the DLQ. No Avro serializer here: we forward
    # the original bytes verbatim.
    dlq_producer = Producer({
        "bootstrap.servers": common.BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
    })

    consumer.subscribe([common.TOPIC_ORDERS])

    avg = RunningAverage()
    stats = {"ok": 0, "dlq_permanent": 0, "dlq_exhausted": 0, "dlq_undecodable": 0}

    print("=" * 78)
    print(f"CONSUMER  group='{args.group}'  topic='{common.TOPIC_ORDERS}'  "
          f"dlq='{common.TOPIC_DLQ}'")
    print(f"  max_retries={args.max_retries}  base_backoff={args.backoff}s"
          f"  offset_reset={'earliest' if args.from_beginning else 'latest'}")
    print("  Ctrl+C to stop")
    print("=" * 78)

    poll_timeout = 1.0
    idle = 0.0

    try:
        while True:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                # No traffic. With --idle-timeout the consumer drains the
                # backlog and then stops on its own, which makes the run
                # scriptable and prints the final report without a Ctrl+C.
                idle += poll_timeout
                if args.idle_timeout and idle >= args.idle_timeout:
                    print(f"\n[CONSUMER] idle for {args.idle_timeout:.0f}s - stopping")
                    break
                continue

            idle = 0.0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[KAFKA-ERROR] {msg.error()}")
                continue

            # --- 1. Deserialize --------------------------------------------
            # A decoding failure is permanent by definition: the same bytes will
            # never decode differently, so it goes straight to the DLQ.
            try:
                order = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
            except Exception as exc:
                print(f"[DLQ] offset={msg.offset()} undecodable payload: {exc}")
                send_to_dlq(dlq_producer, msg, exc, 1, "DeserializationError")
                stats["dlq_undecodable"] += 1
                consumer.commit(message=msg, asynchronous=False)
                continue

            oid = order["orderId"]
            product = order["product"]
            price = float(order["price"])

            print(f"[RECV] orderId={oid:<6} product={product:<12} "
                  f"price={price:>8.2f}")

            # --- 2. Process with retry --------------------------------------
            ok, err, attempts = process_with_retry(
                order, args.max_retries, args.backoff
            )

            # --- 3. Success -> update the real-time aggregate ---------------
            if ok:
                current = avg.add(price)
                stats["ok"] += 1
                print(f"        [OK after {attempts} attempt(s)]  "
                      f"processed={avg.count}  RUNNING AVG = {current:.2f}")

            # --- 3b. Failure -> Dead Letter Queue ---------------------------
            else:
                if isinstance(err, PermanentError):
                    kind = "PermanentError"
                    stats["dlq_permanent"] += 1
                else:
                    kind = "TransientError(retries exhausted)"
                    stats["dlq_exhausted"] += 1

                send_to_dlq(dlq_producer, msg, err, attempts, kind)
                print(f"        [-> DLQ] {kind} after {attempts} attempt(s): {err}")

            # --- 4. Commit -------------------------------------------------
            # Reached only once the message is either processed OR safely stored
            # in the DLQ.
            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        print("\n[CONSUMER] interrupted by user")
    finally:
        dlq_producer.flush(timeout=10)
        consumer.close()

        print("-" * 78)
        print("FINAL REPORT")
        print(f"  processed successfully : {stats['ok']}")
        print(f"  -> DLQ (permanent)     : {stats['dlq_permanent']}")
        print(f"  -> DLQ (retries gone)  : {stats['dlq_exhausted']}")
        print(f"  -> DLQ (undecodable)   : {stats['dlq_undecodable']}")
        print(f"  RUNNING AVERAGE PRICE  : {avg.mean:.2f}  (over {avg.count} orders)")
        print("-" * 78)

    return 0


if __name__ == "__main__":
    sys.exit(main())
