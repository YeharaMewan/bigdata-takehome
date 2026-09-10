"""
Dead Letter Queue replay tool.

Reads `orders.DLQ` and republishes the failed messages back onto `orders` so the
consumer can process them again - the operational workflow you follow after
fixing whatever caused the failure.

DECISION    : Replay the ORIGINAL bytes and add provenance headers.
ALTERNATIVES: (a) Decode -> re-encode the order before republishing.
              (b) Let the consumer retry forever instead of using a DLQ at all.
              (c) Manual re-entry / a Kafka Streams reprocessing topology.
WHY OPTIMAL : (a) fails for messages whose *decoding* was the problem, and any
              re-encode risks changing the payload. (b) blocks the partition
              indefinitely on a poison message. Forwarding the untouched bytes
              means replay is lossless and works for every failure class.

LOOP SAFETY : A replayed message that fails again lands back in the DLQ. Without
              a counter you could replay it forever. Each replay stamps
              `x-replay-count`; consumer.py preserves that header, and this tool
              refuses to replay past `--max-replays`.

Run:  python dlq_replay.py --dry-run          # show what WOULD be replayed
      python dlq_replay.py --only transient   # replay just the retryable ones
"""

import argparse
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer

import common

HDR_REPLAY_COUNT = "x-replay-count"
HDR_REPLAYED_AT = "x-replayed-at"


def decode_headers(msg) -> dict:
    out = {}
    for key, value in (msg.headers() or []):
        try:
            out[key] = value.decode("utf-8") if value is not None else ""
        except Exception:
            out[key] = ""
    return out


def classify(error_type: str) -> str:
    """Map the consumer's error label onto 'transient' or 'permanent'."""
    return "transient" if "TransientError" in error_type else "permanent"


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay orders.DLQ back into orders")
    parser.add_argument("--only", choices=["all", "transient", "permanent"],
                        default="all",
                        help="which failures to replay (default: all). "
                             "'transient' is the usual choice after a downstream "
                             "service recovers; 'permanent' only makes sense once "
                             "the data or the business rule has changed.")
    parser.add_argument("--max-replays", type=int, default=1,
                        help="refuse to replay a message more than this many "
                             "times (default: 1) - prevents infinite loops")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would happen; publish nothing, commit nothing")
    parser.add_argument("--idle-timeout", type=float, default=5.0,
                        help="stop after N seconds with no new DLQ messages")
    parser.add_argument("--group", default="dlq-replayer",
                        help="consumer group id (a new name rescans the whole DLQ)")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": common.BOOTSTRAP_SERVERS,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        # Manual commits: we only advance past a message once it has actually
        # been republished (or deliberately skipped).
        "enable.auto.commit": False,
    })

    producer = Producer({
        "bootstrap.servers": common.BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
    })

    consumer.subscribe([common.TOPIC_DLQ])

    mode = "DRY RUN (nothing will be published)" if args.dry_run else "LIVE"
    print("=" * 78)
    print(f"DLQ REPLAY  {common.TOPIC_DLQ} -> {common.TOPIC_ORDERS}   [{mode}]")
    print(f"  filter=--only {args.only}   max_replays={args.max_replays}   "
          f"group={args.group}")
    print("=" * 78)

    stats = {"replayed": 0, "skipped_filter": 0, "skipped_limit": 0}
    idle = 0.0
    poll_timeout = 1.0

    try:
        while True:
            msg = consumer.poll(poll_timeout)

            if msg is None:
                idle += poll_timeout
                if idle >= args.idle_timeout:
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[KAFKA-ERROR] {msg.error()}")
                continue

            idle = 0.0
            hdr = decode_headers(msg)
            error_type = hdr.get("x-error-type", "unknown")
            kind = classify(error_type)
            origin = (f"{hdr.get('x-original-topic', '?')}"
                      f"[{hdr.get('x-original-partition', '?')}]"
                      f"@{hdr.get('x-original-offset', '?')}")

            try:
                replay_count = int(hdr.get(HDR_REPLAY_COUNT, "0"))
            except ValueError:
                replay_count = 0

            # --- Filter by failure class ---------------------------------
            if args.only != "all" and kind != args.only:
                stats["skipped_filter"] += 1
                print(f"[SKIP  ] {origin}  ({kind}, filtered out)")
                if not args.dry_run:
                    consumer.commit(message=msg, asynchronous=False)
                continue

            # --- Loop protection -----------------------------------------
            if replay_count >= args.max_replays:
                stats["skipped_limit"] += 1
                print(f"[SKIP  ] {origin}  already replayed {replay_count}x "
                      f"(limit {args.max_replays}) - needs manual attention")
                if not args.dry_run:
                    consumer.commit(message=msg, asynchronous=False)
                continue

            # --- Replay ---------------------------------------------------
            if args.dry_run:
                stats["replayed"] += 1
                print(f"[WOULD ] {origin}  {error_type}  "
                      f"(replay #{replay_count + 1})")
                continue

            headers = [
                (HDR_REPLAY_COUNT, str(replay_count + 1).encode("utf-8")),
                (HDR_REPLAYED_AT,
                 datetime.now(timezone.utc).isoformat().encode("utf-8")),
            ]
            producer.produce(
                topic=common.TOPIC_ORDERS,
                key=msg.key(),
                value=msg.value(),   # untouched original payload
                headers=headers,
            )
            # Publish before committing: if we crash in between the message is
            # simply replayed twice, which is far better than losing it.
            producer.flush(timeout=10)
            consumer.commit(message=msg, asynchronous=False)

            stats["replayed"] += 1
            print(f"[REPLAY] {origin}  {error_type}  -> {common.TOPIC_ORDERS} "
                  f"(replay #{replay_count + 1})")

    except KeyboardInterrupt:
        print("\n[DLQ-REPLAY] interrupted by user")
    finally:
        producer.flush(timeout=10)
        consumer.close()

    verb = "would replay" if args.dry_run else "replayed"
    print("-" * 78)
    print(f"  {verb:<20} : {stats['replayed']}")
    print(f"  {'skipped (filter)':<20} : {stats['skipped_filter']}")
    print(f"  {'skipped (replay cap)':<20} : {stats['skipped_limit']}")
    print("-" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
