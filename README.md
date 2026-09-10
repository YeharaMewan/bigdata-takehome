# Kafka Order-Processing System (Avro · Retry · DLQ · Real-time Average)

A Kafka-based system that **produces** and **consumes** order messages using
**Avro** serialization, demonstrating four production concerns:

1. **Real-time aggregation** — a running average of order prices, updated per message.
2. **Retry logic** — bounded retries with exponential backoff for *temporary* failures.
3. **Dead Letter Queue (DLQ)** — a dedicated topic for *permanently* failed messages.
4. **Live demonstration** — deterministic fault injection makes every path reproducible.

> Course: EC8202 – Big Data and Analytics.

---

## Architecture

```
┌──────────────┐   Avro    ┌─────────────────┐   Avro    ┌──────────────┐
│   Producer   │──────────▶│  Kafka topic:   │──────────▶│   Consumer   │
│ (random      │  encode   │     orders      │  decode   │  process()   │
│  orders)     │           └─────────────────┘           └──────┬───────┘
└──────┬───────┘                    ▲                           │
       │ register/lookup            │ schema-id                 │
       │        ┌───────────────────┴─────────┐                 │
       └───────▶│      Schema Registry         │◀───────────────┤
                │   (order.avsc registered)    │                 │
                └──────────────────────────────┘   ┌─────────────┼─────────────┐
                                                    ▼             ▼             ▼
                                                 success      temporary     permanent
                                                    │          failure       failure
                                                    ▼             │             │
                                            running average    retry × 3       │
                                             (Welford)         (0.5→1→2s)      │
                                                                   │ exhausted  │
                                                                   ▼            ▼
                                                          ┌──────────────────────────┐
                                                          │   DLQ topic: orders.DLQ  │
                                                          │   (+ error headers)      │
                                                          └──────────────────────────┘
```

---

## Technology decisions (Decision · Alternatives · Why optimal)

| Concern | Decision | Alternatives considered | Why this is optimal |
|---|---|---|---|
| **Language** | Python + `confluent-kafka` | Java (Spring Kafka), Node.js (kafkajs) | First-class Avro + Schema Registry support, least boilerplate, clearest live demo. |
| **Broker** | Docker Compose, `cp-kafka` in **KRaft** mode | Confluent Cloud, manual tarball install | One command, fully reproducible, no Zookeeper, easy reset. |
| **Serialization** | Avro via **Confluent Schema Registry** | Registry-less Avro (fastavro), JSON/Protobuf | Assignment requires Avro; compact wire format (4-byte schema id) + validation + evolution. |
| **Aggregation** | In-consumer Welford incremental mean | Kafka Streams, ksqlDB, store-all-and-recompute | O(1) memory on an unbounded stream; numerically stable; no extra service. |
| **Retry** | In-consumer bounded retry + exponential backoff | Retry-topic pattern, producer-side retries | Producer retries don't cover consumer processing at all. Retry-topics are the production answer but need extra topics/consumers and are hard to narrate live. |
| **DLQ** | `orders.DLQ` topic, **original bytes** + error headers | Re-serialized failure record, log-and-skip, DB table | Works even when *deserialization* failed; keeps the record byte-for-byte replayable; keeps `order.avsc` unpolluted. |
| **Offsets** | Manual commit, only after success-or-DLQ | Auto-commit (default) | Auto-commit can acknowledge a message mid-retry — a crash then loses the order silently. |
| **Fault injection** | Deterministic, driven by product name | Random failure probability | A repeatable demo: you can point at an order and say what will happen to it. |
| **DLQ replay** | Republish original bytes, `x-replay-count` capped | Decode-then-re-encode; retry forever instead of a DLQ; manual re-entry | Re-encoding breaks for messages whose *decoding* failed. An uncapped replay lets a poison message ping-pong `DLQ → orders → DLQ` forever, so the counter is what makes replay safe to automate. |

---

## Prerequisites

- **Docker Desktop** (running) — provides Kafka + Schema Registry.
- **Python 3.9+** (tested on 3.13).

---

## Setup

### 1. Start the infrastructure

```bash
docker compose up -d
```

| Service | Port | Purpose |
|---|---|---|
| `kafka` | `9092` | Kafka broker (KRaft — no Zookeeper) |
| `schema-registry` | `8081` | Avro schema store (REST) |
| `kafka-init` | — | one-shot: creates `orders` + `orders.DLQ`, then exits |

Verify (`kafka-init` showing `Exited (0)` is expected — it finished its job):

```bash
docker compose ps
```

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### 2. Python environment

```bash
python -m venv .venv
```

```bash
.venv\Scripts\activate
```

```bash
pip install -r requirements.txt
```

---

## Running

Open **two terminals** (both with the venv activated).

**Terminal 1 — consumer** (start it first so it sees everything):

```bash
python consumer.py --from-beginning
```

**Terminal 2 — producer:**

```bash
python producer.py --count 20 --interval 0.5
```

**Afterwards — inspect the DLQ:**

```bash
python dlq_inspector.py
```

### Useful flags

| Script | Flag | Meaning |
|---|---|---|
| `producer.py` | `--count N` | how many orders to send |
| | `--interval S` | delay between orders |
| | `--poison-rate 0.0` | send only healthy orders |
| `consumer.py` | `--max-retries N` | retries after the first attempt (default 3) |
| | `--backoff S` | base backoff, doubled each retry (default 0.5) |
| | `--from-beginning` | read the topic from offset 0 |
| | `--idle-timeout S` | stop after S seconds of silence and print the final report |
| | `--group NAME` | consumer group id (a new name re-reads the whole topic) |
| `dlq_inspector.py` | `--follow` | keep watching instead of exiting when idle |
| `dlq_replay.py` | `--dry-run` | report what would be replayed; publish nothing |
| | `--only transient\|permanent\|all` | which failure class to replay (default `all`) |
| | `--max-replays N` | refuse to replay a message more than N times (default 1) |

---

## Tests

```bash
pip install -r requirements-dev.txt
```

```bash
python -m pytest tests/ -q
```

**39 tests, ~0.5 s, and they need no running Kafka** — they cover the schema
contract, the running-average maths, the retry/DLQ decision logic and the DLQ
header format in isolation. A grader can run them without starting Docker.

| File | Covers |
|---|---|
| `test_schema_and_producer.py` | `order.avsc` matches the spec; every generated order encodes; poison products are reachable |
| `test_aggregation.py` | Welford mean agrees with a batch mean over 5 000 values; memory stays constant |
| `test_retry_and_dlq.py` | permanent failures skip retries; transient ones use exactly `max_retries + 1`; backoff doubles; DLQ keeps original bytes + diagnostics |
| `test_dlq_replay.py` | failure classification and header decoding for the replay tool |

---

## Live demo runbook

The producer emits three "poison" products that drive the failure paths
deterministically:

| Product | Consumer behaviour | Ends up |
|---|---|---|
| `Item1` … `Item5` | succeeds on attempt 1 | running average |
| `FLAKY_ITEM` | fails twice, **succeeds on attempt 3** | running average (proves retry works) |
| `ALWAYS_FAIL` | transient error, all 4 attempts fail | `orders.DLQ` |
| `BAD_ITEM` (price `-1.0`) | **permanent** error, no retry at all | `orders.DLQ` |

**Suggested demo sequence**

1. `docker compose ps` — show Kafka + Schema Registry are up.
2. Start `consumer.py --from-beginning`.
3. Run `producer.py --count 5 --poison-rate 0` — clean run; point at the
   `RUNNING AVG` climbing after every order.
4. Run `producer.py --count 15 --poison-rate 0.4 --start-id 2001` — now show:
   - a `FLAKY_ITEM` printing `[RETRY 1/3]`, `[RETRY 2/3]`, then `[OK after 3 attempts]`
     → **retry recovery**
   - an `ALWAYS_FAIL` exhausting retries → `[-> DLQ]`
   - a `BAD_ITEM` going `[-> DLQ]` immediately with **no** retries
     → shows permanent vs transient classification
5. `curl http://localhost:8081/subjects` — the schema auto-registered as
   `orders-value`.
6. Stop the consumer with `Ctrl+C` — the **FINAL REPORT** prints the totals and
   the final running average.
7. Run `dlq_inspector.py` — every failed message with its error type, attempt
   count and origin offset.
8. **Recovery story.** `python dlq_replay.py --dry-run` shows what would be
   replayed without touching anything; then
   `python dlq_replay.py --only transient` pushes the retryable failures back
   into `orders` while leaving the genuinely-bad data behind. Restart the
   consumer to watch them be reprocessed.
9. **Loop safety.** Run `dlq_replay.py --only transient` a second time — the
   messages that failed again are refused with
   `already replayed 1x (limit 1) - needs manual attention`, so a poison
   message can never ping-pong between the two topics forever.

---

## Verified run (24 orders, `--poison-rate 0.5`)

```
[RECV] orderId=1015   product=FLAKY_ITEM   price=  255.71
        [RETRY 1/3] temporary glitch (attempt 1) - backing off 0.5s
        [RETRY 2/3] temporary glitch (attempt 2) - backing off 1.0s
        [OK after 3 attempt(s)]  processed=4  RUNNING AVG = 288.23     <- retry recovered
[RECV] orderId=1013   product=BAD_ITEM     price=   -1.00
        [-> DLQ] PermanentError after 1 attempt(s): invalid price      <- no retry
[RECV] orderId=1019   product=ALWAYS_FAIL  price=  434.43
        [RETRY 1/3] ... [RETRY 2/3] ... [RETRY 3/3] ...
        [-> DLQ] TransientError(retries exhausted) after 4 attempt(s)  <- retries used up
------------------------------------------------------------------------------
FINAL REPORT
  processed successfully : 14
  -> DLQ (permanent)     : 4
  -> DLQ (retries gone)  : 6
  -> DLQ (undecodable)   : 0
  RUNNING AVERAGE PRICE  : 273.37  (over 14 orders)
```

14 + 4 + 6 = 24 — every produced order is accounted for, none silently dropped.

> Note: orders are consumed out of numeric sequence (1001, 1003, 1005, …)
> because `orders` has **3 partitions** and the consumer drains them one at a
> time. Kafka guarantees ordering *within* a partition, not across the topic.

---

## Design notes

**Retry vs. DLQ decision.** Failures are modelled as two exception types.
`PermanentError` (invalid data) short-circuits immediately — retrying corrupt
data only delays the partition. `TransientError` is retried up to
`--max-retries` times with backoff `0.5s → 1s → 2s`; if it still fails, the
message is treated as permanently failed and forwarded to the DLQ.

**Why blocking retries are safe here.** Total backoff is 3.5 s, far below Kafka's
`max.poll.interval.ms` (5 minutes), so the consumer is never evicted from its
group. If backoffs had to grow to minutes, the correct design would switch to the
non-blocking **retry-topic pattern**.

**Ordering of side effects.** For every message the consumer does:
process (or DLQ-publish + `flush()`) **first**, and commits the offset **last**.
This yields at-least-once semantics with no silent loss — a crash mid-way simply
replays the message.

**Avro `float` precision.** The schema specifies `float` (32-bit) as required by
the assignment, so `123.45` decodes as `123.44999694824219`. All output is
formatted to two decimals; use `double` if exact decimal prices ever matter.

---

## Project layout

```
kafka-order-avro/
├─ docker-compose.yml   # Kafka (KRaft) + Schema Registry + topic init
├─ schemas/
│  └─ order.avsc        # Avro schema: { orderId, product, price }
├─ common.py            # shared config, topic names, schema loader
├─ producer.py          # Avro producer (randomized orders + fault injection)
├─ consumer.py          # Avro consumer: running average + retry + DLQ
├─ dlq_inspector.py     # read-only DLQ viewer for the demo
├─ dlq_replay.py        # replay DLQ messages back into `orders` (loop-safe)
├─ tests/               # pytest suite - no Kafka required
│  ├─ conftest.py
│  ├─ test_schema_and_producer.py
│  ├─ test_aggregation.py
│  ├─ test_retry_and_dlq.py
│  └─ test_dlq_replay.py
├─ requirements.txt
├─ requirements-dev.txt
└─ README.md
```

---

## Teardown

```bash
docker compose down
```

```bash
docker compose down -v
```

(`-v` also wipes all Kafka data, giving a clean slate for the next demo run.)
