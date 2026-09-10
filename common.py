"""
Shared configuration and helpers for the Kafka Order-Avro system.

DECISION    : Put connection settings, topic names and the schema loader in ONE
              module that both the producer and the consumer import.
ALTERNATIVES: Duplicate the constants in each script, or use a YAML/.env config file.
WHY OPTIMAL : A single source of truth prevents "producer writes to `orders`,
              consumer listens on `order`" style bugs. A .env/YAML layer would be
              extra machinery for ~8 constants; environment-variable overrides
              (below) already give us deployment flexibility for free.
"""

import os
import sys

# -----------------------------------------------------------------------------
# Windows console safety net.
# The default Windows code page (cp1252) raises UnicodeEncodeError on non-ASCII
# output. Forcing UTF-8 keeps the live demo from crashing on a stray character.
# -----------------------------------------------------------------------------
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:  # pragma: no cover - older Python / redirected streams
    pass


# --- Connection settings -----------------------------------------------------
# These match the PLAINTEXT_HOST listener (9092) and the Schema Registry port
# (8081) exposed by docker-compose.yml. Env vars allow overriding without edits.
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# --- Topics ------------------------------------------------------------------
TOPIC_ORDERS = "orders"
TOPIC_DLQ = "orders.DLQ"

# --- Consumer group ----------------------------------------------------------
CONSUMER_GROUP = "order-processing-group"

# --- Avro schema -------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA_PATH = os.path.join(_HERE, "schemas", "order.avsc")


def load_schema_str() -> str:
    """Read order.avsc as a string (what AvroSerializer/AvroDeserializer expect)."""
    with open(SCHEMA_PATH, "r", encoding="utf-8") as fh:
        return fh.read()


# -----------------------------------------------------------------------------
# Special product names used to demonstrate the retry / DLQ paths.
#
# DECISION    : Trigger failures from the message CONTENT rather than randomly.
# ALTERNATIVES: Random failure probability inside the consumer; killing a
#               downstream container mid-demo.
# WHY OPTIMAL : Deterministic faults make the live demo REPEATABLE - you can
#               point at a specific order and say "this one will go to the DLQ".
#               A random failure rate cannot be narrated with confidence.
# -----------------------------------------------------------------------------
PRODUCT_FLAKY = "FLAKY_ITEM"        # transient error, recovers after a few attempts
PRODUCT_ALWAYS_FAIL = "ALWAYS_FAIL"  # transient error that never recovers -> DLQ
PRODUCT_BAD = "BAD_ITEM"             # permanent error (invalid data)     -> DLQ

NORMAL_PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]
POISON_PRODUCTS = [PRODUCT_FLAKY, PRODUCT_ALWAYS_FAIL, PRODUCT_BAD]


def friendly_startup_error(exc: Exception) -> str:
    """Turn a connection failure into an actionable message for the demo."""
    return (
        f"\n[FATAL] Could not reach Kafka / Schema Registry: {exc}\n"
        f"        bootstrap.servers = {BOOTSTRAP_SERVERS}\n"
        f"        schema.registry   = {SCHEMA_REGISTRY_URL}\n"
        "        Is Docker Desktop running?  Try:  docker compose ps\n"
    )
