"""
Tests for the retry policy (assignment requirement #2) and the Dead Letter
Queue (requirement #3).
"""

import time

import pytest

import common
import consumer


def order(product, price=100.0, oid="1"):
    return {"orderId": oid, "product": product, "price": price}


# =============================================================================
# Error classification: permanent failures must NOT consume retries
# =============================================================================
def test_healthy_order_succeeds_on_the_first_attempt():
    ok, err, attempts = consumer.process_with_retry(order("Item1"), 3, 0.001)
    assert ok is True
    assert err is None
    assert attempts == 1


def test_bad_item_is_permanent_and_is_never_retried():
    ok, err, attempts = consumer.process_with_retry(
        order(common.PRODUCT_BAD, price=-1.0), 3, 0.001
    )
    assert ok is False
    assert isinstance(err, consumer.PermanentError)
    assert attempts == 1, "retrying invalid data only delays the partition"


@pytest.mark.parametrize("bad_price", [-5.0, 0.0])
def test_invalid_price_is_permanent_even_for_a_normal_product(bad_price):
    ok, err, attempts = consumer.process_with_retry(
        order("Item2", price=bad_price), 3, 0.001
    )
    assert ok is False
    assert isinstance(err, consumer.PermanentError)
    assert attempts == 1


# =============================================================================
# Retry exhaustion
# =============================================================================
@pytest.mark.parametrize("max_retries", [0, 1, 3, 5])
def test_unrecoverable_transient_error_uses_exactly_max_retries_plus_one(max_retries):
    ok, err, attempts = consumer.process_with_retry(
        order(common.PRODUCT_ALWAYS_FAIL), max_retries, 0.001
    )
    assert ok is False
    assert isinstance(err, consumer.TransientError)
    assert attempts == max_retries + 1


def test_backoff_grows_exponentially():
    """0.05s -> 0.10s -> 0.20s: each wait must double."""
    base = 0.05
    started = time.monotonic()
    ok, _, attempts = consumer.process_with_retry(
        order(common.PRODUCT_ALWAYS_FAIL), 3, base
    )
    elapsed = time.monotonic() - started

    assert ok is False and attempts == 4
    expected = base * (1 + 2 + 4)          # 0.35s of total backoff
    assert elapsed >= expected * 0.9, "backoff was skipped or too short"
    assert elapsed < expected * 4, "backoff grew faster than exponentially"


# =============================================================================
# Retry recovery - the point of having retries at all
# =============================================================================
def test_flaky_item_recovers_once_retries_are_allowed():
    ok, err, attempts = consumer.process_with_retry(
        order(common.PRODUCT_FLAKY), 3, 0.001
    )
    assert ok is True
    assert err is None
    assert attempts == consumer.FLAKY_RECOVERS_AFTER + 1


def test_flaky_item_falls_through_to_dlq_when_retries_are_too_few():
    ok, err, attempts = consumer.process_with_retry(
        order(common.PRODUCT_FLAKY), 1, 0.001
    )
    assert ok is False
    assert isinstance(err, consumer.TransientError)
    assert attempts == 2


# =============================================================================
# DLQ payload and headers
# =============================================================================
class FakeMessage:
    """Minimal stand-in for a confluent_kafka Message."""

    def __init__(self, topic="orders", partition=1, offset=42,
                 key=b"1001", value=b"\x00\x00\x00\x00\x01payload", headers=None):
        self._topic, self._partition, self._offset = topic, partition, offset
        self._key, self._value, self._headers = key, value, headers

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value

    def headers(self):
        return self._headers


class FakeProducer:
    def __init__(self):
        self.produced = []

    def produce(self, topic, key=None, value=None, headers=None):
        self.produced.append(
            {"topic": topic, "key": key, "value": value, "headers": headers}
        )

    def flush(self, timeout=None):
        return 0


def _headers_of(record):
    return {k: v.decode("utf-8") for k, v in record["headers"]}


def test_dlq_record_keeps_the_original_bytes_untouched():
    fake = FakeProducer()
    payload = b"\x00\x00\x00\x00\x01raw-avro-bytes"
    msg = FakeMessage(value=payload)

    consumer.send_to_dlq(
        fake, msg, consumer.PermanentError("bad price"), 1, "PermanentError"
    )

    assert len(fake.produced) == 1
    record = fake.produced[0]
    assert record["topic"] == common.TOPIC_DLQ
    assert record["value"] == payload, "the DLQ must stay byte-for-byte replayable"
    assert record["key"] == b"1001"


def test_dlq_record_carries_the_diagnostics_needed_to_triage():
    fake = FakeProducer()
    consumer.send_to_dlq(
        fake, FakeMessage(), consumer.TransientError("service down"), 4,
        "TransientError(retries exhausted)",
    )

    hdr = _headers_of(fake.produced[0])
    assert hdr["x-error-type"] == "TransientError(retries exhausted)"
    assert hdr["x-error-message"] == "service down"
    assert hdr["x-attempts"] == "4"
    assert hdr["x-original-topic"] == "orders"
    assert hdr["x-original-partition"] == "1"
    assert hdr["x-original-offset"] == "42"
    assert "x-failed-at" in hdr


def test_dlq_preserves_replay_headers_to_prevent_infinite_loops():
    """
    A message replayed by dlq_replay.py that fails again must keep its
    x-replay-count, otherwise the replay limit could never be enforced.
    """
    fake = FakeProducer()
    msg = FakeMessage(headers=[("x-replay-count", b"1"),
                               ("x-replayed-at", b"2026-01-01T00:00:00+00:00")])

    consumer.send_to_dlq(
        fake, msg, consumer.TransientError("still down"), 4,
        "TransientError(retries exhausted)",
    )

    hdr = _headers_of(fake.produced[0])
    assert hdr["x-replay-count"] == "1"
    assert hdr["x-replayed-at"] == "2026-01-01T00:00:00+00:00"


def test_error_message_is_truncated_so_a_huge_exception_cannot_break_the_dlq():
    fake = FakeProducer()
    consumer.send_to_dlq(
        fake, FakeMessage(), consumer.PermanentError("x" * 5000), 1, "PermanentError"
    )
    assert len(_headers_of(fake.produced[0])["x-error-message"]) == 500
