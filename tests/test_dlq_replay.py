"""Tests for the DLQ replay tool's classification and header handling."""

import pytest

import dlq_replay
from test_retry_and_dlq import FakeMessage


@pytest.mark.parametrize("error_type,expected", [
    ("TransientError(retries exhausted)", "transient"),
    ("PermanentError", "permanent"),
    ("DeserializationError", "permanent"),
    ("unknown", "permanent"),
])
def test_failures_are_classified_for_the_only_filter(error_type, expected):
    """
    'transient' failures are the ones worth replaying after a service recovers;
    everything else needs a data or business-rule change first, so it defaults
    to 'permanent' - the conservative choice.
    """
    assert dlq_replay.classify(error_type) == expected


def test_headers_decode_to_plain_strings():
    msg = FakeMessage(headers=[("x-error-type", b"PermanentError"),
                               ("x-attempts", b"1")])
    assert dlq_replay.decode_headers(msg) == {
        "x-error-type": "PermanentError",
        "x-attempts": "1",
    }


def test_missing_headers_decode_to_an_empty_mapping():
    assert dlq_replay.decode_headers(FakeMessage(headers=None)) == {}


def test_null_header_value_does_not_crash_the_decoder():
    msg = FakeMessage(headers=[("x-error-type", None)])
    assert dlq_replay.decode_headers(msg) == {"x-error-type": ""}


def test_replay_count_header_name_matches_what_the_consumer_preserves():
    """
    consumer.send_to_dlq() forwards headers starting with 'x-replay'. If this
    prefix ever drifted, the loop protection would silently stop working.
    """
    assert dlq_replay.HDR_REPLAY_COUNT.startswith("x-replay")
    assert dlq_replay.HDR_REPLAYED_AT.startswith("x-replay")
