"""Tests for the real-time running average (assignment requirement #1)."""

import random
import statistics

import pytest

import consumer


def test_empty_average_is_zero():
    avg = consumer.RunningAverage()
    assert avg.count == 0
    assert avg.mean == 0.0


def test_single_value_is_its_own_mean():
    avg = consumer.RunningAverage()
    assert avg.add(7.5) == pytest.approx(7.5)
    assert avg.count == 1


def test_known_sequence():
    avg = consumer.RunningAverage()
    for value in (10.0, 20.0, 30.0, 40.0):
        avg.add(value)
    assert avg.count == 4
    assert avg.mean == pytest.approx(25.0)


def test_add_returns_the_current_mean():
    avg = consumer.RunningAverage()
    avg.add(10.0)
    assert avg.add(20.0) == pytest.approx(15.0)


def test_mean_updates_after_every_message():
    """The average must be available continuously, not only at the end."""
    avg = consumer.RunningAverage()
    seen = [avg.add(v) for v in (100.0, 200.0, 300.0)]
    assert seen == [pytest.approx(100.0), pytest.approx(150.0), pytest.approx(200.0)]


def test_welford_matches_batch_mean_over_many_values():
    """
    The incremental (Welford) update must agree with a batch computation.
    This is what justifies not keeping every price in memory.
    """
    random.seed(42)
    values = [random.uniform(1.0, 1000.0) for _ in range(5000)]

    avg = consumer.RunningAverage()
    for value in values:
        avg.add(value)

    assert avg.count == 5000
    assert avg.mean == pytest.approx(statistics.mean(values), rel=1e-9)


def test_memory_is_constant():
    """RunningAverage must not accumulate per-message state (unbounded stream)."""
    avg = consumer.RunningAverage()
    for value in range(1000):
        avg.add(float(value))
    # Only the three scalars - no list of prices hiding anywhere.
    assert set(vars(avg)) == {"count", "total", "mean"}
