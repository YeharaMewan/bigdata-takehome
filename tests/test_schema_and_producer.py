"""Tests for the Avro schema contract and the producer's generated orders."""

import io
import json

import fastavro
import pytest

import common
import producer


@pytest.fixture(scope="module")
def schema():
    return fastavro.parse_schema(json.loads(common.load_schema_str()))


# --- The schema contract -----------------------------------------------------
def test_schema_matches_the_assignment_specification():
    """
    Guards order.avsc against accidental edits: the assignment fixes these three
    fields and their types exactly.
    """
    raw = json.loads(common.load_schema_str())
    assert raw["type"] == "record"
    fields = {f["name"]: f["type"] for f in raw["fields"]}
    assert fields == {"orderId": "string", "product": "string", "price": "float"}


def test_round_trip_preserves_values(schema):
    original = {"orderId": "1001", "product": "Item1", "price": 123.45}

    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, original)
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, schema)

    assert decoded["orderId"] == "1001"
    assert decoded["product"] == "Item1"
    # Avro `float` is 32-bit, so exact equality does not hold - see README.
    assert decoded["price"] == pytest.approx(123.45, abs=0.01)


def test_wrong_types_are_rejected(schema):
    """Avro must refuse an int orderId - this is the validation we get for free."""
    buf = io.BytesIO()
    with pytest.raises(Exception):
        fastavro.schemaless_writer(
            buf, schema, {"orderId": 1001, "product": "Item1", "price": 1.0}
        )


# --- The producer ------------------------------------------------------------
def test_every_generated_order_encodes(schema):
    for i in range(500):
        order = producer.build_order(1001 + i, poison_rate=0.3)
        fastavro.schemaless_writer(io.BytesIO(), schema, order)  # raises on mismatch


def test_order_id_is_a_string():
    order = producer.build_order(1001, poison_rate=0.0)
    assert isinstance(order["orderId"], str)
    assert order["orderId"] == "1001"


def test_poison_rate_zero_produces_only_healthy_orders():
    for i in range(200):
        order = producer.build_order(1000 + i, poison_rate=0.0)
        assert order["product"] in common.NORMAL_PRODUCTS
        assert order["price"] > 0


def test_poison_rate_one_produces_only_poison_orders():
    for i in range(200):
        order = producer.build_order(1000 + i, poison_rate=1.0)
        assert order["product"] in common.POISON_PRODUCTS


def test_bad_item_always_carries_an_invalid_price():
    """BAD_ITEM must reliably trigger the permanent-failure path in the demo."""
    seen = 0
    for i in range(500):
        order = producer.build_order(1000 + i, poison_rate=1.0)
        if order["product"] == common.PRODUCT_BAD:
            assert order["price"] <= 0
            seen += 1
    assert seen > 0, "BAD_ITEM was never generated - the test proved nothing"


def test_all_three_failure_modes_are_reachable():
    """The live demo depends on every poison product actually appearing."""
    products = {producer.build_order(1000 + i, poison_rate=1.0)["product"]
                for i in range(500)}
    assert products == set(common.POISON_PRODUCTS)
