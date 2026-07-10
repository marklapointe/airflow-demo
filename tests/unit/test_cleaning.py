"""Tests for the cleaning pipeline.  These tests are the *behavioural
spec* for the warehouse; if you change `clean_orders` semantics, you
should expect to touch these tests.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from include.domain import (
    CustomerId,
    OrderId,
    OrderRecord,
    ProductId,
    QualityReport,
)
from include.io import CsvSource, SchemaMismatch, SourceUnavailable
from include.transforms import (
    _REQUIRED_STATUSES,
    clean_orders,
    drop_unknown_status,
    price_is_sane,
    quality_report,
    require_positive_qty,
    to_cleaned,
)

# --- a small in-memory fixture ----------------------------------------

def make_order(**overrides) -> OrderRecord:
    base = dict(
        order_id=OrderId(9001),
        customer_id=CustomerId(1001),
        product_id=ProductId(5001),
        quantity=2,
        ordered_at=datetime(2024, 1, 1, 10, 0),
        unit_price_cents=1299,
        status="shipped",
    )
    base.update(overrides)
    return OrderRecord(**base)


KEEP = make_order(order_id=OrderId(1))  # shipped, qty=2, price=1299 → ok
KEEP.unit_price_cents  # noqa: E501 — keep linters quiet (1299)


# --- unit tests on each stage -----------------------------------------

class TestDropUnknownStatus:
    def test_keeps_shipped(self):
        assert list(drop_unknown_status([make_order(status="shipped")]))[0] == make_order(status="shipped")

    def test_keeps_pending(self):
        assert list(drop_unknown_status([make_order(status="pending")]))[0] == make_order(status="pending")

    def test_drops_cancelled(self):
        assert list(drop_unknown_status([make_order(status="cancelled")])) == []

    def test_required_statuses_is_a_frozenset(self):
        # Backing type chosen for O(1) membership and immutability.
        assert isinstance(_REQUIRED_STATUSES, frozenset)
        assert "shipped" in _REQUIRED_STATUSES
        assert "pending" in _REQUIRED_STATUSES
        assert "cancelled" not in _REQUIRED_STATUSES


class TestPositiveGuards:
    def test_qty_zero_rejected(self):
        assert list(require_positive_qty([make_order(quantity=0)])) == []

    def test_qty_negative_rejected(self):
        assert list(require_positive_qty([make_order(quantity=-3)])) == []

    def test_qty_positive_kept(self):
        assert list(require_positive_qty([make_order(quantity=1)]))[0].quantity == 1

    def test_price_zero_rejected(self):
        assert list(price_is_sane([make_order(unit_price_cents=0)])) == []

    def test_price_negative_rejected(self):
        assert list(price_is_sane([make_order(unit_price_cents=-100)])) == []


class TestToCleaned:
    def test_revenue_is_quantity_times_price(self):
        [cleaned] = to_cleaned([make_order(quantity=3, unit_price_cents=1299)])
        assert cleaned.revenue_cents == 3 * 1299

    def test_drops_status_field(self):
        # `CleanedOrder` is a *narrower* projection — status is not there.
        [cleaned] = to_cleaned([KEEP])
        assert not hasattr(cleaned, "status")


class TestCleanOrders:
    def test_canonical_pipeline_filters_in_order(self):
        orders = [
            make_order(order_id=OrderId(1), status="cancelled"),    # dropped by stage 1
            make_order(order_id=OrderId(2), quantity=0),            # dropped by stage 2
            make_order(order_id=OrderId(3), unit_price_cents=-1),   # dropped by stage 3
            make_order(order_id=OrderId(4)),                        # kept
        ]
        cleaned = list(clean_orders(orders))
        assert [c.order_id for c in cleaned] == [OrderId(4)]

    def test_empty_input_yields_empty_output(self):
        assert list(clean_orders([])) == []


class TestQualityReport:
    def test_counts_each_row_once(self):
        # A row with *multiple* problems is rejected once but lists
        # every violation as a separate string.  This invariant matches
        # the cleaning pipeline order.
        bad = make_order(order_id=OrderId(99), quantity=0, unit_price_cents=-1, status="cancelled")
        _, report = quality_report([bad])
        assert report.rows_seen == 1
        assert report.rows_accepted == 0
        assert report.rows_rejected == 1
        assert len(report.errors) == 3
        assert report.passed is False

    def test_mixed_population(self):
        good = make_order(order_id=OrderId(1))
        bad1 = make_order(order_id=OrderId(2), status="cancelled")
        bad2 = make_order(order_id=OrderId(3), quantity=0)
        kept, report = quality_report([good, bad1, bad2])
        assert len(kept) == 1
        assert report.rows_seen == 3
        assert report.rows_accepted == 1
        assert report.rows_rejected == 2


# --- an integration test against the sample CSV -----------------------

class TestAgainstSampleCsv:
    """The numbers in include/data/orders.csv are pinned by this test.
    That gives you a single place to look when sample data changes
    deliberately.
    """

    @pytest.fixture
    def orders(self) -> list[OrderRecord]:
        path = Path(__file__).parents[2] / "include" / "data" / "orders.csv"
        return list(CsvSource(path).read_orders())

    def test_csv_reads_ten_rows(self, orders):
        assert len(orders) == 10

    def test_canonical_pipeline_keeps_seven(self, orders):
        cleaned = list(clean_orders(orders))
        assert len(cleaned) == 7

    def test_total_revenue(self, orders):
        cleaned = list(clean_orders(orders))
        revenue = sum(c.revenue_cents for c in cleaned)
        assert revenue == 24282

    def test_quality_report_matches_pipeline(self, orders):
        cleaned = list(clean_orders(orders))
        _, report = quality_report(orders)
        assert len(cleaned) == report.rows_accepted
        assert (10 - len(cleaned)) == report.rows_rejected


# --- source error handling --------------------------------------------

class TestCsvSourceErrors:
    def test_missing_file_raises(self, tmp_path: Path):
        with pytest.raises(SourceUnavailable):
            list(CsvSource(tmp_path / "nope.csv").read_orders())

    def test_bad_header_raises(self, tmp_path: Path):
        bad = tmp_path / "bad.csv"
        bad.write_text("wrong,columns\n1,2\n")
        with pytest.raises(SchemaMismatch, match="missing required columns"):
            list(CsvSource(bad).read_orders())
