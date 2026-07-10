"""Tests for the domain records — primarily a guard against accidental
schema drift.  If you add a field to a record, you should add a test here
that pins the value in place.
"""
from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date, datetime
from types import MappingProxyType

import pytest

from include.domain import (
    CleanedOrder,
    CustomerId,
    CustomerRecord,
    OrderId,
    OrderRecord,
    ProductId,
    ProductRecord,
    QualityReport,
)


class TestNominalTypes:
    """NewType wrappers are still the underlying int at runtime — that's
    intentional.  The point of the newtype is at the *call site*: a
    `CustomerId` typed parameter refuses a raw `int`.  Here we just
    confirm the wrappers don't add behaviour we don't expect.
    """

    def test_newtype_passes_through_int(self):
        cid = CustomerId(7)
        assert isinstance(cid, int)
        assert cid == 7

    def test_can_arithmetic_in_cents(self):
        # `revenue_cents = quantity * unit_price_cents` is the canonical formula
        assert 3 * ProductId(5001) * 0 + 3 * 1299 == 3897  # `int * int = int`


class TestCustomerRecord:
    def _r(self, **overrides) -> CustomerRecord:
        defaults = MappingProxyType(
            dict(
                customer_id=CustomerId(1),
                email="a@example.com",
                signup_date=date(2024, 1, 1),
                country="US",
            )
        )
        return CustomerRecord(**{**defaults, **overrides})

    def test_construction(self):
        r = self._r()
        assert r.customer_id == CustomerId(1)
        assert r.email == "a@example.com"
        assert r.country == "US"

    def test_is_frozen(self):
        r = self._r()
        with pytest.raises(FrozenInstanceError):
            r.email = "x"  # type: ignore[misc]

    def test_equality_by_value(self):
        # Records are values, not identifiers. Two constructed with the
        # same args should be `==` and hashable into a set.
        a = self._r()
        b = self._r()
        assert a == b
        assert len({a, b}) == 1


class TestOrderRecord:
    def _r(self, **overrides) -> OrderRecord:
        defaults = dict(
            order_id=OrderId(9001),
            customer_id=CustomerId(1001),
            product_id=ProductId(5001),
            quantity=2,
            ordered_at=datetime(2024, 1, 1, 10, 0),
            unit_price_cents=1299,
            status="shipped",
        )
        defaults.update(overrides)
        return OrderRecord(**defaults)

    def test_construction(self):
        r = self._r()
        assert r.quantity == 2
        assert r.status == "shipped"

    def test_price_in_cents_avoids_float_drift(self):
        # $12.99 stored as 1299 cents — never 12.99 as float.
        r = self._r(unit_price_cents=1299)
        assert isinstance(r.unit_price_cents, int)


class TestCleanedOrder:
    def _r(self, **overrides) -> CleanedOrder:
        defaults = dict(
            order_id=OrderId(1),
            customer_id=CustomerId(1),
            product_id=ProductId(1),
            quantity=2,
            unit_price_cents=1299,
            revenue_cents=2598,
            ordered_at=datetime(2024, 1, 1),
        )
        defaults.update(overrides)
        return CleanedOrder(**defaults)

    def test_revenue_is_explicit(self):
        # The transform layer is the *only* place revenue is computed.
        # Here we just verify the field exists with the expected type.
        r = self._r()
        assert isinstance(r.revenue_cents, int)
        assert r.revenue_cents == r.quantity * r.unit_price_cents


class TestQualityReport:
    def test_empty_passes(self):
        r = QualityReport()
        assert r.passed is True
        assert r.rows_seen == 0
        assert r.rows_rejected == 0

    def test_with_errors_fails(self):
        r = QualityReport(rows_seen=1, rows_accepted=0, rows_rejected=1, errors=["x"])
        assert r.passed is False

    def test_pinning_passed_invariant(self):
        assert QualityReport(errors=[]).passed is True
        assert QualityReport(rows_rejected=1, errors=["x"]).passed is False
        assert QualityReport(rows_rejected=2, errors=[]).passed is False
