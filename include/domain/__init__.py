"""Domain records — the single source of truth for row shapes.

TAOCP-flavored: a *record* here is an algebraic type — a frozen struct with
a defined field set. Reading code can rely on the fact that an `OrderRecord`
cannot appear without all six fields populated. That invariant removes a
whole class of "what if `None`?" branches downstream.

These dataclasses are deliberately:
  * immutable (`frozen=True`) — records behave as values, not as stateful handles.
  * slots — predictable memory layout, faster attribute access, hashable out
    of the box, which makes them safe to use as dict keys and in sets.
  * `kw_only` — constructors read like the schema they represent; you cannot
    accidentally swap two same-typed positional arguments.

If you add a field here, also add a fixture and a test in
`tests/unit/test_records.py`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import NewType

# --- newtypes --------------------------------------------------------
# Wrap primitives in nominal types so signatures self-document and
# a `CustomerId` cannot be silently passed where a `ProductId` is expected.

CustomerId = NewType("CustomerId", int)
ProductId = NewType("ProductId", int)
OrderId = NewType("OrderId", int)


# --- records ---------------------------------------------------------

@dataclass(frozen=True, slots=True, kw_only=True)
class CustomerRecord:
    """One row of the customers source. PII fields are kept opaque on
    purpose — only `customer_id` is used as a key.
    """
    customer_id: CustomerId
    email: str
    signup_date: date
    country: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ProductRecord:
    """Reference data for a product. `unit_price_cents` is an integer
    to avoid the floating-point pennies problem (TAOCP §4.3.1 on
    arithmetic for currency).
    """
    product_id: ProductId
    sku: str
    name: str
    category: str
    unit_price_cents: int  # always >= 0; enforced by transform layer


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderRecord:
    """One row of the orders source. Amounts are in cents (int)."""
    order_id: OrderId
    customer_id: CustomerId
    product_id: ProductId
    quantity: int                # always >= 1; enforced by transform layer
    ordered_at: datetime
    unit_price_cents: int        # captured at order time, not at report time
    status: str                  # "pending" | "shipped" | "cancelled"


@dataclass(frozen=True, slots=True, kw_only=True)
class CleanedOrder:
    """The shape that lands in the warehouse table. Note that the
    `CleanedOrder` is *narrower* than `OrderRecord`: it drops `status`
    and freezes the price we will bill. The transform layer's job is
    to enforce this narrowing — see include/transforms/cleaning.py.
    """
    order_id: OrderId
    customer_id: CustomerId
    product_id: ProductId
    quantity: int
    unit_price_cents: int
    revenue_cents: int
    ordered_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class QualityReport:
    """Aggregate counts that a downstream quality check produces.
    `errors` is the list of human-readable reasons; the report is
    considered a *failure* iff `errors` is non-empty.
    """
    rows_seen: int = 0
    rows_accepted: int = 0
    rows_rejected: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Invariant: report passes iff every row was accepted."""
        return not self.errors and self.rows_rejected == 0
