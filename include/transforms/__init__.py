"""Pure transformations — read records, yield records, never I/O.

By keeping these functions total and side-effect-free they compose:
any of them can be unit-tested with `pytest` in milliseconds, and any
pipeline is just `f ∘ g ∘ h`.

The shape `Iterable[X] -> Iterable[Y]` is intentional. It means:
  * we can chain pipelines without materialising intermediates, and
  * we can measure memory as "one row" regardless of input size.
"""
from __future__ import annotations

from typing import Iterable, Iterator

from include.domain import (
    CleanedOrder,
    OrderRecord,
    QualityReport,
)


# --- the cleaning pipeline -----------------------------------------

_REQUIRED_STATUSES = frozenset({"pending", "shipped"})


def drop_unknown_status(orders: Iterable[OrderRecord]) -> Iterator[OrderRecord]:
    """Discard orders whose status is not actionable.

    `cancelled` orders are out of the warehouse's scope — they are a
    refunds-system concern.  Returning them would force the warehouse
    table to carry status semantics it doesn't need.
    """
    return (o for o in orders if o.status in _REQUIRED_STATUSES)


def require_positive_qty(orders: Iterable[OrderRecord]) -> Iterator[OrderRecord]:
    """Skip rows where quantity < 1. Sentinel for "we don't know how
    many, don't bill at all".
    """
    return (o for o in orders if o.quantity >= 1)


def price_is_sane(orders: Iterable[OrderRecord]) -> Iterator[OrderRecord]:
    """Skip non-positive prices.  Without this guard, a negative
    price on an order would *subtract* revenue from the aggregate.
    """
    return (o for o in orders if o.unit_price_cents > 0)


def to_cleaned(orders: Iterable[OrderRecord]) -> Iterator[CleanedOrder]:
    """Project the wide `OrderRecord` into the narrower `CleanedOrder`.

    The projection is the single place where `revenue_cents` is defined
    as `quantity * unit_price_cents`.  Anywhere else in the codebase that
    needs revenue should call into the warehouse table or recompute via
    the records — never invent a third formula.
    """
    for o in orders:
        yield CleanedOrder(
            order_id=o.order_id,
            customer_id=o.customer_id,
            product_id=o.product_id,
            quantity=o.quantity,
            unit_price_cents=o.unit_price_cents,
            revenue_cents=o.quantity * o.unit_price_cents,
            ordered_at=o.ordered_at,
        )


# --- the canonical pipeline ----------------------------------------

def clean_orders(orders: Iterable[OrderRecord]) -> Iterator[CleanedOrder]:
    """The canonical cleaning pipeline. Order matters:
      1. drop_unknown_status  — narrow the population
      2. require_positive_qty — basic shape invariant
      3. price_is_sane        — basic shape invariant
      4. to_cleaned           — project to the warehouse shape

    Tests in tests/unit/test_cleaning.py lock this composition order in.
    """
    return to_cleaned(
        price_is_sane(require_positive_qty(drop_unknown_status(orders)))
    )


# --- observability: counting what we accepted and what we skipped --

def quality_report(orders: Iterable[OrderRecord]) -> tuple[list[OrderRecord], QualityReport]:
    """Walk the orders once, partition into the kept set and the report.

    This is a classic Knuth "partition with a side counter" pattern.
    We deliberately do not do the counting in `clean_orders` so the
    cleaning pipeline itself stays a single `filter` chain.
    """
    kept: list[OrderRecord] = []
    seen = 0
    accepted = 0
    rejected = 0
    errors: list[str] = []
    for o in orders:
        seen += 1
        reasons = _violations(o)
        if reasons:
            rejected += 1
            errors.extend(reasons)
            continue
        kept.append(o)
        accepted += 1
    return kept, QualityReport(
        rows_seen=seen,
        rows_accepted=accepted,
        rows_rejected=rejected,
        errors=errors,
    )


def _violations(o: OrderRecord) -> list[str]:
    """Return a list of human-readable rejection reasons for `o`.
    Empty list means the row passes.
    """
    msgs: list[str] = []
    if o.status not in _REQUIRED_STATUSES:
        msgs.append(f"order_id={o.order_id} has unknown status {o.status!r}")
    if o.quantity < 1:
        msgs.append(f"order_id={o.order_id} has non-positive quantity {o.quantity}")
    if o.unit_price_cents <= 0:
        msgs.append(f"order_id={o.order_id} has non-positive price {o.unit_price_cents}")
    return msgs
