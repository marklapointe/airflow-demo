"""Sources — read-only adapters that hand back validated records.

A source never mutates state. It either returns records or raises
`SourceUnavailable` with enough context to log and retry. The DAG layer
is responsible for retries; this layer raises and gets out of the way.

Why csv + sqlite only?  Two reasons:
  1. The learning project should run with zero infrastructure.
  2. Every additional format would add setup friction and obscure the
     pattern that *any* source looks the same from the DAG's perspective.
"""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Iterable, Iterator

from include.domain import (
    CustomerId,
    CustomerRecord,
    OrderId,
    OrderRecord,
    ProductId,
    ProductRecord,
)


# --- exceptions --------------------------------------------------

class SourceUnavailable(RuntimeError):
    """Raised when a source cannot be read right now.  Recovery is up
    to the caller; this layer never silently returns partial data.
    """


class SchemaMismatch(ValueError):
    """Raised when the source headers don't match what we expect.
    Failing fast here is cheaper than debugging a downstream null later.
    """


# --- CSV source --------------------------------------------------

class CsvSource:
    """Typed CSV reader. Usage:

        src = CsvSource(Path("data/customers.csv"))
        for row in src.read_customers():
            ...

    The reader is an iterator — streaming — which keeps memory bounded
    no matter how large the source gets.  That matters: a DAG can run on
    a single worker and a 50 GB CSV will not fit.  Streaming + partitioning
    is the standard answer (see Knuth, TAOCP §2.6 on linked allocation and
    "tape" algorithms — same idea, different storage).
    """

    REQUIRED_CUSTOMER_COLS = ("customer_id", "email", "signup_date", "country")
    REQUIRED_PRODUCT_COLS = ("product_id", "sku", "name", "category", "unit_price_cents")
    REQUIRED_ORDER_COLS = (
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "ordered_at",
        "unit_price_cents",
        "status",
    )

    def __init__(self, path: Path) -> None:
        self._path = path

    # -- private helpers --------------------------------------------------

    def _open(self) -> tuple[Iterable[dict], list[str]]:
        if not self._path.exists():
            raise SourceUnavailable(f"CSV not found at {self._path}")
        try:
            fh = self._path.open(newline="", encoding="utf-8")
        except OSError as exc:
            raise SourceUnavailable(f"Cannot open {self._path}: {exc}") from exc
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            fh.close()
            raise SchemaMismatch(f"{self._path} is missing a header row")
        return reader, list(reader.fieldnames)

    @staticmethod
    def _check(expected: tuple[str, ...], actual: list[str], path: Path) -> None:
        missing = [c for c in expected if c not in actual]
        if missing:
            raise SchemaMismatch(
                f"{path} is missing required columns: {missing}; "
                f"got {actual}"
            )

    # -- public readers ---------------------------------------------------

    def read_customers(self) -> Iterator[CustomerRecord]:
        rows, headers = self._open()
        self._check(self.REQUIRED_CUSTOMER_COLS, headers, self._path)
        for raw in rows:
            yield CustomerRecord(
                customer_id=CustomerId(int(raw["customer_id"])),
                email=raw["email"].strip().lower(),
                signup_date=_parse_date(raw["signup_date"]),
                country=raw["country"].strip(),
            )

    def read_products(self) -> Iterator[ProductRecord]:
        rows, headers = self._open()
        self._check(self.REQUIRED_PRODUCT_COLS, headers, self._path)
        for raw in rows:
            yield ProductRecord(
                product_id=ProductId(int(raw["product_id"])),
                sku=raw["sku"].strip(),
                name=raw["name"].strip(),
                category=raw["category"].strip(),
                unit_price_cents=int(raw["unit_price_cents"]),
            )

    def read_orders(self) -> Iterator[OrderRecord]:
        rows, headers = self._open()
        self._check(self.REQUIRED_ORDER_COLS, headers, self._path)
        for raw in rows:
            yield OrderRecord(
                order_id=OrderId(int(raw["order_id"])),
                customer_id=CustomerId(int(raw["customer_id"])),
                product_id=ProductId(int(raw["product_id"])),
                quantity=int(raw["quantity"]),
                ordered_at=_parse_dt(raw["ordered_at"]),
                unit_price_cents=int(raw["unit_price_cents"]),
                status=raw["status"].strip().lower(),
            )


# --- SQLite source / sink (used by DAGs that need a warehouse) ----

class SqliteSource:
    """Read records from a SQLite table.  Returns generators so the
    DAG layer can stream large tables.
    """

    def __init__(self, path: Path) -> None:
        self._path = path

    def fetch(self, query: str, params: tuple = ()) -> Iterator[dict]:
        if not self._path.exists():
            raise SourceUnavailable(f"DB not found at {self._path}")
        with sqlite3.connect(self._path) as conn:
            conn.row_factory = sqlite3.Row
            yield from conn.execute(query, params)


class SqliteSink:
    """Append-only sink. The DAG layer is responsible for deciding when
    to commit; here we do "one transaction per write call" so that
    either the whole write succeeds or none of it does.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def ensure_table(self, ddl: str) -> None:
        with sqlite3.connect(self._path) as conn:
            conn.executescript(ddl)
            conn.commit()

    def write(
        self,
        table: str,
        rows: Iterable[dict],
        conflict_target: str | None = None,
    ) -> int:
        """Insert `rows` into `table`. If `conflict_target` is supplied
        the insert becomes `INSERT OR IGNORE INTO ... ON CONFLICT(target)
        DO NOTHING` — making the write idempotent on the natural key.
        Returns the number of rows newly inserted.

        Why idempotent? See design_decisions.md §4.  Without it, a retry
        after a transient SQLite hiccup would duplicate rows and corrupt
        downstream aggregates.
        """
        rows = list(rows)
        if not rows:
            return 0
        first = rows[0]
        columns = list(first.keys())
        placeholders = ", ".join("?" for _ in columns)
        col_list = ", ".join(columns)
        if conflict_target:
            sql = (
                f"INSERT OR IGNORE INTO {table} ({col_list}) "
                f"VALUES ({placeholders})"
            )
        else:
            sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        with sqlite3.connect(self._path) as conn:
            try:
                cur = conn.executemany(sql, [tuple(r[c] for c in columns) for r in rows])
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise SchemaMismatch(str(exc)) from exc
        return cur.rowcount


# --- helpers --------------------------------------------------------

from datetime import date, datetime


def _parse_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError as exc:
        raise SchemaMismatch(f"Bad date {s!r}: {exc}") from exc


def _parse_dt(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s)
    except ValueError as exc:
        raise SchemaMismatch(f"Bad datetime {s!r}: {exc}") from exc
