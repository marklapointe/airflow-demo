"""Unit tests for `include/io/` — covers SqliteSource, SqliteSink, and the
private date-parsing helpers.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from include.io import (
    CsvSource,
    SchemaMismatch,
    SourceUnavailable,
    SqliteSink,
    SqliteSource,
    _parse_date,
    _parse_dt,
)
from include.domain import CustomerRecord, ProductRecord


# --- SqliteSource ----------------------------------------------------------


class TestSqliteSource:
    def _seed(self, path: Path) -> Path:
        """Create a tiny SQLite DB with one table for fetch() tests."""
        import sqlite3
        path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(path) as conn:
            conn.executescript(
                """
                CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
                INSERT INTO t (id, name) VALUES (1, 'alice');
                INSERT INTO t (id, name) VALUES (2, 'bob');
                """
            )
        return path

    def test_fetch_returns_rows_as_dicts(self, tmp_path: Path):
        path = self._seed(tmp_path / "x.db")
        rows = list(SqliteSource(path).fetch("SELECT id, name FROM t ORDER BY id"))
        assert [r["id"] for r in rows] == [1, 2]
        assert [r["name"] for r in rows] == ["alice", "bob"]

    def test_fetch_iterates_lazily(self, tmp_path: Path):
        # `fetch` is a generator — consuming it via list() drives iteration.
        path = self._seed(tmp_path / "x.db")
        gen = SqliteSource(path).fetch("SELECT id FROM t")
        first = next(iter(gen))
        assert first["id"] == 1

    def test_fetch_passes_params(self, tmp_path: Path):
        path = self._seed(tmp_path / "x.db")
        rows = list(
            SqliteSource(path).fetch(
                "SELECT name FROM t WHERE id = ?", (2,)
            )
        )
        assert [r["name"] for r in rows] == ["bob"]  # type: ignore[index]  # noqa: E501

    def test_fetch_missing_db_raises_source_unavailable(self, tmp_path: Path):
        with pytest.raises(SourceUnavailable) as exc:
            list(SqliteSource(tmp_path / "nope.db").fetch("SELECT 1"))
        assert "DB not found" in str(exc.value)


# --- SqliteSink ------------------------------------------------------------


class TestSqliteSink:
    DDL = """
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        sku TEXT NOT NULL,
        qty INTEGER NOT NULL
    );
    """

    def _path(self, tmp_path: Path, name: str = "sink.db") -> Path:
        return tmp_path / "data" / name

    def test_ensure_table_creates_table(self, tmp_path: Path):
        path = self._path(tmp_path)
        SqliteSink(path).ensure_table(self.DDL)
        import sqlite3
        with sqlite3.connect(path) as conn:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='orders'"
            ).fetchone()
        assert row is not None

    def test_write_inserts_all_rows(self, tmp_path: Path):
        sink = SqliteSink(self._path(tmp_path))
        sink.ensure_table(self.DDL)
        n = sink.write(
            "orders",
            [
                {"order_id": 1, "sku": "A", "qty": 3},
                {"order_id": 2, "sku": "B", "qty": 5},
            ],
        )
        assert n == 2

    def test_write_empty_rows_returns_zero(self, tmp_path: Path):
        sink = SqliteSink(self._path(tmp_path))
        sink.ensure_table(self.DDL)
        assert sink.write("orders", []) == 0

    def test_write_with_conflict_target_is_idempotent(self, tmp_path: Path):
        # Idempotency: re-running the same write must not duplicate rows.
        sink = SqliteSink(self._path(tmp_path))
        sink.ensure_table(self.DDL)
        first = {"order_id": 1, "sku": "A", "qty": 1}
        assert sink.write("orders", [first], conflict_target="order_id") == 1
        # Second write with same natural key — INSERT OR IGNORE → 0 newly inserted.
        assert sink.write("orders", [first], conflict_target="order_id") == 0

    def test_write_schema_mismatch_raised(self, tmp_path: Path):
        sink = SqliteSink(self._path(tmp_path))
        sink.ensure_table(self.DDL)
        # Duplicate primary key (no `INSERT OR IGNORE`) raises IntegrityError
        # which the sink re-raises as SchemaMismatch.
        with pytest.raises(SchemaMismatch):
            sink.write(
                "orders",
                [
                    {"order_id": 1, "sku": "A", "qty": 1},
                    {"order_id": 1, "sku": "B", "qty": 1},  # duplicate
                ],
            )


# --- CsvSource happy paths not yet covered --------------------------------


class TestCsvSourceProductPaths:
    REQUIRED_PRODUCT_COLS = CsvSource.REQUIRED_PRODUCT_COLS

    def _write(self, tmp_path: Path, header: str, rows: list[str]) -> Path:
        p = tmp_path / "products.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")
        return p

    def test_read_products_yields_records(self, tmp_path: Path):
        path = self._write(
            tmp_path,
            ",".join(self.REQUIRED_PRODUCT_COLS),
            [
                "1,SKU-001,Widget,tools,499",
                "2,SKU-002,Gadget,tools,1299",
            ],
        )
        out = list(CsvSource(path).read_products())
        assert len(out) == 2
        assert isinstance(out[0], ProductRecord)
        assert out[1].name == "Gadget"

    def test_read_products_wrong_columns_raises(self, tmp_path: Path):
        path = self._write(tmp_path, "wrong,header,set", ["x,y,z"])
        with pytest.raises(SchemaMismatch):
            list(CsvSource(path).read_products())

    def test_read_customers_happy(self, tmp_path: Path):
        path = tmp_path / "customers.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "customer_id,email,signup_date,country\n"
            "1,ALICE@example.com,2024-01-02,us\n",
            encoding="utf-8",
        )
        out = list(CsvSource(path).read_customers())
        assert len(out) == 1
        assert isinstance(out[0], CustomerRecord)
        assert out[0].email == "alice@example.com"  # lowered

    def test_csv_missing_header_row(self, tmp_path: Path):
        path = tmp_path / "blank.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        # Empty file — DictReader sees no header.
        path.write_text("", encoding="utf-8")
        with pytest.raises(SchemaMismatch) as exc:
            list(CsvSource(path).read_customers())
        assert "missing a header row" in str(exc.value)

    def test_csv_oserror_wrapped(self, tmp_path: Path):
        # An unreadable file (e.g. a directory) surfaces OSError on `open`,
        # which the source re-raises as SourceUnavailable.
        path = tmp_path / "not_a_file"
        path.mkdir()  # It IS a directory; opening as a file raises IsADirectoryError.
        with pytest.raises(SourceUnavailable):
            list(CsvSource(path).read_customers())


# --- private date helpers --------------------------------------------------


class TestDateHelpers:
    def test_parse_date_ok(self):
        from datetime import date
        assert _parse_date("2024-01-02") == date(2024, 1, 2)

    def test_parse_date_bad_raises_schema_mismatch(self):
        with pytest.raises(SchemaMismatch):
            _parse_date("not-a-date")

    def test_parse_dt_ok(self):
        from datetime import datetime
        assert _parse_dt("2024-06-15T10:30:00") == datetime(2024, 6, 15, 10, 30)

    def test_parse_dt_bad_raises_schema_mismatch(self):
        with pytest.raises(SchemaMismatch):
            _parse_dt("garbage")
