"""hooks_and_connections.py - Hooks and Connections in Airflow.

A Hook is Airflow's integration seam: a small Python object that knows how to
connect to one external system and expose focused methods for operators and DAG
code.  Use a Hook when connection setup, retries, credentials, or protocol
specifics would otherwise leak into every task body.

Connections are abstractions over credentials and endpoints.  The DAG asks for a
``conn_id`` such as ``my_http`` or ``hooks_demo_sqlite``; Airflow resolves that
identifier from the UI, secrets backends, or ``AIRFLOW_CONN_*`` environment
variables.  That indirection lets the same DAG run in dev, CI, and production
without hard-coding hostnames or passwords.

This learning DAG demonstrates three patterns:

A. Built-in provider Hook: ``SqliteHook`` writes and reads demo rows.
B. Custom Hook: ``CsvHook`` wraps the project-local ``CsvSource``.  Keeping the
   class here is convenient for a single-file lesson; in production it would
   move to ``plugins/hooks/`` or a shared package so multiple DAGs can reuse and
   test it independently.
C. Connection lookup: ``BaseHook.get_connection`` returns a Connection object,
   with the missing-connection case handled gracefully.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar

from airflow import DAG
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from include.domain import CustomerRecord
from include.io import CsvSource


# ---------------------------------------------------------------------------
# Paths and reusable constants
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
CUSTOMERS_CSV: Path = PROJECT_ROOT / "include" / "data" / "customers.csv"
DEMO_SQLITE_PATH: Path = Path("/tmp/airflow_hooks_demo.sqlite")
SQLITE_CONN_ID: str = "hooks_demo_sqlite"
HTTP_CONN_ID: str = "my_http"

DEMO_TABLE_DDL: str = """
CREATE TABLE IF NOT EXISTS demo_table (
    demo_id INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    created_on TEXT NOT NULL
)
"""
DEMO_ROWS: tuple[tuple[int, str, str], ...] = (
    (1, "SqliteHook hides the DB-API connection setup", "2024-01-01"),
    (2, "Connections let a DAG use a stable conn_id", "2024-01-02"),
)

# ---------------------------------------------------------------------------
# default_args - applied to every task in this DAG
# ---------------------------------------------------------------------------
default_args: dict[str, Any] = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


# ---------------------------------------------------------------------------
# Helper functions and tiny hook/adapter classes
# ---------------------------------------------------------------------------
def _ensure_demo_sqlite_connection() -> None:
    """Provide a local SQLite connection via Airflow's env-var abstraction.

    A real deployment would create ``hooks_demo_sqlite`` in the Airflow UI or a
    secrets backend.  For this self-contained learning DAG, the task process
    supplies ``AIRFLOW_CONN_HOOKS_DEMO_SQLITE`` when it is absent so the built-in
    ``SqliteHook`` can still demonstrate a real local SQLite file.
    """
    env_name: str = f"AIRFLOW_CONN_{SQLITE_CONN_ID.upper()}"
    os.environ.setdefault(env_name, f"sqlite:///{DEMO_SQLITE_PATH}")


def _sqlite_hook() -> SqliteHook:
    """Return the built-in SQLite provider hook used by both SQLite tasks."""
    _ensure_demo_sqlite_connection()
    return SqliteHook(sqlite_conn_id=SQLITE_CONN_ID)


class CsvHook(BaseHook):
    """Minimal custom Hook that wraps ``include.io.CsvSource``.

    This intentionally keeps only a tiny surface area: read customer records and
    describe the source.  Production Hooks usually live outside a single DAG file
    (for example under ``plugins/hooks/``) so they can be versioned, imported by
    multiple DAGs, and covered by focused unit tests.
    """

    conn_name_attr: ClassVar[str] = "csv_conn_id"
    default_conn_name: ClassVar[str] = "csv_default"
    conn_type: ClassVar[str] = "csv"
    hook_name: ClassVar[str] = "CSV"

    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path: Path = path
        self._source: CsvSource = CsvSource(path)

    def read_customers(self) -> list[CustomerRecord]:
        """Read all customer records through the wrapped ``CsvSource``."""
        return list(self._source.read_customers())

    def describe(self) -> dict[str, Any]:
        """Return small metadata about the CSV source for logging/demo use."""
        customers: list[CustomerRecord] = self.read_customers()
        countries: list[str] = sorted({customer.country for customer in customers})
        return {
            "path": str(self.path),
            "columns": list(CsvSource.REQUIRED_CUSTOMER_COLS),
            "row_count": len(customers),
            "countries": countries,
        }


class ConnAdapter:
    """Tiny adapter showing the shape of ``BaseHook.get_connection`` usage."""

    def __init__(self, conn_id: str) -> None:
        self.conn_id: str = conn_id

    def describe(self) -> dict[str, Any]:
        """Return safe connection details or a graceful missing-connection note."""
        try:
            conn: Any = BaseHook.get_connection(conn_id=self.conn_id)
        except AirflowNotFoundException as exc:
            return {
                "conn_id": self.conn_id,
                "status": "missing",
                "message": (
                    "Create this connection in the Airflow UI or define "
                    f"AIRFLOW_CONN_{self.conn_id.upper()} to make this adapter live."
                ),
                "error": str(exc),
            }

        extra: dict[str, Any] = conn.extra_dejson or {}
        return {
            "conn_id": conn.conn_id,
            "conn_type": conn.conn_type,
            "host": conn.host,
            "schema": conn.schema,
            "login_present": bool(conn.login),
            "password_present": bool(conn.password),
            "port": conn.port,
            "extra_keys": sorted(extra),
            "status": "found",
        }


def insert_demo_rows() -> None:
    """Create ``demo_table`` if needed and upsert two demo rows."""
    hook: SqliteHook = _sqlite_hook()
    conn: sqlite3.Connection = hook.get_conn()
    try:
        cursor: sqlite3.Cursor = conn.cursor()
        cursor.execute(DEMO_TABLE_DDL)
        cursor.executemany(
            """
            INSERT OR REPLACE INTO demo_table (demo_id, description, created_on)
            VALUES (?, ?, ?)
            """,
            DEMO_ROWS,
        )
        conn.commit()
    finally:
        conn.close()

    print(
        f"[A] insert_demo_rows: wrote {len(DEMO_ROWS)} rows to "
        f"{DEMO_SQLITE_PATH} via conn_id={SQLITE_CONN_ID!r}"
    )


def query_and_summarise() -> None:
    """Read back rows from ``demo_table`` and print a compact summary."""
    hook: SqliteHook = _sqlite_hook()
    conn: sqlite3.Connection = hook.get_conn()
    try:
        conn.row_factory = sqlite3.Row
        cursor: sqlite3.Cursor = conn.cursor()
        rows: list[sqlite3.Row] = cursor.execute(
            """
            SELECT demo_id, description, created_on
            FROM demo_table
            ORDER BY demo_id
            """
        ).fetchall()
    finally:
        conn.close()

    for row in rows:
        print(f"[A] query_and_summarise row: {dict(row)}")
    print(f"[A] query_and_summarise: read {len(rows)} rows from demo_table")


def _customer_to_dict(customer: CustomerRecord) -> dict[str, int | str]:
    """Convert a typed ``CustomerRecord`` to a log-friendly dictionary."""
    return {
        "customer_id": int(customer.customer_id),
        "email": customer.email,
        "signup_date": customer.signup_date.isoformat(),
        "country": customer.country,
    }


def load_via_hook() -> None:
    """Use the custom ``CsvHook`` to read customer rows from the sample CSV."""
    hook: CsvHook = CsvHook(path=CUSTOMERS_CSV)
    customers: list[CustomerRecord] = hook.read_customers()
    for customer in customers:
        print(f"[B] load_via_hook row: {_customer_to_dict(customer)}")
    print(f"[B] load_via_hook: loaded {len(customers)} customers")


def describe_via_hook() -> None:
    """Use the custom ``CsvHook`` to fetch source metadata."""
    hook: CsvHook = CsvHook(path=CUSTOMERS_CSV)
    metadata: dict[str, Any] = hook.describe()
    print(f"[B] describe_via_hook: {metadata}")


def describe_connection() -> None:
    """Show how a task would use a Connection object returned by BaseHook."""
    adapter: ConnAdapter = ConnAdapter(conn_id=HTTP_CONN_ID)
    details: dict[str, Any] = adapter.describe()
    print(f"[C] describe_connection: {details}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="hooks_and_connections",
    description="Built-in hooks, custom hooks, and Airflow connection lookup.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["learning", "hooks", "connections", "advanced"],
) as dag:

    # =======================================================================
    # A) Built-in hook - SqliteHook provider integration
    # =======================================================================
    with TaskGroup(group_id="sqlite_hook") as sqlite_hook:
        insert_rows_task: PythonOperator = PythonOperator(
            task_id="insert_demo_rows",
            python_callable=insert_demo_rows,
        )

        query_rows_task: PythonOperator = PythonOperator(
            task_id="query_and_summarise",
            python_callable=query_and_summarise,
        )

        insert_rows_task >> query_rows_task

    # =======================================================================
    # B) Custom hook - CsvHook wraps include.io.CsvSource
    # =======================================================================
    with TaskGroup(group_id="custom_hook") as custom_hook:
        load_customers_task: PythonOperator = PythonOperator(
            task_id="load_via_hook",
            python_callable=load_via_hook,
        )

        describe_customers_task: PythonOperator = PythonOperator(
            task_id="describe_via_hook",
            python_callable=describe_via_hook,
        )

        load_customers_task >> describe_customers_task

    # =======================================================================
    # C) Connection via UI/env - BaseHook.get_connection and missing conn
    # =======================================================================
    with TaskGroup(group_id="connection") as connection:
        describe_connection_task: PythonOperator = PythonOperator(
            task_id="describe_connection",
            python_callable=describe_connection,
        )

    connections_done: EmptyOperator = EmptyOperator(task_id="connections_done")

    [sqlite_hook, custom_hook, connection] >> connections_done
