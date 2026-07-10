"""Producer DAG — emits the ``orders_clean`` dataset.

Pattern (Airflow 3.0 dataset-driven scheduling):

    build.extract_clean_orders
            │  reads include/data/orders.csv via ``CsvSource``
            │  applies ``clean_orders(...)`` from include/transforms
            │  writes the result to ``data/datasets/orders_clean.db``
            │  via ``SqliteSink`` (idempotent on ``order_id``)
            ▼
    build.update_dataset_summary
            │  reads the freshly-written SQLite table
            │  logs a one-line summary
            │  carries ``outlets=[ORDERS_DATASET]`` ← the producer edge
            ▼
       (scheduler records a dataset update; the matching consumer DAG
        ``cross_dag_consumer`` schedules on this dataset and fires.)

Why this is *the* modern answer to cross-DAG dependencies:
    The producer advertises *what* it produces (a single line:
    ``outlets=[Dataset(...)]``). The consumer advertises *what* it
    consumes (one line: ``schedule=Dataset(...)``). The scheduler
    wires them up. There is no imperative cross-DAG pointer to update
    when one side changes — see
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html
"""
from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from include.domain import CleanedOrder, OrderRecord
from include.io import CsvSource, SqliteSink, SqliteSource
from include.transforms import clean_orders


# --- paths ----------------------------------------------------------

def _project_root() -> Path:
    """Anchor everything off AIRFLOW_HOME so the scheduler can run from
    any working directory (mirrors the pattern in csv_to_warehouse.py).
    """
    return Path(os.environ.get("AIRFLOW_HOME", ".")).resolve()


def _data_path(filename: str) -> Path:
    return Path(__file__).resolve().parent.parent / "include" / "data" / filename


def _dataset_db_path() -> Path:
    p = _project_root() / "data" / "datasets" / "orders_clean.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


# --- dataset URI ----------------------------------------------------
# Stable, human-readable identifier. Both producer and consumer reference
# the same string; Airflow treats dataset identity as string equality
# on the URI. Form ``sqlite://<relative-path>`` is unambiguous within
# this project (anchored at AIRFLOW_HOME) and survives copy/clone.

ORDERS_DATASET_URI: str = "sqlite://data/datasets/orders_clean.db"
ORDERS_DATASET: Dataset = Dataset(ORDERS_DATASET_URI)


# --- DDL ------------------------------------------------------------
# Matches the ``CleanedOrder`` projection plus ``status`` (which
# ``clean_orders`` deliberately drops from the record but which the
# consumer wants for its count-by-status breakdown). Re-using the
# producer's DB for a status-aware summary keeps the consumer
# stateless — no need to re-read the source CSV.

CLEAN_TABLE: str = "orders_clean"
_CLEAN_DDL: str = f"""
CREATE TABLE IF NOT EXISTS {CLEAN_TABLE} (
    order_id         INTEGER PRIMARY KEY,
    customer_id      INTEGER NOT NULL,
    product_id       INTEGER NOT NULL,
    quantity         INTEGER NOT NULL,
    unit_price_cents INTEGER NOT NULL,
    revenue_cents    INTEGER NOT NULL,
    ordered_at       TEXT    NOT NULL,
    status           TEXT    NOT NULL
)
"""


def _records_to_dicts(
    records: Iterable[CleanedOrder],
    status_by_id: dict[int, str],
) -> list[dict]:
    """Project ``CleanedOrder`` rows to SQLite-friendly dicts, reattaching
    ``status`` (looked up from the pre-clean stream).

    ``dataclasses.asdict`` deep-copies but leaves ``datetime`` as a Python
    object; we coerce ``ordered_at`` to ISO 8601 so it round-trips through
    TEXT affinity cleanly and is JSON-friendly for downstream consumers.
    """
    out: list[dict] = []
    for r in records:
        d = asdict(r)
        d["ordered_at"] = r.ordered_at.isoformat()
        d["status"] = status_by_id[int(r.order_id)]
        out.append(d)
    return out


# --- task callables -------------------------------------------------

def extract_clean_orders(**context: Any) -> int:
    """CSV → ``clean_orders`` → SqliteSink (idempotent on ``order_id``).

    Returns the number of *newly inserted* rows. On a re-run of the
    same logical day the natural-key conflict silently drops the
    duplicates, which is exactly what makes the load retry-safe.
    """
    raw: list[OrderRecord] = list(
        CsvSource(_data_path("orders.csv")).read_orders()
    )
    # ``clean_orders`` filters out ``cancelled`` orders via
    # ``drop_unknown_status``; snapshotting the status here lets us
    # reattach it for the summary later.
    status_by_id: dict[int, str] = {int(o.order_id): o.status for o in raw}
    cleaned: list[CleanedOrder] = list(clean_orders(raw))
    sink = SqliteSink(_dataset_db_path())
    sink.ensure_table(_CLEAN_DDL)
    return sink.write(
        CLEAN_TABLE,
        _records_to_dicts(cleaned, status_by_id),
        conflict_target="order_id",
    )


def update_dataset_summary(**context: Any) -> str:
    """Read the freshly-written SQLite table and emit a one-line summary.

    Declared with ``outlets=[ORDERS_DATASET]`` so every successful run
    of this task registers a dataset update; the scheduler then fans
    out to every DAG whose ``schedule=`` is this dataset.
    """
    ti = context["ti"]
    inserted: int = ti.xcom_pull(task_ids="build.extract_clean_orders")
    source = SqliteSource(_dataset_db_path())
    rows = list(source.fetch(
        f"SELECT COUNT(*) AS n, "
        f"COALESCE(SUM(revenue_cents), 0) AS rev "
        f"FROM {CLEAN_TABLE}"
    ))
    row_count = rows[0]["n"]
    total_rev = rows[0]["rev"]
    summary = (
        f"[{ORDERS_DATASET_URI}] inserted={inserted} "
        f"total_in_table={row_count} total_revenue_cents={total_rev}"
    )
    print(summary)
    return summary


# --- DAG ------------------------------------------------------------

default_args: dict[str, Any] = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="cross_dag_producer",
    description=(
        "Daily CSV → clean → SQLite. Emits the orders_clean dataset on "
        "which the cross_dag_consumer DAG schedules."
    ),
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["datasets", "producer", "etl", "sqlite"],
) as dag:

    with TaskGroup("build") as build_group:
        extract_clean = PythonOperator(
            task_id="extract_clean_orders",
            python_callable=extract_clean_orders,
        )

        # Airflow 3.0 producer edge — declaring outlets registers this
        # task's success as an update of ORDERS_DATASET. The scheduler
        # picks up the change and triggers every DAG scheduled on it.
        update_summary = PythonOperator(
            task_id="update_dataset_summary",
            python_callable=update_dataset_summary,
            outlets=[ORDERS_DATASET],
        )

        extract_clean >> update_summary