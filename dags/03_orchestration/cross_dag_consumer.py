"""Consumer DAG — fires every time the producer's dataset updates.

This DAG is **data-driven only**: it has no time-based schedule. The
``schedule=ORDERS_DATASET`` parameter binds the consumer to the
producer's ``outlets=[...]`` declaration in
``dags/03_orchestration/cross_dag_producer.py``. Each time the
producer's ``build.update_dataset_summary`` task succeeds, the
scheduler records a dataset update and triggers this DAG exactly
once — no polling, no sensors, no ``TriggerDagRunOperator``.

Why datasets instead of TriggerDagRunOperator?
    Datasets make the producer→consumer edge *declarative*. Each side
    names a URI; the scheduler wires them up. If you change the
    producer's identity, you change one string on each side — there is
    no imperative cross-DAG reference to track down by hand.

See: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html
"""
from __future__ import annotations

import os
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from include.io import SqliteSource


# --- paths ----------------------------------------------------------

def _dataset_db_path() -> Path:
    """Mirror the producer's path. Anchored at AIRFLOW_HOME so the
    scheduler's working directory doesn't matter.
    """
    return Path(os.environ.get("AIRFLOW_HOME", ".")) / "data" / "datasets" / "orders_clean.db"


# --- dataset URI ----------------------------------------------------
# MUST equal the producer's ORDERS_DATASET_URI byte-for-byte. Dataset
# identity in Airflow is string equality on the URI; a single character
# of drift breaks the producer→consumer edge silently.

ORDERS_DATASET_URI: str = "sqlite://data/datasets/orders_clean.db"
ORDERS_DATASET: Dataset = Dataset(ORDERS_DATASET_URI)
CLEAN_TABLE: str = "orders_clean"


# --- task callables -------------------------------------------------

def summarize_clean_orders(**context: Any) -> str:
    """Read the dataset's SQLite DB and print a one-paragraph summary.

    Computes four aggregates the producer's cleaning pipeline makes
    cheap: row count, total ``revenue_cents``, average ``quantity``,
    and the count-by-status breakdown (preserved in the producer's
    table even though ``CleanedOrder`` strips ``status`` from the
    record).
    """
    source = SqliteSource(_dataset_db_path())
    rows: list[dict] = list(source.fetch(
        f"SELECT quantity, revenue_cents, status FROM {CLEAN_TABLE}"
    ))

    n = len(rows)
    total_revenue_cents: int = sum(r["revenue_cents"] for r in rows)
    avg_quantity: float = (sum(r["quantity"] for r in rows) / n) if n else 0.0
    by_status: dict[str, int] = dict(
        sorted(Counter(r["status"] for r in rows).items())
    )

    summary = (
        f"Dataset {ORDERS_DATASET_URI} summary: {n} cleaned rows, "
        f"total_revenue_cents={total_revenue_cents}, "
        f"average_quantity={avg_quantity:.2f}, "
        f"by_status={by_status}. "
        f"Triggered by dataset update at logical_date="
        f"{context.get('data_interval_start')}."
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
    dag_id="cross_dag_consumer",
    description=(
        "Triggered by the orders_clean dataset update emitted by "
        "cross_dag_producer. Reads the SQLite warehouse and prints a "
        "summary — no time-based schedule."
    ),
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    # The whole point: schedule *is* the dataset. No @daily / @hourly.
    schedule=[ORDERS_DATASET],
    catchup=False,
    tags=["datasets", "consumer"],
) as dag:

    with TaskGroup("consume") as consume_group:
        summarize = PythonOperator(
            task_id="summarize_clean_orders",
            python_callable=summarize_clean_orders,
        )