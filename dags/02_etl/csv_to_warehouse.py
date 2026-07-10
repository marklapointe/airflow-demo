"""csv_to_warehouse — a real ETL pipeline from CSV to a SQLite warehouse.

Pattern (Knuth, TAOCP §1.4.3 — the canonical pipeline shape):
  1. Extract  : three CSV sources → frozen dataclass records → XCom.
  2. Transform: `clean_orders(...)` drops cancelled/zero/negative rows
     and projects `OrderRecord` into the narrower `CleanedOrder`.
  3. Quality  : a side-count via `quality_report(...)`; a branch routes
     the run to `load` (passed) or `quality_alert` (failed).
  4. Load     : append cleaned orders into `fact_orders`, idempotent on
     `order_id` so retries are safe.

NOTE: the sample numbers in `include/data/orders.csv` are pinned by
`tests/unit/test_cleaning.py::TestAgainstSampleCsv` at 10 rows,
7 cleaned, 24282 cents of revenue. Do not silently change the sample.
"""
from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.utils.task_group import TaskGroup

from include.domain import (
    CleanedOrder,
    CustomerRecord,
    OrderRecord,
    ProductRecord,
    QualityReport,
)
from include.io import CsvSource, SqliteSink
from include.transforms import clean_orders, quality_report

# --- configuration --------------------------------------------------

WAREHOUSE_TABLE = "fact_orders"

# `order_id` PRIMARY KEY makes `INSERT OR IGNORE` idempotent — the
# second insert of the same `order_id` is silently dropped.
_FACT_ORDERS_DDL = f"""
CREATE TABLE IF NOT EXISTS {WAREHOUSE_TABLE} (
    order_id         INTEGER PRIMARY KEY,
    customer_id      INTEGER NOT NULL,
    product_id       INTEGER NOT NULL,
    quantity         INTEGER NOT NULL,
    unit_price_cents INTEGER NOT NULL,
    revenue_cents    INTEGER NOT NULL,
    ordered_at       TEXT    NOT NULL
)
"""


def _data_path(filename: str) -> Path:
    # Resolved off `__file__`, not cwd — the scheduler may have a
    # different working directory than the DAG author.
    return Path(__file__).resolve().parent.parent / "include" / "data" / filename


def _warehouse_path() -> Path:
    return Path(os.environ.get("AIRFLOW_HOME", ".")) / "data" / "warehouse.db"


def records_to_dicts(records: Iterable[CleanedOrder]) -> list[dict]:
    """Convert frozen dataclass records into SQLite-friendly dicts.

    `dataclasses.asdict` deep-copies and recurses through nested
    fields, but leaves `datetime` as a Python object. We coerce
    `ordered_at` to ISO 8601 so the dict round-trips through TEXT
    affinity cleanly and is JSON-friendly for downstream consumers.
    """
    out: list[dict] = []
    for r in records:
        d = asdict(r)
        d["ordered_at"] = r.ordered_at.isoformat()
        out.append(d)
    return out


# --- task callables -------------------------------------------------

def extract_customers() -> list[CustomerRecord]:
    return list(CsvSource(_data_path("customers.csv")).read_customers())


def extract_products() -> list[ProductRecord]:
    return list(CsvSource(_data_path("products.csv")).read_products())


def extract_orders() -> list[OrderRecord]:
    return list(CsvSource(_data_path("orders.csv")).read_orders())


def transform_clean_orders(**context: Any) -> list[CleanedOrder]:
    """Apply `clean_orders` to the raw orders pulled from `extract`."""
    ti = context["ti"]
    raw: list[OrderRecord] = ti.xcom_pull(task_ids="extract.extract_orders")
    return list(clean_orders(raw))


def run_quality_check(**context: Any) -> QualityReport:
    """Run `quality_report` against the raw orders; return the report.

    Returning the report (rather than just logging) lets both the
    branch downstream and the `quality_alert` BashOperator pull it.
    """
    ti = context["ti"]
    raw: list[OrderRecord] = ti.xcom_pull(task_ids="extract.extract_orders")
    _, report = quality_report(raw)
    return report


def choose_path(**context: Any) -> str:
    """BranchPythonOperator callback: `load` or `quality_alert`."""
    ti = context["ti"]
    report: QualityReport = ti.xcom_pull(task_ids="quality_check")
    return "load" if report.passed else "quality_alert"


def load_to_sqlite(**context: Any) -> int:
    """Materialise cleaned orders into the warehouse table.

    Returns the number of rows newly inserted; duplicates on re-run
    are silently dropped, which is what makes the load idempotent.
    """
    ti = context["ti"]
    cleaned: list[CleanedOrder] = ti.xcom_pull(task_ids="transform.clean_orders")
    if not cleaned:
        return 0
    sink = SqliteSink(_warehouse_path())
    sink.ensure_table(_FACT_ORDERS_DDL)
    return sink.write(WAREHOUSE_TABLE, records_to_dicts(cleaned), conflict_target="order_id")


# --- DAG ------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="csv_to_warehouse",
    description="CSV → clean → quality check → SQLite warehouse",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "csv", "sqlite"],
) as dag:

    with TaskGroup("extract") as extract:
        customers_task = PythonOperator(
            task_id="extract_customers",
            python_callable=extract_customers,
        )
        products_task = PythonOperator(
            task_id="extract_products",
            python_callable=extract_products,
        )
        orders_task = PythonOperator(
            task_id="extract_orders",
            python_callable=extract_orders,
        )

    with TaskGroup("transform") as transform:
        clean_orders_task = PythonOperator(
            task_id="clean_orders",
            python_callable=transform_clean_orders,
        )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=run_quality_check,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_quality",
        python_callable=choose_path,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_to_sqlite,
    )

    quality_alert = BashOperator(
        task_id="quality_alert",
        bash_command=(
            "echo 'Quality check FAILED — see report:'\n"
            "echo \"{{ ti.xcom_pull(task_ids='quality_check') }}\"\n"
        ),
    )

    final = EmptyOperator(
        task_id="final",
        trigger_rule="none_failed_or_skipped",
    )

    # `extract_orders` fans out: one branch to quality_check → branch,
    # the other to clean_orders → load. `final` joins both targets
    # of the branch; the non-chosen target is skipped, hence the
    # `none_failed_or_skipped` trigger rule.
    orders_task >> quality_check >> branch
    orders_task >> clean_orders_task >> load
    branch >> [load, quality_alert]
    [load, quality_alert] >> final