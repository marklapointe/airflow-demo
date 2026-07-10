"""Kitchen-sink demo of Airflow 3.0 basics.

DESIGN NOTE
-----------
This DAG is the canonical "hello world" of Airflow features. Anyone reading
the file from index 0 should understand its scope in 3 lines: it shows how to
wire operators, branch on XCom, group work in a TaskGroup, and converge with a
one-success join — nothing more. No external APIs, no clever abstractions.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from include.io import CsvSource


# --- paths -----------------------------------------------------------------

# Resolve relative to the project root so the DAG works regardless of the
# working directory Airflow was started from.
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
CUSTOMERS_CSV = _PROJECT_ROOT / "include" / "data" / "customers.csv"


# --- task id constants -----------------------------------------------------
#
# Operators inside a TaskGroup are prefixed with the group id at runtime, so
# the *qualified* id (e.g. "processing_group.extract_customer_count") is what
# `xcom_pull` actually expects. We expose both the bare and the qualified
# forms as constants so nothing has to be repeated as a string literal.

TASK_EXTRACT = "extract_customer_count"
TASK_VALIDATE = "validate"
GROUP_PROCESSING = "processing_group"
EXTRACT_FULL_ID = f"{GROUP_PROCESSING}.{TASK_EXTRACT}"

TASK_BRANCH = "branch_on_count"
TASK_HIGH = "high_value_path"
TASK_LOW = "low_value_path"
TASK_JOIN = "final_report"
TASK_END = "end"

# Branching threshold. Counts above (or equal to) this take the full path.
HIGH_VALUE_THRESHOLD = 3


# --- default args ----------------------------------------------------------

default_args: dict[str, Any] = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


# --- python_callables ------------------------------------------------------

def extract_customer_count() -> int:
    """Read customers.csv via the shared `CsvSource` and return the row count.

    The return value is auto-pushed to XCom by `PythonOperator` and consumed
    downstream by `branch_on_count`.
    """
    src = CsvSource(CUSTOMERS_CSV)
    count = sum(1 for _ in src.read_customers())
    print(f"Read {count} customer rows from {CUSTOMERS_CSV.name}")
    return count


def branch_on_count(**context: Any) -> str:
    """Pick a leaf branch based on the count pulled from XCom."""
    count: int = context["ti"].xcom_pull(task_ids=EXTRACT_FULL_ID)
    chosen = TASK_HIGH if count >= HIGH_VALUE_THRESHOLD else TASK_LOW
    print(f"Count={count} (threshold={HIGH_VALUE_THRESHOLD}); branching to {chosen!r}")
    return chosen


def process_high_value(**context: Any) -> None:
    """Full-pipeline path — pretend we do the heavy lifting here."""
    count: int = context["ti"].xcom_pull(task_ids=EXTRACT_FULL_ID)
    print(f"[high-value] running full pipeline for {count} customers")


def process_low_value(**context: Any) -> None:
    """Summary-only path — skip the heavy lifting because volume is low."""
    count: int = context["ti"].xcom_pull(task_ids=EXTRACT_FULL_ID)
    print(f"[low-value] emitting summary for {count} customers")


def finalize(**context: Any) -> None:
    """Join task — runs as long as *any* upstream branch succeeded."""
    chosen: str = context["ti"].xcom_pull(task_ids=TASK_BRANCH)
    print(f"[final] DAG done; branch taken was {chosen!r}")


# --- DAG -------------------------------------------------------------------

with DAG(
    dag_id="airflow_features_demo",
    description=(
        "Kitchen-sink demo of Airflow basics: operators, XCom, TaskGroup, "
        "branching, and a one-success join."
    ),
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    tags=["example", "demo", "basics"],
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup(GROUP_PROCESSING) as processing:
        extract = PythonOperator(
            task_id=TASK_EXTRACT,
            python_callable=extract_customer_count,
        )
        validate = BashOperator(
            task_id=TASK_VALIDATE,
            bash_command='echo "validated customer count from upstream"',
        )
        extract >> validate

    branching = BranchPythonOperator(
        task_id=TASK_BRANCH,
        python_callable=branch_on_count,
    )

    high_value_path = PythonOperator(
        task_id=TASK_HIGH,
        python_callable=process_high_value,
    )

    low_value_path = PythonOperator(
        task_id=TASK_LOW,
        python_callable=process_low_value,
    )

    final_report = PythonOperator(
        task_id=TASK_JOIN,
        python_callable=finalize,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    end = EmptyOperator(task_id=TASK_END)

    # --- data flow ---------------------------------------------------------
    #
    # start  ->  processing_group {extract -> validate}
    #       ->  branch_on_count (XCom: extract_customer_count)
    #       ->  [high_value_path | low_value_path]
    #       ->  final_report      (trigger_rule=one_success)
    #       ->  end
    #
    # Real data enters at `extract` via the shared `CsvSource`; everything
    # downstream is local orchestration that consumes that count through XCom.
    start >> processing >> branching
    branching >> [high_value_path, low_value_path] >> final_report >> end