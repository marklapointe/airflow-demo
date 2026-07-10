"""data_quality pipeline — quality gates that run BEFORE the warehouse load.

Why a dedicated DAG: the warehouse load is expensive and the *whole batch* is
worthless if any quality invariant is broken; running checks in their own DAG
isolates failures so a bad batch costs the cheap check DAG, not the heavy ETL
load. The report is also useful when *no* load happens — on-call consumes it.

Shape: extract -> checks.{row_count, schema, null, duplicate, value_range}
      -> aggregate_report -> branch_on_result -> quality_ok | quality_failed
      -> finish (trigger_rule='all_done').
"""
from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator, PythonOperator,
)
from airflow.utils.task_group import TaskGroup

from include.io import CsvSource

# --- constants ------------------------------------------------------------
ORDERS_CSV_PATH: Path = (
    Path(__file__).resolve().parents[2] / "include" / "data" / "orders.csv"
)
EXPECTED_MIN_ROWS: int = 5
EXPECTED_MAX_ROWS: int = 10_000
NULL_FIELDS: tuple[str, ...] = (
    "order_id", "customer_id", "product_id", "quantity",
    "ordered_at", "unit_price_cents", "status",
)
ALLOWED_STATUSES: frozenset[str] = frozenset({"pending", "shipped", "cancelled"})
CHECK_NAMES: tuple[str, ...] = (
    "row_count_check", "schema_check", "null_check",
    "duplicate_check", "value_range_check",
)


# --- helpers --------------------------------------------------------------
def _read_csv_raw(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Open ``path`` and return ``(headers, raw row dicts)``.

    Bypasses :class:`CsvSource` so empty strings stay empty (rather than
    collapsing to ``None``) — that distinction is what ``null_check`` flags.
    """
    if not path.exists():
        raise FileNotFoundError(f"orders CSV missing at {path}")
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError(f"{path} is missing a header row")
        return list(reader.fieldnames), [dict(r) for r in reader]


def _safe_int(value: str, default: int) -> int:
    """Parse ``value`` as int, returning ``default`` on parse failure.

    Used so a malformed cell counts as a violation rather than crashing the
    whole check with ``ValueError``.
    """
    try:
        return int(value)
    except ValueError:
        return default


def _emit(ti: Any, name: str, passed: bool, details: str) -> dict[str, Any]:
    """Push ``{check, passed, details}`` to XCom under key ``name``; raise on failure.

    Pushing under the check name (rather than relying on Airflow's default
    ``return_value`` push) keeps each summary under a stable key, so the
    aggregate step is immune to out-of-order execution.
    """
    payload: dict[str, Any] = {"check": name, "passed": bool(passed), "details": details}
    ti.xcom_push(key=name, value=payload)
    if not passed:
        raise ValueError(f"{name} failed: {details}")
    return payload


def _ti(context: Any) -> Any:
    return context["ti"]


# --- task callables -------------------------------------------------------
def extract(**context: Any) -> dict[str, int]:
    """Open the CSV once, push headers + rows to XCom for the check tasks."""
    headers, rows = _read_csv_raw(ORDERS_CSV_PATH)
    ti = _ti(context)
    ti.xcom_push(key="headers", value=headers)
    ti.xcom_push(key="rows", value=rows)
    return {"row_count": len(rows)}


def row_count_check(**context: Any) -> dict[str, Any]:
    """Invariant: row count is between EXPECTED_MIN_ROWS and EXPECTED_MAX_ROWS."""
    n = len(_ti(context).xcom_pull(task_ids="extract", key="rows"))
    passed = EXPECTED_MIN_ROWS <= n <= EXPECTED_MAX_ROWS
    return _emit(_ti(context), "row_count_check", passed,
                 f"{n} rows (expected {EXPECTED_MIN_ROWS}..{EXPECTED_MAX_ROWS})")


def schema_check(**context: Any) -> dict[str, Any]:
    """Invariant: every column in ``CsvSource.REQUIRED_ORDER_COLS`` is present."""
    headers = _ti(context).xcom_pull(task_ids="extract", key="headers")
    expected = CsvSource.REQUIRED_ORDER_COLS
    missing = [c for c in expected if c not in headers]
    return _emit(_ti(context), "schema_check", not missing,
                 (f"all {len(expected)} required columns present"
                  if not missing else f"missing columns {missing} in header {headers}"))


def null_check(**context: Any) -> dict[str, Any]:
    """Invariant: no row has an empty/None value in any required field."""
    rows = _ti(context).xcom_pull(task_ids="extract", key="rows")
    offenders: list[tuple[int, str]] = [
        (i, f) for i, row in enumerate(rows)
        for f in NULL_FIELDS
        if (v := row.get(f)) is None or (isinstance(v, str) and v.strip() == "")
    ]
    if not offenders:
        details = "no null required fields"
    else:
        details = f"{len(offenders)} null violations (first 5: {offenders[:5]})"
    return _emit(_ti(context), "null_check", not offenders, details)


def duplicate_check(**context: Any) -> dict[str, Any]:
    """Invariant: every order_id appears exactly once across the dataset."""
    rows = _ti(context).xcom_pull(task_ids="extract", key="rows")
    counts: dict[str, int] = {}
    for row in rows:
        oid = (row.get("order_id") or "").strip()
        if oid:
            counts[oid] = counts.get(oid, 0) + 1
    duplicates = [oid for oid, n in counts.items() if n > 1]
    passed = not duplicates
    details = (f"{len(counts)} unique order_ids" if passed
               else f"{len(duplicates)} duplicate order_ids (first 5: {duplicates[:5]})")
    return _emit(_ti(context), "duplicate_check", passed, details)


def value_range_check(**context: Any) -> dict[str, Any]:
    """Invariant: quantity >= 1, unit_price_cents > 0, status in ALLOWED_STATUSES."""
    rows = _ti(context).xcom_pull(task_ids="extract", key="rows")
    bad_qty: list[tuple[int, str]] = []
    bad_price: list[tuple[int, str]] = []
    bad_status: list[tuple[int, str]] = []
    for idx, row in enumerate(rows):
        qty_raw = (row.get("quantity") or "").strip()
        price_raw = (row.get("unit_price_cents") or "").strip()
        status = (row.get("status") or "").strip()
        if _safe_int(qty_raw, -1) < 1:
            bad_qty.append((idx, qty_raw))
        if _safe_int(price_raw, 0) <= 0:
            bad_price.append((idx, price_raw))
        if status not in ALLOWED_STATUSES:
            bad_status.append((idx, status))
    passed = not bad_qty and not bad_price and not bad_status
    if passed:
        details = "all quantity/price/status values in range"
    else:
        details = f"violations: qty={len(bad_qty)} price={len(bad_price)} status={len(bad_status)}"
    return _emit(_ti(context), "value_range_check", passed, details)


def aggregate_report(**context: Any) -> dict[str, Any]:
    """Pull each check's summary and emit ``{all_passed, checks}``.

    A check that raised before reaching ``_emit`` leaves ``None`` in its slot —
    we synthesise a failed summary so downstream branching stays well-defined.
    """
    ti = _ti(context)
    summaries: list[dict[str, Any]] = []
    for name in CHECK_NAMES:
        s = ti.xcom_pull(task_ids=f"checks.{name}", key=name)
        summaries.append(s if s is not None else
                         {"check": name, "passed": False,
                          "details": "task did not produce a summary (raised before push)"})
    report: dict[str, Any] = {
        "all_passed": all(item["passed"] for item in summaries),
        "checks": summaries,
    }
    print(f"FINAL REPORT: {report}")
    ti.xcom_push(key="final_report", value=report)
    return report


def branch_on_result(**context: Any) -> str:
    """Return ``quality_ok`` or ``quality_failed`` based on the final report."""
    report = _ti(context).xcom_pull(task_ids="aggregate_report", key="final_report")
    return "quality_ok" if report and report.get("all_passed") else "quality_failed"


# --- DAG assembly ---------------------------------------------------------

with DAG(
    dag_id="data_quality_checks",
    description="Daily quality gates on the orders CSV — runs BEFORE the warehouse load.",
    schedule="@daily",
    catchup=False,
    start_date=datetime(2024, 1, 1),
    tags=["etl", "quality", "02_etl"],
) as dag:

    extract_op = PythonOperator(task_id="extract", python_callable=extract)

    with TaskGroup("checks") as checks_group:
        PythonOperator(task_id="row_count_check", python_callable=row_count_check)
        PythonOperator(task_id="schema_check", python_callable=schema_check)
        PythonOperator(task_id="null_check", python_callable=null_check)
        PythonOperator(task_id="duplicate_check", python_callable=duplicate_check)
        PythonOperator(task_id="value_range_check", python_callable=value_range_check)

    aggregate_op = PythonOperator(task_id="aggregate_report", python_callable=aggregate_report)
    branch_op = BranchPythonOperator(task_id="branch_on_result", python_callable=branch_on_result)
    quality_ok_op = BashOperator(task_id="quality_ok", bash_command='echo "Quality OK"')
    quality_failed_op = BashOperator(
        task_id="quality_failed", bash_command='echo "Quality FAILED" && exit 1',
    )
    finish_op = EmptyOperator(task_id="finish", trigger_rule="all_done")

    extract_op >> checks_group >> aggregate_op >> branch_op
    branch_op >> [quality_ok_op, quality_failed_op]
    quality_ok_op >> finish_op
    quality_failed_op >> finish_op
