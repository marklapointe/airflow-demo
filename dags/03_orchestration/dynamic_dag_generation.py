"""
Dynamic DAG Generation Example.

This DAG demonstrates two distinct flavours of "tasks produced at parse time":

1. STATIC FAN-OUT (``TaskGroup "static_fanout"``)
   The set of downstream systems is KNOWN at design time (a hardcoded list of
   external system names).  We iterate over that list ONCE inside the DAG body
   and emit one ``PythonOperator`` per system, naming each deterministically as
   ``process_<system>``.

   Use this when the catalogue of downstream work is stable and you want full
   control over per-branch task IDs, retries, trigger rules, and UI rendering.
   The trade-off is that adding or removing a branch requires a code change
   and redeploy.

2. TRULY DYNAMIC FAN-OUT (``TaskGroup "dynamic_fanout"``)
   The set of inputs is only known at RUN time.  We use the modern TaskFlow
   ``.expand()`` mapping idiom: ``square.expand(n=produce_numbers())``.  Airflow
   materialises one mapped task instance per element of the upstream list and
   gives each one an auto-generated, indexed name (``square_0``, ``square_1``,
   ...).  The DAG definition itself is tiny and does not change as the
   upstream payload grows or shrinks.

   Use this when the cardinality of work is data-driven and may vary per run.
   The trade-off is that you surrender some control over per-instance task IDs
   (you can only influence them via ``expand_kwargs`` / map metadata), and the
   mapped inputs must be JSON-serialisable.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATIC_SYSTEMS: list[str] = [
    "salesforce",
    "stripe",
    "hubspot",
    "snowflake",
    "segment",
]


def _default_args() -> dict[str, Any]:
    """Default args shared by every operator in this DAG."""
    return {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }


# ---------------------------------------------------------------------------
# Python callables for the static fan-out
# ---------------------------------------------------------------------------


def choose_items() -> list[str]:
    """Read the static catalogue of systems and push it to XCom.

    Implemented as a plain ``python_callable`` so that the next operator can
    consume it via ``ti.xcom_pull`` if desired.  (In practice the downstream
    loop also uses the module-level ``STATIC_SYSTEMS`` constant for
    determinism -- XCom here is illustrative.)
    """
    items: list[str] = list(STATIC_SYSTEMS)
    print(f"[choose_items] dispatching work for: {items}")
    return items


def _make_process_fn(system: str):
    """Return a closure that simulates per-system work."""

    def _process() -> dict[str, Any]:
        result: dict[str, Any] = {
            "system": system,
            "status": "ok",
            "rows_processed": 100,  # placeholder
        }
        print(f"[process_{system}] completed: {result}")
        return result

    # Cosmetic: helps tracebacks / Airflow logs identify the per-branch callable.
    _process.__name__ = f"process_{system}"
    return _process


def join_results(**context: Any) -> dict[str, Any]:
    """Pull every per-system result and aggregate it into one summary."""
    ti = context["ti"]
    aggregated: dict[str, Any] = {}
    # Use the fully-qualified task_id (TaskGroup prefix included) so XCom
    # resolution is unambiguous regardless of caller location.
    for system in STATIC_SYSTEMS:
        full_task_id: str = f"static_fanout.process_{system}"
        value: dict[str, Any] | None = ti.xcom_pull(task_ids=full_task_id)
        aggregated[system] = value
    print(f"[join_results] aggregated: {aggregated}")
    return aggregated


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dynamic_dag_generation",
    description="Static fan-out via loop + dynamic fan-out via TaskFlow expand().",
    default_args=_default_args(),
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["learning", "dynamic-generation"],
) as dag:

    # ---------------------------------------------------------------------
    # FLAVOUR 1 -- Static fan-out (loop over a known catalogue at parse time)
    # ---------------------------------------------------------------------
    with TaskGroup(
        group_id="static_fanout",
        tooltip="Known-at-design-time fan-out",
    ) as static_fanout_group:

        choose_items_op = PythonOperator(
            task_id="choose_items",
            python_callable=choose_items,
        )

        # Emit one PythonOperator per system.  task_ids are deterministic and
        # human-readable, e.g. ``process_salesforce``, ``process_stripe``,
        # which gives us clean Graph View labels in the UI.
        process_ops: list[PythonOperator] = [
            PythonOperator(
                task_id=f"process_{system}",  # includes the system name
                python_callable=_make_process_fn(system),
            )
            for system in STATIC_SYSTEMS
        ]

        join_results_op = PythonOperator(
            task_id="join_results",
            python_callable=join_results,
            trigger_rule="all_done",
        )

        choose_items_op >> process_ops >> join_results_op

    # ---------------------------------------------------------------------
    # FLAVOUR 2 -- Truly dynamic fan-out via TaskFlow .expand()
    # ---------------------------------------------------------------------
    with TaskGroup(
        group_id="dynamic_fanout",
        tooltip="Runtime-driven fan-out via TaskFlow mapping",
    ) as dynamic_fanout_group:

        @task
        def produce_numbers() -> list[int]:
            """Produce the dynamic input list at runtime."""
            numbers: list[int] = [1, 2, 3, 4, 5]
            print(f"[produce_numbers] -> {numbers}")
            return numbers

        @task
        def square(n: int) -> int:
            """Map over each input integer."""
            result: int = n * n
            print(f"[square] {n}^2 = {result}")
            return result

        # .expand() materialises one mapped task instance per element of the
        # upstream list.  In the UI each instance shows up as
        # ``dynamic_fanout.square__1`` etc., with Airflow assigning a unique
        # map_index per run.
        numbers = produce_numbers()
        squared: list[int] = square.expand(n=numbers)

    # ---------------------------------------------------------------------
    # Final join across both flavours
    # ---------------------------------------------------------------------
    done = EmptyOperator(
        task_id="done",
        trigger_rule="all_done",
    )

    static_fanout_group >> done
    dynamic_fanout_group >> done
