"""Branching & joins in Airflow 3.0.

This DAG demonstrates three distinct control-flow patterns, each isolated inside
its own :class:`airflow.utils.task_group.TaskGroup` so the patterns can be read
and executed independently:

Group 1 - ``branches`` (``branch_logic``)
    A :class:`BranchPythonOperator` selects one of two downstream paths based on
    a random value, then the two paths are re-joined with an ``EmptyOperator``
    that uses ``trigger_rule='one_success'``.

Group 2 - ``parallel_join`` (``parallel_join``)
    A fan-out / fan-in shape that does *not* use ``BranchPythonOperator``: one
    producer fans out to five workers, which converge into a ``join`` task
    (``all_success``) and a parallel ``cross_join`` task (``one_success``) that
    would still fire even if some upstream branches had been skipped.

Group 3 - ``conditional`` (``short_circuit``)
    A :class:`ShortCircuitOperator` gates a downstream ``BashOperator`` so that
    the bash step only runs when the short-circuit returns a truthy value; an
    ``EmptyOperator`` with ``trigger_rule='all_done'`` demonstrates that the
    downstream graph remains traversable in both the "publish" and "skip"
    outcomes.

Airflow 3.0 notes
-----------------
* ``schedule=`` keyword is used (not the deprecated ``schedule_interval=``).
* All operators are imported from ``airflow.providers.standard.*`` (the
  Airflow 2 ``airflow.operators.*`` namespace is deprecated).
* ``ShortCircuitOperator`` lives in
  ``airflow.providers.standard.operators.python`` (verified on Airflow 3.0.6).
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.utils.task_group import TaskGroup


# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------

default_args: dict[str, Any] = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Group 1 - branch_logic
# ---------------------------------------------------------------------------
#
# Flow:
#
#     decide_branch  ──► large_branch  ──► merge
#                  └──► small_branch ──►
#
# ``merge`` uses ``trigger_rule='one_success'`` so it fires as soon as the
# chosen branch succeeds; the skipped branch is ignored by the join.

#: Identifier returned by :func:`_decide_branch` when the random value > 50.
LARGE_PATH_ID: str = "large_branch"
#: Identifier returned by :func:`_decide_branch` when the random value <= 50.
SMALL_PATH_ID: str = "small_branch"
#: Threshold used by :func:`_decide_branch` to pick between the two paths.
BRANCH_THRESHOLD: int = 50


def _push_random_value(**kwargs: Any) -> int:
    """Generate a random integer in [1, 100] and push it via XCom."""
    value: int = random.randint(1, 100)
    print(f"[branches] generated value = {value}")
    return value


def _decide_branch(**kwargs: Any) -> str:
    """Return the task_id of the branch to follow based on the pushed value.

    ``BranchPythonOperator`` consumes the returned string as the downstream
    task_id(s) to execute; every other downstream task is set to ``skipped``.
    """
    ti = kwargs["ti"]
    value: int = ti.xcom_pull(task_ids="branches.decide_source")
    if value > BRANCH_THRESHOLD:
        print(f"[branches] {value} > {BRANCH_THRESHOLD} -> {LARGE_PATH_ID}")
        return LARGE_PATH_ID
    print(f"[branches] {value} <= {BRANCH_THRESHOLD} -> {SMALL_PATH_ID}")
    return SMALL_PATH_ID


def _large_path(**kwargs: Any) -> None:
    """Print the large-value path message and echo the XCom value."""
    ti = kwargs["ti"]
    value: int = ti.xcom_pull(task_ids="branches.decide_source")
    print(f"Large value: {value}")


def _small_path(**kwargs: Any) -> None:
    """Print the small-value path message and echo the XCom value."""
    ti = kwargs["ti"]
    value: int = ti.xcom_pull(task_ids="branches.decide_source")
    print(f"Small value: {value}")


# ---------------------------------------------------------------------------
# Group 2 - parallel_join
# ---------------------------------------------------------------------------
#
# Flow:
#
#     fan_out ──► work_a ─┐
#           └──► work_b ─┤
#           └──► work_c ─┼──► join         (all_success)
#           └──► work_d ─┤
#           └──► work_e ─┴──► cross_join   (one_success)
#
# ``join`` only fires if every worker succeeded; ``cross_join`` would still
# fire even if some workers had been skipped, illustrating two different
# fan-in trigger rules on the same fan-out.

#: IDs of the five fan-out workers. Defined as a module-level constant so the
#: ``_join`` callable and the task wiring share one source of truth.
WORKER_IDS: list[str] = ["work_a", "work_b", "work_c", "work_d", "work_e"]
#: Payload pushed by :func:`_fan_out`; mirrors :data:`WORKER_IDS` semantically.
WORKER_PAYLOAD: list[str] = [
    "file_alpha.csv",
    "file_beta.csv",
    "file_gamma.csv",
    "file_delta.csv",
    "file_epsilon.csv",
]


def _fan_out(**kwargs: Any) -> list[str]:
    """Return the list of worker identifiers/payloads to be processed."""
    print(f"[parallel_join] fanning out to {len(WORKER_PAYLOAD)} workers")
    return list(WORKER_PAYLOAD)


def _make_worker(label: str):
    """Build a Python callable bound to a specific worker label.

    Using a tiny factory keeps ``work_a`` ... ``work_e`` distinct without
    duplicating five near-identical top-level functions.
    """

    def _worker(**kwargs: Any) -> None:
        ti = kwargs["ti"]
        payload = ti.xcom_pull(task_ids="parallel_join.fan_out")
        print(f"worker {label} processing (payload={payload!r})")

    _worker.__name__ = f"_worker_{label}"
    return _worker


def _join(**kwargs: Any) -> None:
    """Pull from every worker via XCom and aggregate the results."""
    ti = kwargs["ti"]
    aggregated = ti.xcom_pull(task_ids=WORKER_IDS)
    non_null = [item for item in aggregated if item is not None]
    print(f"[parallel_join] join collected {len(non_null)} items: {aggregated!r}")


def _cross_join(**kwargs: Any) -> None:
    """Demonstrate ``trigger_rule='one_success'``: fires even if peers are skipped."""
    ti = kwargs["ti"]
    aggregated = ti.xcom_pull(task_ids=WORKER_IDS)
    print(f"[parallel_join] cross_join (one_success) saw: {aggregated!r}")


# ---------------------------------------------------------------------------
# Group 3 - short_circuit
# ---------------------------------------------------------------------------
#
# Flow:
#
#     produce ──► should_publish ──► publish
#                              └──► skip_marker
#
# ``should_publish`` is a :class:`ShortCircuitOperator`. When its callable
# returns a falsy value every direct downstream is set to ``skipped``; the
# ``skip_marker`` task with ``trigger_rule='all_done'`` still runs so the
# downstream graph remains traversable in both outcomes.

#: XCom key used to source the value consumed by :func:`_should_publish`.
PUBLISH_SOURCE_TASK_ID: str = "conditional.produce"
#: String returned by :func:`_produce` when "publishing" should be allowed.
PUBLISH_PAYLOAD: str = "article-2026-07-09"


def _produce(**kwargs: Any) -> str:
    """Push a non-empty payload so :func:`_should_publish` evaluates to True."""
    print(f"[conditional] producing payload: {PUBLISH_PAYLOAD!r}")
    return PUBLISH_PAYLOAD


def _should_publish(**kwargs: Any) -> bool:
    """Return True iff an XCom-pulled value is non-empty.

    :class:`ShortCircuitOperator` will skip its downstream tasks whenever the
    returned value is falsy.
    """
    ti = kwargs["ti"]
    value = ti.xcom_pull(task_ids=PUBLISH_SOURCE_TASK_ID)
    is_truthy = bool(value)
    print(f"[conditional] should_publish={is_truthy} (value={value!r})")
    return is_truthy


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


with DAG(
    dag_id="branching_and_joins",
    description="Three branching / join patterns demonstrated in isolated TaskGroups.",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["learning", "branching", "joins", "taskgroup"],
) as dag:
    # ------------------------------------------------------------------
    # Group 1: branches (BranchPythonOperator + EmptyOperator merge)
    # ------------------------------------------------------------------
    with TaskGroup(group_id="branches") as branches_group:
        decide_source = PythonOperator(
            task_id="decide_source",
            python_callable=_push_random_value,
        )
        decide_branch = BranchPythonOperator(
            task_id="decide_branch",
            python_callable=_decide_branch,
        )
        large_branch = PythonOperator(
            task_id="large_branch",
            python_callable=_large_path,
        )
        small_branch = PythonOperator(
            task_id="small_branch",
            python_callable=_small_path,
        )
        merge = EmptyOperator(
            task_id="merge",
            trigger_rule="one_success",
        )

        decide_source >> decide_branch >> [large_branch, small_branch] >> merge

    # ------------------------------------------------------------------
    # Group 2: parallel_join (fan-out / fan-in without BranchPythonOperator)
    # ------------------------------------------------------------------
    with TaskGroup(group_id="parallel_join") as parallel_group:
        fan_out = PythonOperator(
            task_id="fan_out",
            python_callable=_fan_out,
        )
        workers: list[PythonOperator] = [
            PythonOperator(
                task_id=worker_id,
                python_callable=_make_worker(worker_id),
            )
            for worker_id in WORKER_IDS
        ]
        join = PythonOperator(
            task_id="join",
            python_callable=_join,
            trigger_rule="all_success",
        )
        cross_join = PythonOperator(
            task_id="cross_join",
            python_callable=_cross_join,
            trigger_rule="one_success",
        )

        fan_out >> workers
        workers >> join
        workers >> cross_join

    # ------------------------------------------------------------------
    # Group 3: conditional (ShortCircuitOperator + BashOperator)
    # ------------------------------------------------------------------
    with TaskGroup(group_id="conditional") as conditional_group:
        produce = PythonOperator(
            task_id="produce",
            python_callable=_produce,
        )
        should_publish = ShortCircuitOperator(
            task_id="should_publish",
            python_callable=_should_publish,
        )
        publish = BashOperator(
            task_id="publish",
            bash_command='echo "Publishing approved payload to downstream system"',
        )
        skip_marker = EmptyOperator(
            task_id="skip_marker",
            trigger_rule="all_done",
        )

        produce >> should_publish
        should_publish >> publish
        should_publish >> skip_marker