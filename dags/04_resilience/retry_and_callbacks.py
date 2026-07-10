"""Resilience patterns in Airflow 3.0.

This DAG demonstrates four independent resilience mechanisms, each isolated in
its own :class:`airflow.utils.task_group.TaskGroup` so the patterns can be
read and exercised independently:

Group 1 - ``retries`` (``flaky_task``)
    Exponential backoff with ``retry_exponential_backoff=True`` capped by
    ``max_retry_delay``. The task randomly raises so the retry machinery
    occasionally fires; the backoff curve is documented inline.

Group 2 - ``callbacks`` (``success_task``, ``failure_task``)
    Module-level ``on_failure_callback``, ``on_success_callback`` and
    ``on_retry_callback`` are wired onto :class:`PythonOperator` instances. A
    downstream :class:`EmptyOperator` with ``trigger_rule='all_done'`` keeps
    the DAG run from being blocked by the intentionally-failing task.

Group 3 - ``sla`` (``long_task``)
    A per-task ``sla=`` together with a DAG-level ``sla_miss_callback`` so
    missed SLAs are reported centrally. ``long_task`` sleeps long enough to
    actually breach its SLA in the demonstration.

Group 4 - ``sensible_defaults`` (``safe_task``)
    The DAG's ``default_args`` declares ``email``, ``email_on_failure`` and
    ``email_on_retry``. ``safe_task`` inherits them verbatim to show the
    "one source of truth" pattern.

Airflow 3.0 notes
-----------------
* ``schedule=`` keyword is used (not the deprecated ``schedule_interval=``).
* All operators are imported from ``airflow.providers.standard.*`` (the
  Airflow 2 ``airflow.operators.*`` namespace is deprecated in 3.0).
* ``sla_miss_callback`` is supplied via the DAG kwargs, not per-task.
* Every callback body is wrapped in ``try/except`` so an alerting bug can
  never fail the underlying task.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Module-level logger used by callbacks and task bodies so log lines are
# clearly attributable to this DAG.
STANDARD = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Callback functions (module-level so Airflow can pickle them by reference)
# ---------------------------------------------------------------------------
#
# Each callback wraps its body in ``try/except`` and *only* the body — the
# ``try`` is inside the function, never around the call site — so a crash in
# alerting logic can never fail the underlying task. If the callback itself
# raises, Airflow would otherwise mark the task as failed, which would
# silently mask the real outcome.

def on_failure_callback(context: dict[str, Any]) -> None:
    """Fire when a task fails after exhausting its retries.

    Logs the failure and emits a minimal "fake alert" line so the demo
    shows what a real PagerDuty / Slack integration would do.
    """
    try:
        dag_id: str = context["dag_id"]
        task_id: str = context["task_id"]
        execution_date: Any = context["execution_date"]

        STANDARD.error(
            "Task %s.%s failed on %s",
            dag_id,
            task_id,
            execution_date,
        )
        # Minimal "fake alert" — pretend we shipped this to PagerDuty / Slack.
        print(
            f"[ALERT] dag={dag_id} task={task_id} "
            f"execution_date={execution_date}"
        )
    except Exception:  # noqa: BLE001 — callback must never propagate
        STANDARD.exception("on_failure_callback itself raised")


def on_success_callback(context: dict[str, Any]) -> None:
    """Fire when a task succeeds."""
    try:
        STANDARD.info(
            "Task %s.%s succeeded",
            context["dag_id"],
            context["task_id"],
        )
    except Exception:  # noqa: BLE001
        STANDARD.exception("on_success_callback itself raised")


def on_retry_callback(context: dict[str, Any]) -> None:
    """Fire on every retry attempt before the task is re-executed.

    ``ti.try_number`` is 1-indexed and includes the first attempt, so an
    attempt value of 1 means "this is the first try, which just failed and
    is about to be retried as attempt 2".
    """
    try:
        ti = context.get("ti")
        attempt: int | None = ti.try_number if ti is not None else None

        STANDARD.warning(
            "Task %s.%s is being retried (attempt=%s)",
            context["dag_id"],
            context["task_id"],
            attempt,
        )
    except Exception:  # noqa: BLE001
        STANDARD.exception("on_retry_callback itself raised")


def sla_miss_callback(
    dag: DAG,
    task_list: list[Any],
    blocking_task_list: list[Any],
    slas: list[Any],
    blocking_tis: list[Any],
) -> None:
    """DAG-level callback fired when one or more tasks miss their SLA.

    Airflow invokes this once per missed SLA with all tasks that breached
    at the same time grouped together. The full list of arguments is
    printed so the demo shows what a real alerting payload looks like.
    """
    try:
        summary = {
            "dag": dag.dag_id,
            "missed_tasks": [t.task_id for t in (task_list or [])],
            "blocking_tasks": [t.task_id for t in (blocking_task_list or [])],
            "slas": [str(s) for s in (slas or [])],
            "blocking_tis": [ti.task_id for ti in (blocking_tis or [])],
        }
        STANDARD.warning("SLA miss: %s", summary)
        print(f"[SLA MISS] {summary}")
    except Exception:  # noqa: BLE001
        STANDARD.exception("sla_miss_callback itself raised")


# Local alias so the DAG kwarg reads naturally as ``sla_miss_callback=miss_callback``.
miss_callback = sla_miss_callback


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _flaky(**_: Any) -> None:
    """Raise about half the time so the retry machinery is exercised.

    Deterministic alternatives exist (e.g. raise on every odd attempt via
    ``ti.try_number``); ``random.random()`` is used here because it makes
    the DAG self-tuning across runs.
    """
    if random.random() < 0.5:  # noqa: S311 — random is fine for a demo
        raise RuntimeError("flaky_task: simulated transient failure")
    STANDARD.info("flaky_task: succeeded on this attempt")


def _always_succeed(**_: Any) -> None:
    """Used to demonstrate ``on_success_callback`` end-to-end."""
    STANDARD.info("success_task: hello, world")


def _always_fail(**_: Any) -> None:
    """Always raises — drives ``on_retry_callback`` then ``on_failure_callback``."""
    raise RuntimeError(
        "failure_task: intentional failure to exercise the failure callbacks"
    )


def _long_running(**_: Any) -> None:
    """Sleep long enough to actually breach the task's ``sla`` of 5 seconds.

    ``execution_timeout`` is 10 seconds, so an 8-second sleep completes
    successfully *and* violates the SLA — the demo we want.
    """
    time.sleep(8)
    STANDARD.info("long_task: finished sleeping, now succeeding")


def _safe_default(**_: Any) -> None:
    """Inherits the DAG's ``default_args`` (email, ``email_on_failure``, ...)."""
    STANDARD.info("safe_task: running with DAG-level default_args")


# ---------------------------------------------------------------------------
# Default arguments applied to every task in the DAG that does not override.
# ---------------------------------------------------------------------------

default_args: dict[str, Any] = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["ops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="retry_and_callbacks",
    description=(
        "Resilience patterns: exponential backoff, task-lifecycle callbacks, "
        "SLAs, and DAG-level default_args."
    ),
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    sla_miss_callback=miss_callback,
    tags=["learning", "resilience", "04_resilience"],
) as dag:

    # ---- GROUP 1: retries ------------------------------------------------
    # Backoff curve with ``retry_delay=2s``, ``max_retry_delay=60s``,
    # ``retries=4`` and ``retry_exponential_backoff=True``:
    #
    #     delay = min(retry_delay * 2 ** (attempt - 1), max_retry_delay)
    #
    #     attempt 1 -> 2 * 2**0 =   2s
    #     attempt 2 -> 2 * 2**1 =   4s
    #     attempt 3 -> 2 * 2**2 =   8s
    #     attempt 4 -> 2 * 2**3 =  16s   (well under the 60s ceiling)
    #
    # ``pool='small_pool'`` throttles concurrency so a real outage is not
    # amplified by a thundering herd of retries.
    with TaskGroup("retries") as retries_group:
        flaky_task = PythonOperator(
            task_id="flaky_task",
            python_callable=_flaky,
            retries=4,
            retry_delay=timedelta(seconds=2),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=1),
            pool="small_pool",
        )

    # ---- GROUP 2: callbacks ---------------------------------------------
    #
    # Flow:
    #
    #     success_task ─┐
    #                   ├──► downstream   (trigger_rule='all_done')
    #     failure_task ─┘
    #
    # ``trigger_rule='all_done'`` on ``downstream`` lets the DAG run finish
    # even when ``failure_task`` has failed — the failure is reported via
    # the callbacks, but the pipeline is "best effort" downstream.
    with TaskGroup("callbacks") as callbacks_group:
        success_task = PythonOperator(
            task_id="success_task",
            python_callable=_always_succeed,
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
            on_retry_callback=on_retry_callback,
        )
        failure_task = PythonOperator(
            task_id="failure_task",
            python_callable=_always_fail,
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
            on_retry_callback=on_retry_callback,
        )
        downstream = EmptyOperator(
            task_id="downstream",
            trigger_rule="all_done",
        )

        [success_task, failure_task] >> downstream

    # ---- GROUP 3: sla ----------------------------------------------------
    #
    # ``sla=timedelta(seconds=5)`` on the task plus an 8-second sleep means
    # the SLA is intentionally violated — ``sla_miss_callback`` fires after
    # the task instance completes, even though the task itself succeeds.
    with TaskGroup("sla") as sla_group:
        long_task = PythonOperator(
            task_id="long_task",
            python_callable=_long_running,
            execution_timeout=timedelta(seconds=10),
            sla=timedelta(seconds=5),
        )

    # ---- GROUP 4: sensible_defaults -------------------------------------
    # ``safe_task`` declares no overrides — it inherits ``email``,
    # ``email_on_failure``, ``email_on_retry``, ``owner``, etc. from the
    # DAG's ``default_args`` so every task in the project gets the same
    # safety net from a single source of truth.
    with TaskGroup("sensible_defaults") as defaults_group:
        safe_task = PythonOperator(
            task_id="safe_task",
            python_callable=_safe_default,
        )
