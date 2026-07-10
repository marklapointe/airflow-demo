"""xcom_demo.py — Five canonical XCom (Cross-Communication) patterns.

XCom is Airflow's mechanism for tasks to exchange small control values
across task boundaries within a DAG run.  Each XCom row is a
(key, value, dag_id, task_id, run_id, map_index) tuple persisted in the
metadata database.  It is the right tool for IDs, flags, dates, short
summaries, and small JSON-serialisable dicts/lists.

It is NOT a data pipeline.  Large blobs (dataframes, files, images)
should land in an external store (S3, GCS, a DB) and only the *reference*
should pass through XCom.  Rule of thumb: anything above ~256 bytes
probably belongs elsewhere — see inline comments where applicable.

This DAG demonstrates the five canonical XCom shapes A–E, grouped under
``xcom_patterns``:

    A. PUSH via return value       (PythonOperator; auto-key 'return_value')
    B. PUSH via ti.xcom_push(key)  (explicit key, multiple values per task)
    C. PUSH via TaskFlow (@task)   (auto-XCom + automatic arg binding)
    D. PULL via Jinja template     (BashOperator renders xcom_pull at runtime)
    E. PULL from MULTIPLE upstream (ti.xcom_pull(task_ids=[...]) + aggregate)

The final task prints one line reporting how many upstream tasks
contributed, the integer sum produced by pattern C, and the count of
distinct XCom patterns on display.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# ---------------------------------------------------------------------------
# default_args — applied to every task in this DAG
# ---------------------------------------------------------------------------
default_args: dict[str, Any] = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="xcom_demo",
    description="Five canonical XCom patterns (A–E) collected in one TaskGroup.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["learning", "xcom", "basics"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ---------------------------------------------------------------------
    # xcom_patterns — the five XCom shapes
    # ---------------------------------------------------------------------
    with TaskGroup(group_id="xcom_patterns") as xcom_patterns:

        # --- A) push-via-return -------------------------------------------
        # Returning a value from a PythonOperator callable causes Airflow
        # to auto-call ti.xcom_push(key="return_value", value=...); no
        # explicit push is required.  Downstream tasks pull implicitly.
        def push_via_return() -> dict[str, str]:
            """Producer for pattern A.  Returns a small dict.

            NOTE: this dict is ~60 bytes — well under the ~256 byte
            guideline.  Anything bigger should land in S3/GCS/DB and
            XCom a reference (e.g. ``"s3://bucket/key"``) instead.
            """
            return {
                "ts": "2024-01-01T00:00:00+00:00",
                "user": "alice",
            }

        def consume_via_return(ti: Any) -> dict[str, str]:
            """Consumer for pattern A.  Pulls via implicit 'return_value'.

            We re-emit the value so the final aggregate task (pattern E)
            can pick it up alongside two other upstreams.
            """
            payload: dict[str, str] = ti.xcom_pull(
                task_ids="xcom_patterns.push_via_return",
            )
            print(f"[A] pulled via implicit key='return_value': {payload!r}")
            return payload

        a_push = PythonOperator(
            task_id="push_via_return",
            python_callable=push_via_return,
        )
        a_pull = PythonOperator(
            task_id="consume_via_return",
            python_callable=consume_via_return,
        )
        a_push >> a_pull

        # --- B) push-via-ti.xcom_push (explicit key) ---------------------
        # Useful when one task publishes MULTIPLE values under distinct
        # keys.  The pull side must specify ``key=`` (or pass ``key=None``
        # to receive every row from that task as a list).
        def push_with_explicit_key(ti: Any) -> None:
            """Producer for pattern B.  Publishes two XCom rows."""
            ti.xcom_push(key="summary", value="hello-from-pattern-B")
            ti.xcom_push(key="length", value=len("hello-from-pattern-B"))

        def consume_with_explicit_key(ti: Any) -> str:
            """Consumer for pattern B.  Pulls the 'summary' key.

            Re-emits the string under ``return_value`` so pattern E's
            multi-pull can reach it as well.
            """
            msg: str = ti.xcom_pull(
                task_ids="xcom_patterns.push_with_explicit_key",
                key="summary",
            )
            print(f"[B] pulled via explicit key='summary': {msg!r}")
            return msg

        b_push = PythonOperator(
            task_id="push_with_explicit_key",
            python_callable=push_with_explicit_key,
        )
        b_pull = PythonOperator(
            task_id="consume_explicit_key",
            python_callable=consume_with_explicit_key,
        )
        b_push >> b_pull

        # --- C) TaskFlow (@task) ------------------------------------------
        # Two @task functions chain automatically: the upstream's return
        # value is written to XCom under ``return_value`` and bound to the
        # downstream's argument by Airflow at runtime.  No xcom_pull
        # is required on either side.
        @task
        def taskflow_sum_input() -> list[int]:
            """Pattern C, step 1: produce the integer list."""
            return [1, 2, 3, 4, 5]

        @task
        def taskflow_sum(numbers: list[int]) -> int:
            """Pattern C, step 2: consume the list and sum it."""
            total = sum(numbers)
            print(f"[C] TaskFlow upstream produced {numbers!r}, sum={total}")
            return total

        c_input = taskflow_sum_input()
        c_sum = taskflow_sum(c_input)  # Airflow wires XCom between them.

        # --- D) BashOperator + Jinja template ----------------------------
        # ``bash_command`` is Jinja-rendered at task-start time, so
        # ``{{ ti.xcom_pull(...) }}`` is substituted into the shell
        # command before execution.  Important: xcom_pull in a template
        # needs the FULL task_id, i.e. ``<group_id>.<task_id>``.
        d_bash = BashOperator(
            task_id="bash_render_xcom",
            bash_command=(
                'echo "[D] rendered via Jinja from TaskFlow input: "'
                ' "{{ ti.xcom_pull('
                "task_ids='xcom_patterns.taskflow_sum_input', "
                "key='return_value') }}\""
            ),
        )

        # --- E) Pull from MULTIPLE upstream tasks + final summary --------
        # ``ti.xcom_pull(task_ids=[...])`` returns one entry per upstream
        # (default key ``return_value``).  This task plays double duty:
        # it demonstrates the multi-pull pattern AND emits the final
        # DAG-level one-line summary with the requested metrics.
        def aggregate_and_summarize(ti: Any) -> None:
            """Pattern E + final summary line.

            Reads three upstreams at once via ``task_ids=[...]`` and
            prints one log line holding every required metric.
            """
            values: list[Any] = ti.xcom_pull(
                task_ids=[
                    "xcom_patterns.consume_via_return",    # dict (from A)
                    "xcom_patterns.consume_explicit_key",  # str  (from B)
                    "xcom_patterns.taskflow_sum",          # int  (from C)
                ],
            )

            n_upstreams: int = len(values)
            int_sum: int = next(v for v in values if isinstance(v, int))
            n_methods: int = 5  # the patterns A, B, C, D, E

            print(
                f"[E/FINAL] upstreams_contributed={n_upstreams} "
                f"sum_of_integers={int_sum} "
                f"methods_demonstrated={n_methods}"
            )

        e_summary = PythonOperator(
            task_id="aggregate_and_summarize",
            python_callable=aggregate_and_summarize,
        )

        # ----- wire dependencies *inside* the group ---------------------
        # The chain runs A → B → C → D → E with each downstream gated on
        # the upstream's success.  C-step1 → C-step2 is wired implicitly
        # by the TaskFlow API (passing c_input into taskflow_sum).
        a_pull >> b_push
        b_pull >> c_input
        c_sum >> d_bash
        d_bash >> e_summary

    # ---------------------------------------------------------------------
    # DAG-level edges: start → group → end
    # ---------------------------------------------------------------------
    start >> xcom_patterns >> end
