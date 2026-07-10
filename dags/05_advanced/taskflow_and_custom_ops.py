"""taskflow_and_custom_ops.py - Four advanced Airflow patterns in one DAG.

This DAG collects four patterns that show off Airflow's higher-level
authoring APIs and the seam between them.  Each pattern lives in its own
:class:`~airflow.utils.task_group.TaskGroup` so it can be read (and run)
in isolation, and the four groups fan in to a single ``advanced_done``
sentinel at the end of the DAG.

The four sections:

A. ``taskflow_only`` - Pure TaskFlow
    Three ``@task`` functions chain end-to-end with no traditional
    operators involved.  Upstream return values are auto-XComed and
    bound to downstream arguments by Airflow at runtime.  Teaches:
    * How ``@task`` removes the boilerplate of writing
      ``PythonOperator(python_callable=...)`` three times.
    * The auto-XCom / automatic-argument-binding contract: every
      return value lands under ``return_value`` and is pulled
      implicitly by the downstream task whose parameter name matches.

B. ``hybrid`` - Mixed TaskFlow + traditional Operator
    A ``@task`` produces a string, a ``BashOperator`` echoes it via
    Jinja, then another ``@task`` consumes the bash exit-line text
    back through XCom.  Teaches:
    * How TaskFlow tasks interoperate with traditional operators -
      ``@task`` *outputs* can be consumed by operators (via Jinja
      ``ti.xcom_pull``), and operator *outputs* can be consumed by
      ``@task`` *inputs* (by passing the operator instance to the
      ``@task`` call, which sets upstream and binds XCom).
    * That the hybrid is the intended workflow in real systems -
      legacy operators and modern TaskFlow tasks share the same
      XCom substrate.

C. ``greet`` - Custom operator
    Uses the project-local ``GreetOperator`` (defined under
    ``plugins/operators/greet_operator.py``).  Teaches:
    * Where custom operators live in an Airflow project (the
      ``plugins/`` folder is on Airflow's import path by default).
    * That a custom operator is just a ``BaseOperator`` subclass -
      nothing magical about it.

D. ``with_params`` - TaskFlow + ``params`` rendering
    An ``@task`` that accepts a string template, paired with a
    ``BashOperator`` whose ``bash_command`` is rendered through the
    task's ``params={...}`` dictionary via Jinja.  Teaches:
    * That operators (and ``@task`` callables) accept a ``params``
      dict whose entries are addressable as ``{{ params.<key> }}``
      inside any templated field (``bash_command``,
      ``env``, ``command``, etc.).

A single ``EmptyOperator(advanced_done)`` joins the four groups so the
DAG finishes with a clean topological tail.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Custom operator lives under the project-level ``plugins/`` tree, which
# Airflow adds to ``sys.path`` automatically at scheduler start.
from plugins.operators.greet_operator import GreetOperator


# ---------------------------------------------------------------------------
# default_args - applied to every task in this DAG
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
    dag_id="taskflow_and_custom_ops",
    description=(
        "Four advanced authoring patterns: pure TaskFlow, TaskFlow + "
        "Operator hybrid, a custom operator, and TaskFlow with params."
    ),
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["learning", "taskflow", "advanced"],
) as dag:

    start = EmptyOperator(task_id="start")

    # =======================================================================
    # A) taskflow_only - Pure TaskFlow chain (no traditional operators)
    # =======================================================================
    # read() returns a dict, transform() consumes it, sum_them() consumes
    # the resulting list, and print_summary() consumes the integer sum.
    # Every link in the chain is an @task callable; Airflow wires the
    # XCom "return_value" rows and the dependencies automatically.
    with TaskGroup(group_id="taskflow_only") as taskflow_only:

        @task
        def read() -> dict[str, Any]:
            """Producer: returns a small dict describing the payload.

            Note: this dict is tiny (~30 bytes) - well within the
            ``~256 byte`` XCom guideline.  Anything bigger should
            land in S3/GCS/DB and XCom a *reference* instead.
            """
            return {"name": "alice", "n": 7}

        @task
        def transform(d: dict[str, Any]) -> list[int]:
            """Step 2: pull ``n`` out of the dict, return ``[0, 2, ..., 2*(n-1)]``.

            The argument ``d`` is bound by Airflow from the upstream
            ``read()`` task's ``return_value`` XCom row - no
            ``ti.xcom_pull(...)`` call is needed on this side.
            """
            n: int = d["n"]
            values: list[int] = [i * 2 for i in range(n)]
            print(f"[A] transform: n={n} -> values={values!r}")
            return values

        @task
        def sum_them(values: list[int]) -> int:
            """Step 3: sum the integer list and emit the total."""
            total: int = sum(values)
            print(f"[A] sum_them: values={values!r} -> total={total}")
            return total

        @task
        def print_summary(n: int) -> None:
            """Final step of section A: log the produced sum.

            Takes the integer from ``sum_them()`` and prints it as the
            closing line of the TaskFlow-only pipeline.
            """
            print(f"[A] print_summary: TaskFlow pipeline produced sum={n}")

        # TaskFlow wiring: pass each upstream output as the matching
        # named argument of the downstream @task.  Airflow translates
        # these calls into (a) XCom dependency edges and (b) implicit
        # ``ti.xcom_pull(task_ids=..., key='return_value')`` bindings.
        data = read()
        transformed = transform(d=data)
        total = sum_them(values=transformed)
        print_summary(n=total)

    # =======================================================================
    # B) hybrid - TaskFlow producer + BashOperator + TaskFlow consumer
    # =======================================================================
    # Demonstrates that @task functions and traditional operators share
    # the same XCom substrate: the @task output is read by the bash
    # command via Jinja ``ti.xcom_pull(...)``, and the bash operator's
    # stdout line is in turn consumed by the downstream @task.
    with TaskGroup(group_id="hybrid") as hybrid:

        @task
        def make_message() -> str:
            """Producer: a short string the bash step will echo back."""
            message: str = "hello from hybrid taskflow"
            print(f"[B] make_message: produced {message!r}")
            return message

        bash_echo = BashOperator(
            task_id="echo_message",
            # Jinja-rendered at task start.  The task_id is the FULL
            # one, i.e. ``<group_id>.<task_id>`` - Airflow resolves
            # the XCom row at runtime.
            bash_command=(
                'echo "[B] bash received: '
                '{{ ti.xcom_pull(task_ids=\'hybrid.make_message\') }}"'
            ),
        )

        @task
        def consume_bash_output(received: str) -> str:
            """Consumer: read the bash operator's stdout line via XCom.

            Passing the ``BashOperator`` instance to this @task call
            (see the wiring below) causes Airflow to (a) make
            ``bash_echo`` an upstream of this task and (b) bind its
            ``return_value`` XCom row to the ``received`` parameter.
            """
            print(f"[B] consume_bash_output: received={received!r}")
            upper: str = received.upper()
            print(f"[B] consume_bash_output: emitted={upper!r}")
            return upper

        # Wire the three together.  ``msg`` is the @task output that
        # the bash command pulls via Jinja; ``consume_bash_output``
        # takes the bash operator instance itself, which Airflow
        # treats as both an upstream-edge and an XCom source.
        msg = make_message()
        bash_echo.set_upstream(msg)
        consume_bash_output(received=bash_echo)

    # =======================================================================
    # C) greet - Custom operator from the project's ``plugins/`` tree
    # =======================================================================
    # Uses GreetOperator from plugins/operators/greet_operator.py (created
    # by sibling task).  Custom operators are just ``BaseOperator``
    # subclasses; the ``plugins/`` folder is on Airflow's import path by
    # default, so importing them is the same as importing any other
    # Python module.
    with TaskGroup(group_id="greet") as greet:

        greet_team = GreetOperator(
            task_id="greet_team",
            name="data-platform",
            loud=True,
        )

        greet_alice = GreetOperator(
            task_id="greet_alice",
            name="alice",
            loud=False,
        )

        greet_bob = GreetOperator(
            task_id="greet_bob",
            name="bob",
            loud=False,
        )

        # Sequential chain: team greeting first, then the two
        # individual greetings can run in parallel.
        greet_team >> [greet_alice, greet_bob]

    # =======================================================================
    # D) with_params - TaskFlow + ``params`` rendering
    # =======================================================================
    # Two demonstrations of how to get templated values into a task:
    #   * The ``@task`` ``render`` shows the TaskFlow side - arguments
    #     can be templated strings and they are passed straight through
    #     to the callable.
    #   * The ``BashOperator`` shows the operator side - a ``params``
    #     dict lets any templated field (here ``bash_command``)
    #     reference ``{{ params.<key> }}`` in Jinja at task-start.
    with TaskGroup(group_id="with_params") as with_params:

        @task
        def render(template: str) -> str:
            """TaskFlow side: take a string template, log it, return it.

            This is intentionally a pass-through - the lesson here is
            *plumbing*, not transformation.  A real version would
            call Jinja / Mustache / ``string.Template`` on ``template``.
            """
            print(f"[D] render: received template={template!r}")
            return template

        # Render a fixed template through TaskFlow arg-binding.
        render(template="hello airflow via taskflow")

        # Operator side: ``params`` is rendered into ``bash_command``
        # via Jinja at task-start.  No XCom is needed - the value
        # lives in the operator's own ``params`` dict.
        bash_with_params = BashOperator(
            task_id="bash_with_params",
            bash_command="echo '{{ params.greeting }}'",
            params={"greeting": "hello airflow"},
        )

    # ---------------------------------------------------------------------------
    # DAG-level tail: fan the four groups into one EmptyOperator so the
    # DAG has a clean topological end.
    # ---------------------------------------------------------------------------
    advanced_done = EmptyOperator(task_id="advanced_done")

    start >> [taskflow_only, hybrid, greet, with_params] >> advanced_done