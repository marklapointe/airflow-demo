# Patterns Reference

A short reference of the patterns that the example DAGs demonstrate. Each
entry names the pattern, the canonical problem it solves, and the DAG file
where you can see it in the wild.

If you are new to Airflow, this file complements the README's "Reading
order" by giving you a vocabulary before you step into the DAGs.

---

## DAG-level patterns

### `Kitchen-sink DAG` — `dags/01_basics/airflow_demo.py`

The single-DAG-form demonstration. If you only read one DAG file, read
this one. Shows: TaskGroups, BranchPythonOperator, XCom, trigger rules,
dependency chaining with `>>`.

### `Composition over operators` — `dags/02_etl/csv_to_warehouse.py`

The DAG is short. The work happens in `include/`. The DAG is just wiring.

### `Producer → consumer across DAGs` — `dags/03_orchestration/cross_dag_datasets.py`

Uses `Dataset` objects to declare "I depend on rows from this other DAG
being available". Cleaner than `TriggerDagRunOperator` for data
dependencies because it doesn't force a whole DAG to re-run.

### `Generated DAGs` — `dags/03_orchestration/dynamic_dag_generation.py`

Loops over a list of inputs at *parse time* and emits one task per
element. Useful when you have 50 source systems and want one task per.

### `One-DAG-per-team, one-task-per-DAG` — see dags/05_advanced

When your DAGs get really long (50+ tasks), split them.  Reference DAGs
in this folder: `taskflow_and_custom_ops.py`.

## Task-level patterns

### `Push-by-return` (TaskFlow) vs `Push-by-xcom_push`

TaskFlow (`@task`) auto-pushes the return value of a Python callable.
That's the modern idiom; the `ti.xcom_push` form is shown for older DAGs
in `xcom_demo.py` group A.

### `Branch-then-join`

A `BranchPythonOperator` returns the next task's id; all others are
skipped. The join task uses `trigger_rule='one_success'`. See
`branching_and_joins.py` for the canonical example.

### `Fail-fast quality gate`

A `PythonOperator` raises on bad data; Airflow stops scheduling downstream
tasks. This is the right choice when "bad data" is unusual and you want
to be paged. See `dags/02_etl/data_quality.py`.

### `Long-running sensor → deferrable operator`

The classic `S3KeySensor` blocks a worker for hours. The deferrable
operator version (`S3KeySensorAsync` etc.) releases the worker back to
the pool and re-attaches when the resource is ready. Both shapes are
shown in `dags/04_resilience/sensor_patterns.py` for comparison.

### `Custom operator`

When you find yourself writing the same `PythonOperator(python_callable=foo)`
five times, write an operator. See `plugins/operators/greet_operator.py`
and `dags/05_advanced/taskflow_and_custom_ops.py`.

## Operational patterns

### `Idempotent re-runs`

A DAG run that triggers the same task twice must produce the same result.
Two mechanisms:

* `INSERT OR IGNORE` on a natural key (write side).
* `@task(retries=N, retry_delay=...)` plus the same `INSERT OR IGNORE`.

You cannot have one without the other.

### `Recovery from partial failure`

`airflow dags backfill --start-date ... --reset-dagruns ...` replays
historical runs with the new logic. Pair with `INSERT OR IGNORE` for
safety.

### `Idempotent + partitioned + sorted = trusted`

The three properties together give you a dataset you can rely on:

* *Idempotent* — re-running doesn't change the result.
* *Partitioned* — you can drop a single window's data.
* *Sorted by time* — you can stream the latest rows in O(1) memory.

We aim for these in `csv_to_warehouse.py`.

## Patterns we deliberately do NOT use

* `SubDagOperator` — retired. Always. Use `TaskGroup` to group, and
  use proper DAGs (`@dag` factories) when you need to share logic.
* `airflow.operators.*` (legacy) — uses `airflow.providers.standard.*`
  on Airflow 3.0.
* `schedule_interval` — uses `schedule` on Airflow 3.0.
* `PythonOperator` to call out to subprocesses — uses
  `BashOperator` / `SSHOperator` instead.
* XCom for *data* (only for control values) — large objects go to S3,
  GCS, or the warehouse.
