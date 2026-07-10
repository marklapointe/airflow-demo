"""Sensor patterns in Apache Airflow 3.0 — poke, reschedule, and deferrable.

DESIGN NOTE
-----------
Airflow gives you three ways to make a sensor "wait" for an external condition.
They differ in *what they do to the worker slot*:

1. **``mode='poke'`` (the default for `FileSensor`).**
   The sensor task occupies a worker slot for the entire wait. Every
   ``poke_interval`` seconds the worker wakes up, runs the poke check, and
   sleeps again. The slot is held the whole time — a sensor that waits an
   hour holds a worker for an hour. Fine for sub-minute waits, wasteful for
   long ones.

2. **``mode='reschedule'`.**
   When the poke check fires and the condition isn't met yet, the task
   *reschedules itself* for ``poke_interval`` seconds later and **releases
   the worker back to the pool**. The scheduler re-queues the sensor later.
   No worker slot is held between pokes. This is what you want for sensors
   that wait minutes or hours on local resources (files, S3 keys, dataset
   updates).

3. **Deferrable sensors (``*Async`` variants).**
   The sensor hands the wait off to a *triggerer* — a separate process
   designed specifically for sleeping on asynchronous conditions. Neither a
   worker nor a triggerer is tied up polling; the triggerer uses asyncio to
   park thousands of in-flight waits in a single process. **This is the
   recommended pattern in Airflow 3.0** for any sensor that has an async
   equivalent (time, time-delta, file, HTTP, etc.).

The DAG below wires up all three patterns inside four `TaskGroup`s so a
reader can compare them side-by-side. No real network calls — every sensor
here waits on either a local file, a clock, or a weekday, so the DAG is
safe to run on a laptop.

Airflow 3.0 import notes (verified against apache-airflow 3.0):
- ``TimeSensorAsync`` & ``TimeDeltaSensorAsync`` come from
  ``airflow.providers.standard.sensors.time`` / ``.time_delta``. The
  legacy ``airflow.sensors.time_sensor`` path is deprecated in 3.0.
- ``DayOfWeekSensor`` comes from
  ``airflow.providers.standard.sensors.weekday``. It accepts a single
  ``WeekDay`` enum value, so this DAG picks ``MONDAY`` to demonstrate
  the pattern (any single weekday suffices — "fire on weekdays" is the
  canonical use, not a single class feature).
- ``TriggerDagRunOperator`` comes from
  ``airflow.providers.standard.operators.trigger_dagrun``.
"""
from __future__ import annotations

from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.file import FileSensor
from airflow.providers.standard.sensors.time import TimeSensorAsync
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync
from airflow.providers.standard.sensors.weekday import DayOfWeekSensor, WeekDay
from airflow.utils.task_group import TaskGroup


# --- paths -----------------------------------------------------------------

# Resolve relative to the project root so the DAG works regardless of the
# working directory Airflow was started from. The seed BashOperator below
# uses the same path so the FileSensors always have something to find.
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
SEED_DIR = _PROJECT_ROOT / "data" / "sample" / "incoming"
SEED_FILE = SEED_DIR / "processed.csv"


# --- task id constants -----------------------------------------------------
#
# Mirrors the style used elsewhere in this repo: name each operator so the
# Airflow UI groups sensors by intent ("time", "file", "weekday"), and
# expose qualified ids only if downstream tasks need them.

TASK_START = "start"
TASK_SEED = "seed_processed_csv"
TASK_END = "everything_done"

GROUP_INTERVAL = "interval"
TASK_WAIT_2PM = "wait_until_2pm"
TASK_WAIT_5S = "wait_5_seconds"

GROUP_FILESYSTEM = "filesystem"
TASK_SEED_BEFORE = "seed_file"
TASK_POKE_FILE = "wait_for_file_poke"
TASK_RESCHEDULE_FILE = "wait_for_file_reschedule"

GROUP_SMART = "smart"
TASK_WEEKDAY_ONLY = "fires_on_weekday"

GROUP_TRIGGER = "trigger_dag"
TRIGGER_DAG_ID = "airflow_features_demo"
TASK_TRIGGER_KITCHEN_SINK = f"trigger_{TRIGGER_DAG_ID}"


# --- DAG defaults ----------------------------------------------------------

# 600 s = 10 min timeout on every sensor: if a poke never succeeds, the
# sensor fails the task rather than blocking the DAG indefinitely.
SENSOR_TIMEOUT = 600

default_args: dict[str, Any] = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
}


# --- DAG -------------------------------------------------------------------

with DAG(
    dag_id="sensor_patterns",
    description="Compare poke / reschedule / deferrable sensor patterns in Airflow 3.0.",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["04_resilience", "sensors", "learning"],
) as dag:
    # Fan-in point: every group runs after `start`. The interval and smart
    # groups have no inter-group ordering dependency, so we just chain them
    # off `start` and let them run in parallel under the scheduler.
    start = EmptyOperator(task_id=TASK_START)

    # The filesystem group needs the target file to exist *before* its
    # sensors start running. We seed it with a tiny BashOperator upstream
    # of (and inside) the group so the group is self-contained.
    #
    # NOTE: this is a learning DAG — in real life, "the file appears" is
    # the producer's job and the sensor is the *only* thing that should
    # be wired into the DAG. We're seeding it here so the DAG is runnable
    # end-to-end on a fresh checkout.
    seed_processed_csv = BashOperator(
        task_id=TASK_SEED,
        bash_command=(
            f"mkdir -p {SEED_DIR.as_posix()} && touch {SEED_FILE.as_posix()}"
        ),
    )

    # GROUP 1 — interval: deferrable (`*Async`) time sensors.
    #
    # These do NOT accept `poke_interval` because they don't poll — they
    # hand the wait off to the triggerer. `timeout` is the only knob that
    # matters for fail-fast behaviour.
    with TaskGroup(group_id=GROUP_INTERVAL) as interval_group:
        wait_until_2pm = TimeSensorAsync(
            task_id=TASK_WAIT_2PM,
            target_time=time(14, 0, 0),
            timeout=SENSOR_TIMEOUT,
        )
        wait_5_seconds = TimeDeltaSensorAsync(
            task_id=TASK_WAIT_5S,
            delta=timedelta(seconds=5),
            timeout=SENSOR_TIMEOUT,
        )

    # GROUP 2 — filesystem: poke vs. reschedule on the same FileSensor.
    #
    # Both sensors look for the same path the seed task created. The only
    # difference is `mode`: the first holds a worker, the second doesn't.
    with TaskGroup(group_id=GROUP_FILESYSTEM) as filesystem_group:
        seed_file = BashOperator(
            task_id=TASK_SEED_BEFORE,
            bash_command=(
                f"mkdir -p {SEED_DIR.as_posix()} && touch {SEED_FILE.as_posix()}"
            ),
        )
        wait_for_file_poke = FileSensor(
            task_id=TASK_POKE_FILE,
            filepath=str(SEED_FILE.as_posix()),
            mode="poke",
            poke_interval=30,
            timeout=SENSOR_TIMEOUT,
        )
        wait_for_file_reschedule = FileSensor(
            task_id=TASK_RESCHEDULE_FILE,
            filepath=str(SEED_FILE.as_posix()),
            mode="reschedule",
            poke_interval=60,
            timeout=SENSOR_TIMEOUT,
        )
        seed_file >> [wait_for_file_poke, wait_for_file_reschedule]

    # GROUP 3 — smart: time-based branching with a weekday sensor.
    #
    # `DayOfWeekSensor` checks whether the execution date (or today, with
    # `use_task_execution_day=False`) is the requested weekday. We pin to
    # MONDAY so the demo fires predictably on at least one weekday per week;
    # in production you'd wire the four sensors (one per weekday) into a
    # branching task or set up multiple DAG runs. A real "weekday-only"
    # scheduler is just `schedule="0 9 * * 1-5"` on the DAG itself, but
    # the sensor is the right tool when the *task* (not the DAG schedule)
    # needs to be day-aware.
    with TaskGroup(group_id=GROUP_SMART) as smart_group:
        fires_on_weekday = DayOfWeekSensor(
            task_id=TASK_WEEKDAY_ONLY,
            week_day=WeekDay.MONDAY,
            use_task_execution_day=True,
            poke_interval=30,
            timeout=SENSOR_TIMEOUT,
        )

    # GROUP 4 — trigger_dag: kick off the kitchen-sink DAG once everything
    # else succeeds. `TriggerDagRunOperator` is the canonical cross-DAG
    # primitive in Airflow 3.0 (alongside Datasets).
    with TaskGroup(group_id=GROUP_TRIGGER) as trigger_group:
        trigger_kitchen_sink = TriggerDagRunOperator(
            task_id=TASK_TRIGGER_KITCHEN_SINK,
            trigger_dag_id=TRIGGER_DAG_ID,
            wait_for_completion=False,
            reset_dag_run=True,
        )

    # Fan-out + fan-in wiring:
    #
    #   start ─┬─▶ interval_group     ─┐
    #          ├─▶ filesystem_group   ─┤
    #          └─▶ smart_group        ─┼─▶ trigger_group ─▶ everything_done
    #
    # `seed_processed_csv` is upstream of `filesystem_group` so its sensor
    # always finds the file. `trigger_group` runs only after every group
    # above succeeds.
    end = EmptyOperator(task_id=TASK_END)

    start >> seed_processed_csv >> filesystem_group
    start >> interval_group
    start >> smart_group
    [interval_group, filesystem_group, smart_group] >> trigger_group >> end
