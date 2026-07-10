# `include/` is on sys.path automatically in Airflow 3.0 — anything here
# becomes importable from a DAG file as `from include.<subpackage>.<mod> import ...`.
#
# Subpackages:
#   domain/    — pure data records and value types (no I/O, no Airflow imports)
#   io/        — sources and sinks (CSV, SQLite) - thin wrappers, no business logic
#   transforms/ — pure functions that take records in and yield records out
#
# Why this split?  See docs/design_decisions.md §2 ("Hexagonal boundary").
# The DAG files should orchestrate; the heavy lifting lives here where it can
# be unit-tested in milliseconds without spinning up a scheduler.
