# Design Decisions

This file is the long-form record of the architectural commitments in this
repo. It mirrors the Honcho **airflow-architect** peer card; if you change
one, update both.

The decisions here are organised from generic ("how do we shape a code
folder?") to specific ("how do we avoid money-eating bugs in the price
column?"). Read top to bottom.

---

## 1. DAG folders are organized by *concern*, not by *person*

We group DAGs by what they're teaching, not by who wrote them. This lets a
new contributor answer "where does the example of using SLAs live?" by
walking a single folder tree instead of grepping.

Cost: a little bit of git-blame cross-pollination when the same author
touches multiple folders. Benefit: a beginner can scan a folder and read
five DAGs in a row without context-switching.

## 2. Hexagonal boundary between DAGs and shared code

DAG files contain only **orchestration**: which task runs in what order,
which trigger rule governs a join, which retries are configured. They do
*not* parse CSVs, compute revenue, or talk to databases directly.

All that happens in `include/`:

```
include/domain/        # pure data: records, value types
include/io/            # I/O adapters: CsvSource, SqliteSink
include/transforms/    # pure functions over records
```

A DAG file looks like:

```python
with DAG(...) as dag:
    extract = PythonOperator(python_callable=extract_orders)
    clean = PythonOperator(python_callable=lambda **kw: ...)
    load = PythonOperator(python_callable=load_to_warehouse)
    extract >> clean >> load
```

We never cross the boundary the other way — `include/` never imports from
`airflow.*`. The reason is **testability**: a scheduler, a webserver, and
a metadata DB are *not* required to validate the cleaning pipeline. The
unit test suite in `tests/unit/` runs in milliseconds.

## 3. Records are algebraic types, dataclass `frozen=True`

`include/domain/__init__.py` defines the records (e.g., `OrderRecord`,
`CleanedOrder`). They are:

* `frozen=True` — immutable. Two records with the same field values are
  `==` and hashable, so you can put them in sets or use as dict keys.
* `slots=True` — predictable memory layout, faster attribute access.
* `kw_only=True` — you cannot accidentally swap two same-typed positional
  args at a call site.

Newtypes (`CustomerId`, `ProductId`, `OrderId`) wrap the underlying `int`
so a `CustomerId` cannot be passed to a function expecting a `ProductId`.
This is essentially the "Parse, don't validate" principle: by the time a
value lands in your function, it has been narrowed to the right shape.

## 4. Idempotency at the sink

Every write in `SqliteSink.write` is `INSERT OR IGNORE` keyed on a
natural id (`order_id` etc.). The reason is *retry safety*:

1. Task crashes mid-write.
2. Airflow retries the task.
3. Without `INSERT OR IGNORE`, you duplicate half the batch and your
   downstream aggregates over-count.

Writes are also wrapped in a single transaction (sqlite's `with` block),
so it's "all or nothing": a partial failure rolls back, not "first
hundred rows inserted, then a crash".

## 5. Streams > lists

`CsvSource.read_*` returns an iterator, not a list. The same goes for
`require_positive_qty` and friends in `include/transforms/`.

Why?  Two reasons:

* **Memory.** A 50 GB CSV will not fit into a single worker's RAM;
  streaming means constant memory.
* **Composability.** A pipeline of streaming functions can be assembled
  without ever materialising an intermediate.  We get the same benefit
  Knuth describes in TAOCP §2.6 with linked allocation and "tape"
  algorithms — they all fit in memory because they never hold more
  than one element at a time.

The cost is small: callers can't `len()` the result, can't index it,
can't iterate twice. We lean on `list(...)` at the DAG-task boundary
where small, fixed inputs are expected.

## 6. Errors are values where they can be, exceptions where they must be

The cleaning pipeline returns a `QualityReport` for the "how many rows
were accepted / rejected" question, not an exception — because partial
success is *not* an exceptional condition in an ETL.

On the other hand, a missing source CSV or a malformed column header *is*
exceptional: we raise `SourceUnavailable` or `SchemaMismatch` and let the
Airflow retry policy decide.

This split mirrors what Knuth argues in TAOCP §2.2 (coroutines and
recoverable failures vs. outright errors). The DAG layer is expected to
know which kind it's seeing.

## 7. We track decisions with Honcho

Because this is a learning project, **why** a thing was built the way it
was matters as much as *what* was built.  We track:

* the invariants each pipeline enforces,
* the contracts between modules,
* the patterns we're deliberately *not* using (and why),

in a Honcho workspace so that someone picking up the project weeks later
can ask "why doesn't this use `SubDagOperator`?" and get the answer.

The CLI for inspecting the workspace is documented at `docs/honcho.md`.

## Open questions (not yet decided)

* Whether to ship a `pyproject.toml` extra for `kubernetes` so people can
  try running the DAGs on k8s.
* Whether `include/transforms/` should grow a `pandas`-backed module
  alongside the pure-stdlib one. Current stance: keep pandas out; it
  changes the contract.
* Whether to introduce a `pre-commit` configuration that runs
  `pytest tests/dags/` on every commit.
