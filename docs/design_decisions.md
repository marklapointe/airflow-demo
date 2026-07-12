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

## 8. The DAG-explorer UI reads DAGs via AST, never via import

`web/extractor.py` parses every `*.py` under `dags/` into an `ast.Module`
and walks it. It never executes user code, never imports `airflow.*`,
and never invokes the DAG-bag. The reasons are the same two reasons
Taoclty argued for static reading in TAOCP §1 (data precedes program):

* **Side-effect freedom.** Importing `dags/03_orchestration/dynamic_dag_generation.py`
  may connect to a remote API, set module-level globals, or register
  webhooks. AST gives us the graph without those surprises.
* **No Airflow dependency at UI time.** The reader can install only
  Flask. The web UI exists for the same reason the unit tests do — to
  push the heavy imports as far back as possible.

Failure to capture a pattern statically (e.g. `.expand()` on
TaskFlow, branch-returned task ids, dynamic fan-out) emits a
**warning** on `DagMetadata.extraction_warnings`, not an exception. The
UI renders the source link for the offending DAG so the reader can see
what the static walker couldn't.

The extractor emits two Mermaid graphs per DAG — a compact one
(`to_mermaid_simple`, task id + operator) for index cards, and a rich
one (`to_mermaid_rich`, icon + id + activity + subgraphs + classDef
colours) for the per-DAG page. Both use the same `_OPERATOR_PRESETS`
table, so icons, labels, and CSS classes stay in lockstep.

## 9. 100% branch coverage on the contract modules, not the DAGs

We pin coverage to **100%** on the modules that own contracts:

* `include/domain/__init__.py` — typed records.
* `include/io/__init__.py` — CSV / SQLite adapters.
* `include/transforms/__init__.py` — pure cleaning pipeline.
* `web/extractor.py` — static DAG metadata extractor.
* `web/app.py` — Flask UI.

DAG files (`dags/**/*.py`) are *integration-tested* via
`tests/dags/test_dag_integrity.py` (cycle / missing-owner / dangling-
reference checks) but are **not** unit-tested. The DAG is mostly
composition, and the meaningful contracts already live in `include/`.
Test energy goes to the contracts; the integration tests guard the
composition.

Enforced via `pytest --cov=web --cov=include --cov-branch` in CI. A
green PR keeps the lines at 100/0/0 across all five modules above.

## 10. Dependency injection in app factories

`web/app.py::create_app(repo_root, dags_dir)` accepts paths as
parameters rather than computing them from `Path(__file__).resolve()` at
module-import time. Tests pass fixture paths; production passes the
project layout. The module-level constants are *defaults*, not globals:

```python
app = create_app(repo_root=fixture_repo, dags_dir=fixture_repo / "dags")
```

The pattern keeps `tests/unit/test_app.py` free of `monkeypatch`, free
of `PYTHONPATH` shenanigans, and free of any coupling to the shape of
the live `dags/` tree. Tests can grow sample DAGs in a tmp dir and
assert against the rendered HTML without one real DAG needing to
exist on disk.

## 11. Real-browser fixture for SPA-shaped flows

`tests/e2e/test_ui.py` uses real Chromium via Playwright because the UI
includes Mermaid.js — a script that only executes inside a browser. A
unit test that asserts "the HTML contains a `<pre class=mermaid>`" is
*not enough*: Mermaid might be silently broken (CDN unreachable,
syntax error in the graph, version mismatch).

The fixture boots a real Werkzeug server in a daemon thread, shares one
Chromium launch across the session's 11 tests, and asserts that
Mermaid materialises an `<svg>` inside each `.mermaid` block:
`expect(pre.locator("svg")).to_be_visible(timeout=5000)`.

The e2e tests are marked `pytest.mark.e2e` and excluded from the default
`pytest` run by the marker registration in `pyproject.toml`. CI runs
both:

* `pytest -m "not e2e"` on every PR (fast feedback).
* `pytest -m e2e` nightly or pre-release (full Chromium spin-up).

## 12. Single binary, two concerns — proxy at `/airflow/`

We boot **one Flask process** that serves the static explorer natively
*and* proxies runtime views to `airflow webserver` via `httpx`. The
boundary is the URL prefix:

* `/`, `/dag/<id>`, `/source/...`, `/system`, `/about` — handled in this
  process. No Airflow dependency at runtime.
* `/airflow/<path>` — proxied to `airflow_webserver_url` (default
  `$AIRFLOW_WEBSERVER_URL`, override via `--airflow-url`).

When the env var is unset, the `/airflow/*` routes aren't registered —
`create_app(airflow_webserver_url=None)` returns an app whose index
returns no proxy links. The static reader remains fully usable without
Airflow installed; the runtime views surface only when the operator
wants them.

Trade-off matrix vs the alternatives:

* **Plugin inside airflow webserver** — costs: requires Airflow
  installed; static-extraction purity is harder to honour. Wins:
  one process, one URL.
* **Reverse proxy** (caddy / nginx / haproxy) — costs: two processes +
  proxy config. Wins: production-grade; both processes restartable
  independently.
* **Single Flask process with proxy (this design)** — costs: HTTP proxy
  adds a network hop for runtime views; some Airflow endpoints use
  WebSockets (live logs) which this proxy does not yet forward.
  Wins: one binary; the static reader stays fast and dependency-free.

The WebSocket limitation is honest: the current proxy passes HTTP only.
For Airflow's live-log streaming, either run airflow webserver on its
own port and bookmark it, or extend this proxy with WebSocket forwarding
(httpx doesn't support WS; consider `websockets` or `flask-sock`).

## 13. Solid default ports — no auto-discovery, no moving targets

Port 5000 is famously taken by macOS Control Center. Port 8080 — the
airflow default — is taken by Jupyter, Synology, half the Python apps
on the planet, and any number of internal HTTP services. Picking
either as a default means the reader hits a confusing bind error on
first run.

* The Flask UI defaults to **port 7123** — verified free at design time on
  the development system (not on auto-discovery, not on the user's system).
* For `airflow webserver` examples we recommend **7161** — same rationale
  (verified free, not a default for anything else).
* No `--find-port` flag, no auto-discovery, no moving targets. If the
  port is busy the CLI exits with a clean error message and a list of
  solid alternatives (`7123`, `7161`, `5050`, `5555`, `7777`) so the reader picks one
  explicitly. The port you start with is the port you stay on.

The CLI's busy-port error enumerates the alternative ports by name and
explicitly names the ones to avoid (5000, 7000, 8080, 8888) so the
reader doesn't have to guess.
