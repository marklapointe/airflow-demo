# Web UI — DAG Explorer

A project-specific Flask app that **statically reads every DAG file via
AST** and renders each one's task graph with what each task actually
*does*. Lives at `web/`.

## Why a custom UI

The Airflow webserver shows *runtime* state (what's currently running, log
lines, XCom values). For a learning project the more useful view is *the
structure of each DAG* — its tasks, dependencies, what each task does.
That's what this UI shows.

A second reason, less obvious:

> This UI works **without Airflow installed.** A reader can clone the
> repo, install only Flask, run `python main.py ui`, and explore all 12
> DAGs end-to-end before they ever run `airflow db migrate`.

Importing a DAG triggers Airflow's DAG-bag registration plus any module-
level side effects. The extractor at `web/extractor.py` parses the file
into AST instead, so it never runs your code.

## Routes

| Route                        | Served by    | Purpose                                                  |
|------------------------------|--------------|----------------------------------------------------------|
| `GET /`                      | This app     | Index of every DAG, each in a card with a mini Mermaid.  |
| `GET /dag/<dag_id>`          | This app     | Per-DAG detail: rich Mermaid, tasks table, edges table.  |
| `GET /source/<path:relpath>` | This app     | Raw DAG source (text/plain). Used by `view source` links. |
| `GET /system`                | This app     | The whole project's architecture diagram.                 |
| `GET /about`                 | This app     | How extraction works.                                    |
| `GET /airflow/`              | Proxy        | 302 redirect to `/airflow/home`.                          |
| `GET/POST/PUT/DELETE/PATCH /airflow/<path>` | Proxy | Forward to `airflow_webserver_url` via httpx.    |

The proxy at `/airflow/*` is **only mounted when you pass
`airflow_webserver_url`** (or set `AIRFLOW_WEBSERVER_URL` in the env and
launch with `python main.py ui`). When unconfigured, those routes
return 404 — the static reader remains fully usable without Airflow.

## Running

```bash
# Activate venv with Flask + httpx (and pytest-playwright for the e2e tests).
pip install flask httpx

# Static explorer only — no Airflow needed.
python main.py ui            # default port 5050 (5000 is taken by macOS Control Center)

# Static + runtime proxy. Start `airflow webserver` first (any free port),
# then point this process at it.
python main.py ui --airflow-url=http://127.0.0.1:8080

# Or set the env var and skip the flag.
AIRFLOW_WEBSERVER_URL=http://127.0.0.1:8080 python main.py ui

# Port already in use? Auto-pick the next free one.
python main.py ui --port 8080 --find-port
```

The CLI sub-command is at `main.py::cmd_ui`. It uses Flask's built-in
server (Werkzeug), which is fine for development. For production we'd
swap in gunicorn or uvicorn+ASGI.

### Port notes

* **Port 5000** is taken by macOS Control Center. Don't use it.
* **Port 8080** is the airflow default but is also taken by lots of
  other things (Jupyter, Synology, MacPython apps). Pass any free port.
* The CLI prints the resolved port and exits cleanly with a friendly
  message if the requested one is busy; `--find-port` makes it
  auto-pick instead.
* `web/ports.py::find_free_port()` is the helper; it strictly checks
  port availability (no `SO_REUSEADDR` shortcut) so the probe matches
  what `bind()` will see at runtime.

## How extraction works

`web/extractor.py` walks the AST of every `*.py` file under `dags/` and
collects:

* `dag_id`, `schedule=`, `description=`, `tags=` from the `with DAG(...)`
  block.
* `TaskNode(task_id, operator, group, line, params, activity, python_callable)`
  for every `XxxOperator(...)` and `@task` call.
* `TaskEdge(source, target)` for every `>>` and `<<` chain (with list
  expansion inside chains).
* `TaskGroup` references, rendered as Mermaid subgraphs.
* `extraction_warnings` for things the static walk couldn't fully resolve
  (dynamic `>>` endpoints, branch-returned task ids, `.expand()`).

The output is a `DagMetadata` with two Mermaid serializers:

* `to_mermaid_simple()` — for index cards: task id + operator label.
* `to_mermaid_rich()` — for per-DAG pages: icon, task id, operator, the
  one-line activity pulled from the `python_callable` docstring.

Both emit `graph TD` with `classDef` directives at the end. The
`_OPERATOR_PRESETS` table maps operator names to (icon, label, css class)
triples.

## Security model

* **No runtime imports of the DAG.** Pure AST walk. No `__import__` calls
  on the DAG module.
* **`/source/<path>` traversal hardening.** The handler resolves the
  requested path against the repo root via `Path.resolve()` and only
  serves files whose absolute path begins with the resolved repo root.
  `..` traversal and absolute paths outside the repo return `403`/`404`.
* **No user input is executed.** All rendering goes through Jinja's
  auto-escape; Mermaid input is a string we built.
* **No secrets in the UI.** Hooks and connections live in Airflow's env
  vars; the UI never sees them.

## Testing strategy

The web module is covered on **three levels**:

1. **Unit tests** — `tests/unit/test_extractor.py` (100 tests) pins
   every branch of the AST walker. `tests/unit/test_app.py` (22 tests)
   boots the Flask app against a fixture repo and walks every route.
   Both layers target **100% branch coverage** (`pytest --cov=web
   --cov-branch`).
2. **End-to-end browser tests** — `tests/e2e/test_ui.py` (11 tests)
   boots the real Flask app in a daemon thread and drives **real
   Chromium** via Playwright. These tests assert that Mermaid actually
   materialises an `<svg>` inside each `<pre class="mermaid">` — not
   just that the HTML contains a `mermaid` class. That's the test a
   unit test would never catch.
3. **Static integration test** — `TestAgainstRepoDags` runs the
   extractor over every real DAG and asserts that none is empty and that
   the discovered edges match the visible structure.

Run them:

```bash
.venv/bin/python -m pytest tests/unit/ --cov=web --cov-branch            # fast
.venv/bin/python -m playwright install chromium                            # once
.venv/bin/python -m pytest tests/e2e/                                      # browser
```

## Why dependency injection in `create_app`

The Flask app factory `web/app.create_app(repo_root, dags_dir)` accepts
paths as arguments instead of hard-coding `Path(__file__).resolve().parent /
"dags"`. Tests pass fixture paths, so the unit suite never depends on the
shape of the live `dags/` tree.

```python
app = create_app(repo_root=fixture_repo, dags_dir=fixture_repo / "dags")
```

If `dags_dir` doesn't exist (e.g. a fresh checkout), the index still
renders with zero DAGs instead of crashing on `rglob`.

## How to extend

* **New operator icon.** Add an entry to `_OPERATOR_PRESETS` in
  `web/extractor.py`. The `_mermaid_classdefs()` directive list already
  covers `py/branch/bash/empty/sc/trigger/sensor/hook/tf/other` — add a
  new class only if your operator needs a new colour.
* **New page.** Add a route to `web/app.py`, a template under
  `web/templates/`, and a CSS class to `web/static/style.css`. Add a
  Playwright e2e test under `tests/e2e/` to assert the page renders.
* **Custom extraction logic.** Extend `extract_dag_metadata` (returns
  `DagMetadata`); never call `ast.walk` directly from a route — keep
  the extraction testable.
