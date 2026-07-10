"""End-to-end Playwright tests for the DAG-explorer web UI.

These tests boot the real Flask app in a background thread, point it at a
fixture repo (so we never depend on the live ``dags/`` tree), and drive a
real Chromium browser through the user-visible flows:

    - Landing page renders all expected DAGs with Mermaid graphs.
    - Per-DAG detail page renders the rich activity graph and tables.
    - 404 page for an unknown DAG.
    - /source/<relpath> serves the actual DAG source.

Why drive a real browser?  Jinja templates reference Mermaid.js from a CDN;
that only renders when a real browser executes the JS.  Static-html tests
would miss Mermaid-side bugs (broken operator icons, missing subgraphs,
SVG never materialising, etc.).

Skipping: these tests are *not* part of the default CI matrix — they need a
Chromium binary and take seconds to spin up.  Run them explicitly::

    .venv/bin/python -m pytest tests/e2e/ -v
"""
from __future__ import annotations

import textwrap
import threading
from pathlib import Path

import pytest
from playwright.sync_api import Page, expect, sync_playwright
from werkzeug.serving import make_server


pytestmark = pytest.mark.e2e

from web.app import create_app


SAMPLE_DAG = textwrap.dedent(
    """
    \"\"\"A sample DAG for fixture use.\"\"\"
    from airflow import DAG
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from datetime import datetime

    def greet() -> None:
        \"\"\"Print a greeting.\"\"\"
        print(\"hello\")

    with DAG(
        dag_id=\"sample\",
        description=\"Learn the basics\",
        schedule=\"@daily\",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=[\"learning\"],
    ):
        start = EmptyOperator(task_id=\"start\")
        g: PythonOperator = PythonOperator(task_id=\"greet\", python_callable=greet)
        end = EmptyOperator(task_id=\"end\")
        start >> g >> end
    """
).strip() + "\n"


CROSS_DAG = textwrap.dedent(
    """
    \"\"\"A second DAG (dataset-driven, single task).\"\"\"
    from airflow import DAG
    from airflow.providers.standard.operators.empty import EmptyOperator
    from datetime import datetime
    with DAG(
        dag_id=\"consumer\",
        schedule=\"@hourly\",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=[\"consumer\"],
    ):
        only = EmptyOperator(task_id=\"only_task\")
    """
).strip() + "\n"


@pytest.fixture(scope="session")
def fixture_repo(tmp_path_factory) -> Path:
    repo = tmp_path_factory.mktemp("e2e_repo")
    dags = repo / "dags"
    dags.mkdir()
    (dags / "sample.py").write_text(SAMPLE_DAG)
    (dags / "consumer.py").write_text(CROSS_DAG)
    (dags / "_skipme.py").write_text(SAMPLE_DAG.replace("sample", "skip_me"))
    return repo


class _Server:
    """A simple threaded WSGI server we can boot/stop in tests."""

    def __init__(self, app, host: str = "127.0.0.1") -> None:
        self.srv = make_server(host, 0, app)
        self.port = self.srv.server_port
        self.thread = threading.Thread(
            target=self.srv.serve_forever, name="flask-e2e", daemon=True
        )

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.srv.shutdown()
        self.thread.join(timeout=5)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


@pytest.fixture(scope="session")
def server(fixture_repo: Path):
    dags = fixture_repo / "dags"
    app = create_app(repo_root=fixture_repo, dags_dir=dags)
    app.config.update(TESTING=True)
    with _Server(app) as srv:
        yield srv.url


@pytest.fixture(scope="session")
def browser():
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        yield browser
        browser.close()


@pytest.fixture(scope="session")
def page(browser):
    ctx = browser.new_context()
    page = ctx.new_page()
    yield page
    ctx.close()


# --- tests ------------------------------------------------------------------


def test_landing_renders_all_visible_dags(page: Page, server: str):
    page.goto(f"{server}/")
    expect(page.locator("body")).to_contain_text("Airflow DAG Explorer")
    expect(page.locator(".card", has_text="sample")).to_be_visible()
    expect(page.locator(".card", has_text="consumer")).to_be_visible()
    # Underscore-prefixed file must not surface.
    assert "skip_me" not in page.content()


def test_index_mermaid_blocks_render(page: Page, server: str):
    page.goto(f"{server}/")
    # Mermaid renders each `<pre class="mermaid">` as an SVG inside it.
    pre = page.locator(".mini-graph pre.mermaid").first
    expect(pre).to_be_visible()
    # Wait for Mermaid to finish (it injects an SVG child).
    svg = pre.locator("svg")
    expect(svg).to_be_visible(timeout=5000)


def test_dag_detail_renders_full_graph(page: Page, server: str):
    page.goto(f"{server}/dag/sample")
    expect(page.locator("h1")).to_contain_text("sample")
    # Module docstring surfaces in the detail page.
    expect(page.locator("pre.docstring")).to_contain_text("fixture use")
    # Rich-graph SVG appears.
    svg = page.locator("#rich-graph svg")
    expect(svg).to_be_visible(timeout=5000)


def test_dag_detail_tasks_table(page: Page, server: str):
    page.goto(f"{server}/dag/sample")
    table = page.locator("table.tasks-table")
    expect(table).to_be_visible()
    # Three tasks: start, greet, end.
    rows = table.locator("tbody tr")
    expect(rows).to_have_count(3)
    # Activity column should show "Print a greeting."
    expect(page.locator(".activity")).to_contain_text("Print a greeting")


def test_unknown_dag_returns_404(page: Page, server: str):
    resp = page.goto(f"{server}/dag/does_not_exist")
    assert resp is not None
    assert resp.status == 404


def test_source_view_renders_dag_file(page: Page, server: str):
    resp = page.goto(f"{server}/source/dags/sample.py")
    assert resp is not None
    assert resp.status == 200
    # Should be text/plain per Flask Response mimetype.
    ctype = resp.header_value("content-type") or ""
    assert ctype.startswith("text/plain"), ctype
    body = page.content()
    assert "with DAG(" in body


def test_source_path_traversal_blocked(page: Page, server: str):
    # Werkzeug normalises `..` segments away before routing, so the request
    # never reaches the handler.  Either 403 or 404 is acceptable.
    resp = page.goto(f"{server}/source/../etc/passwd")
    assert resp is not None
    assert resp.status in (403, 404)


def test_navigation_index_back_to_dag(page: Page, server: str):
    page.goto(f"{server}/")
    page.locator(".card", has_text="sample").click()
    expect(page).to_have_url(f"{server}/dag/sample")
    # The back link returns to /
    page.locator("a.back").click()
    expect(page).to_have_url(f"{server}/")


def test_system_map_renders(page: Page, server: str):
    page.goto(f"{server}/system")
    expect(page.locator("h1")).to_contain_text("System map")
    # The system page embeds a Mermaid graph (LR type).
    pre = page.locator("pre.mermaid")
    expect(pre).to_be_visible()
    svg = pre.locator("svg")
    expect(svg).to_be_visible(timeout=5000)


def test_about_renders(page: Page, server: str):
    page.goto(f"{server}/about")
    expect(page.locator("h1")).to_contain_text("About")


def test_no_console_errors(page: Page, server: str):
    errors: list[str] = []
    page.on("pageerror", lambda exc: errors.append(str(exc)))
    page.on(
        "console",
        lambda msg: errors.append(msg.text)
        if msg.type == "error"
        else None,
    )
    page.goto(f"{server}/")
    page.locator(".card", has_text="sample").click()
    page.goto(f"{server}/system")
    page.goto(f"{server}/about")
    assert not errors, f"console errors: {errors}"
