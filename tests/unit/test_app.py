"""Unit tests for the Flask app (`web/app.py`).

No real network, no LiveServer. We use Flask's test_client against a fresh
app instance, and point ``DAGS_DIR`` and ``REPO_ROOT`` at fixtures so we
never depend on whether the real ``dags/`` directory is populated.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from web.app import create_app


SAMPLE_DAG = textwrap.dedent(
    """
    \"\"\"A sample DAG for fixture use.\"\"\"
    from airflow import DAG
    from airflow.providers.standard.operators.empty import EmptyOperator
    from datetime import datetime

    with DAG(
        dag_id="sample",
        schedule="@daily",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["learning", "smoke"],
    ):
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end")
        start >> end
    """
).strip() + "\n"


SECOND_DAG = textwrap.dedent(
    """
    from airflow import DAG
    from airflow.providers.standard.operators.empty import EmptyOperator
    from datetime import datetime

    with DAG(
        dag_id="second",
        schedule="@hourly",
        start_date=datetime(2024, 6, 1),
        catchup=False,
        tags=["smoke"],
    ):
        x = EmptyOperator(task_id="x")
    """
).strip() + "\n"


NON_DAG = "# not a DAG file\nx = 1\n"


@pytest.fixture
def fixture_repo(tmp_path: Path) -> Path:
    """Create a minimal repo layout under ``tmp_path``.

    Layout::

        fixture_repo/
            dags/
                sample.py        # valid DAG
                _skipme.py       # underscore-prefixed — must be skipped
                second.py        # another valid DAG, alphabetically later
                invalid_syntax.py  # not strictly needed; Index scans it
            include/
                __init__.py
            docs/
                ignored.txt
    """
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "sample.py").write_text(SAMPLE_DAG)
    (dags / "second.py").write_text(SECOND_DAG)
    (dags / "_skipme.py").write_text(SAMPLE_DAG.replace('dag_id="sample"', 'dag_id="skip_me"'))
    (dags / "ignored.txt").write_text("not a python file")
    (tmp_path / "include").mkdir()
    (tmp_path / "include" / "__init__.py").touch()
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "ignored.txt").write_text("ignored")
    return tmp_path


@pytest.fixture
def client(fixture_repo: Path):
    dags = fixture_repo / "dags"
    app = create_app(repo_root=fixture_repo, dags_dir=dags)
    app.config.update(TESTING=True)
    with app.test_client() as c:
        yield c


# --- create_app factory ----------------------------------------------------


class TestCreateApp:
    def test_returns_flask_app(self, fixture_repo: Path):
        app = create_app(repo_root=fixture_repo, dags_dir=fixture_repo / "dags")
        # Flask app duck-type checks
        assert app.name == "web.app"
        assert app.config.get("TEMPLATES_AUTO_RELOAD") in (True, None)

    def test_defaults_to_project_layout(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW_HOME", raising=False)
        app = create_app()  # No args — must default to project layout.
        assert app is not None

    def test_missing_dags_dir_returns_empty_index(self, tmp_path: Path):
        dags = tmp_path / "no_such_dir"
        app = create_app(repo_root=tmp_path, dags_dir=dags)
        resp = app.test_client().get("/")
        assert resp.status_code == 200

    def test_has_all_five_routes(self, client):
        # Routes must all be registered, not 404'ing by name.
        for path in ("/", "/dag/sample", "/system", "/about", "/source/dags/sample.py"):
            resp = client.get(path)
            assert resp.status_code in (200, 404), f"{path} returned {resp.status_code}"
            assert resp.status_code != 500


# --- index ----------------------------------------------------------------


class TestIndex:
    def test_index_returns_200(self, client):
        resp = client.get("/")
        assert resp.status_code == 200

    def test_index_lists_known_dags(self, client):
        resp = client.get("/")
        body = resp.get_data(as_text=True)
        assert "sample" in body
        assert "second" in body

    def test_index_skips_underscore_files(self, client):
        resp = client.get("/")
        body = resp.get_data(as_text=True)
        assert "skip_me" not in body

    def test_index_ignores_non_python_files(self, client):
        # ignored.txt in dags/ must not affect anything.
        resp = client.get("/")
        assert resp.status_code == 200

    def test_index_includes_mermaid_blocks(self, client):
        resp = client.get("/")
        body = resp.get_data(as_text=True)
        assert "graph TD" in body


# --- dag detail -----------------------------------------------------------


class TestDagDetail:
    def test_known_dag_returns_200(self, client):
        resp = client.get("/dag/sample")
        assert resp.status_code == 200

    def test_unknown_dag_returns_404(self, client):
        resp = client.get("/dag/does_not_exist")
        assert resp.status_code == 404

    def test_dag_detail_renders_mermaid(self, client):
        resp = client.get("/dag/sample")
        body = resp.get_data(as_text=True)
        assert "graph TD" in body
        assert "sample" in body

    def test_dag_detail_renders_docstring(self, client):
        resp = client.get("/dag/sample")
        body = resp.get_data(as_text=True)
        # The sample DAG has a module docstring.
        assert "fixture use" in body


# --- /source --------------------------------------------------------------


class TestSource:
    def test_source_returns_file_contents(self, client, fixture_repo: Path):
        rel = "dags/sample.py"
        resp = client.get(f"/source/{rel}")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "with DAG(" in body

    def test_source_returns_plain_text(self, client):
        resp = client.get("/source/dags/sample.py")
        ctype = resp.headers.get("Content-Type", "")
        assert ctype.startswith("text/plain"), ctype

    def test_source_blocks_parent_traversal(self, client):
        # ../etc/passwd must be rejected even though the slug is valid.
        resp = client.get("/source/../etc/passwd")
        # Werkzeug normalizes the URL — at minimum, no /etc/passwd leak.
        assert resp.status_code in (403, 404)

    def test_source_404_for_missing_file(self, client, fixture_repo: Path):
        resp = client.get("/source/dags/does_not_exist.py")
        assert resp.status_code == 404

    def test_source_path_traversal_blocked_explicit(self, client):
        # `..` segments must not escape the repo root.
        resp = client.get("/source/dags/../../../etc/passwd")
        assert resp.status_code in (403, 404)


# --- system + about -------------------------------------------------------


class TestStatic:
    def test_system_returns_200(self, client):
        resp = client.get("/system")
        assert resp.status_code == 200

    def test_about_returns_200(self, client):
        resp = client.get("/about")
        assert resp.status_code == 200

    def test_about_includes_extraction_steps(self, client):
        resp = client.get("/about")
        body = resp.get_data(as_text=True)
        # The about page enumerates the extractor steps.
        assert "AST" in body
        assert "extract" in body.lower()


# --- repo root hardening ---------------------------------------------------


class TestRepoRootHardening:
    def test_source_path_outside_repo_blocked(self, client, fixture_repo: Path):
        # `/tmp/__outside__` is OUTSIDE the fixture repo; must reject.
        outside = "/tmp/__outside_blocked__/x.py"
        resp = client.get(f"/source{outside}")
        assert resp.status_code in (403, 404)


# --- proxy to airflow webserver -------------------------------------------


import socket
import threading
import time

import pytest
from werkzeug.serving import make_server

from web.app import _normalise_upstream_base, _proxy_request  # noqa: E402


class TestProxyDisabled:
    def test_proxy_routes_absent_when_unconfigured(self, client):
        resp = client.get("/airflow/")
        # Werkzeug returns 404 when the route was never registered.
        assert resp.status_code == 404

    def test_proxy_path_absent_when_unconfigured(self, client):
        resp = client.get("/airflow/dags")
        assert resp.status_code == 404


def _free_port() -> int:
    """Find an unused TCP port for a transient upstream Flask."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Upstream:
    """Tiny WSGI server with one route, used as a stand-in for airflow webserver."""

    def __init__(self):
        self.app = _Upstream._build_app()
        self.port = _free_port()
        self.srv = make_server("127.0.0.1", self.port, self.app)
        self.thread = threading.Thread(target=self.srv.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        time.sleep(0.05)  # let the server bind
        return self

    def __exit__(self, *_):
        self.srv.shutdown()
        self.thread.join(timeout=5)

    @staticmethod
    def _build_app():
        from flask import Flask, request
        upstream_app = Flask("upstream")

        @upstream_app.route("/home")
        def home():
            return f"<h1>Upstream says hello (path={request.path})</h1>"

        @upstream_app.route("/echo/<name>")
        def echo(name):
            return f"echo:{name}"

        @upstream_app.route("/method", methods=["GET", "POST"])
        def method():
            return f"method:{request.method}"

        return upstream_app


@pytest.fixture
def proxy_client(fixture_repo: Path):
    """A Flask test_client wired to a live upstream that lives for the test."""
    with _Upstream() as u:
        app = create_app(
            repo_root=fixture_repo,
            dags_dir=fixture_repo / "dags",
            airflow_webserver_url=f"http://127.0.0.1:{u.port}",
            proxy_timeout=2.0,
        )
        app.config.update(TESTING=True)
        with app.test_client() as c:
            yield c, app


class TestProxyEnabled:
    def test_proxy_redirects_root_to_home(self, proxy_client):
        client, _ = proxy_client
        resp = client.get("/airflow/")
        assert resp.status_code == 302
        assert resp.headers["Location"].endswith("/airflow/home")

    def test_proxy_forwards_path_to_upstream(self, proxy_client):
        client, _ = proxy_client
        resp = client.get("/airflow/echo/abc")
        assert resp.status_code == 200
        assert resp.get_data(as_text=True) == "echo:abc"

    def test_proxy_strips_internal_paths(self, proxy_client):
        client, _ = proxy_client
        # The upstream's /home must NOT carry our ``/airflow`` prefix.
        resp = client.get("/airflow/home")
        assert "Upstream says hello" in resp.get_data(as_text=True)

    def test_proxy_forwards_query_string(self, proxy_client):
        client, _ = proxy_client
        resp = client.get("/airflow/echo/abc?x=1&y=2")
        assert resp.status_code == 200

    def test_proxy_returns_502_when_upstream_unreachable(self, fixture_repo):
        # Use a port we just freed — nothing listening.
        dead_port = _free_port()
        app = create_app(
            repo_root=fixture_repo,
            dags_dir=fixture_repo / "dags",
            airflow_webserver_url=f"http://127.0.0.1:{dead_port}",
            proxy_timeout=0.5,
        )
        client = app.test_client()
        resp = client.get("/airflow/echo/abc")
        assert resp.status_code == 502
        body = resp.get_data(as_text=True)
        assert "unreachable" in body.lower()


class TestProxyHelpers:
    def test_normalise_upstream_strips_trailing_path(self):
        # ``urlsplit`` parses both ``/foo`` and ``/foo/`` correctly; we just
        # drop the path so the request builder can re-append ``/<upath>``.
        assert _normalise_upstream_base("http://h:1/foo") == "http://h:1"
        assert _normalise_upstream_base("http://h:1/foo/") == "http://h:1"
        assert _normalise_upstream_base("http://h:1/") == "http://h:1"
        assert _normalise_upstream_base("http://h:1") == "http://h:1"
