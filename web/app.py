"""Flask app for the DAG-explorer web UI + Airflow webserver proxy.

Routes (native, run by this process):
  GET  /                            — index of all DAGs, each card with a mini graph
  GET  /dag/<dag_id>                — per-DAG detail with the rich activity graph
  GET  /source/<relpath>            — raw DAG source file (for "view source" links)
  GET  /system                      — the whole project's architecture diagram
  GET  /about                       — what this UI is and how extraction works

Proxy routes (only when ``airflow_webserver_url`` is configured):
  GET  /airflow/                    — redirect to upstream root
  GET  /airflow/<path:upath>        — HTTP proxy to airflow webserver

The Mermaid.js graph rendering is loaded from a CDN; no JS build pipeline.

Why a custom UI alongside the Airflow webserver?
  1. The Airflow webserver shows *runtime* state (what is currently running, log
     lines, XCom values). For a learning project, the *structure* of each DAG —
     its tasks, dependencies, what each task does — is the more valuable view.
  2. This UI works without Airflow installed. A reader can clone the repo,
     install only Flask + httpx, run `python main.py ui`, and explore all 12
     DAGs end-to-end before they ever run `airflow db migrate`.
  3. The proxy at ``/airflow/*`` lets users *also* visit the Airflow webserver
     views through the same origin, so the two UIs feel like one product
     without merging them at the code level.

The proxy uses httpx with a short timeout. If the upstream is unreachable we
return 502 Bad Gateway so the reader knows where the failure is.
"""
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import httpx
from flask import Flask, Response, abort, render_template, request

from web.extractor import extract_dag_metadata


DEFAULT_REPO_ROOT: Path = Path(__file__).resolve().parents[1]
DEFAULT_DAGS_DIR: Path = DEFAULT_REPO_ROOT / "dags"

# We forward headers but strip ones that would leak our origin or pin the
# wrong Host back to the upstream. ``X-Forwarded-`` headers are useful to set
# explicitly (with our own scheme/host) rather than let the upstream infer.
_HOP_BY_HOP = frozenset({
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
})


def create_app(
    repo_root: Path | None = None,
    dags_dir: Path | None = None,
    airflow_webserver_url: str | None = None,
    proxy_timeout: float = 5.0,
) -> Flask:
    """Build a Flask app pointing at the given repo / dags directory.

    Args:
        repo_root: Project root (used for ``/source/...`` resolution).
        dags_dir: Directory to scan for DAGs (default: ``<repo_root>/dags``).
        airflow_webserver_url: If set, mount the ``/airflow/*`` reverse proxy
            pointing at this URL (typically ``http://127.0.0.1:8080``).
        proxy_timeout: Seconds to wait on the upstream before returning 502.
    """
    repo_root = Path(repo_root or DEFAULT_REPO_ROOT)
    dags_dir = Path(dags_dir or DEFAULT_DAGS_DIR)

    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
        static_folder=str(Path(__file__).parent / "static"),
    )
    app.config["AIRFLOW_WEBSERVER_URL"] = airflow_webserver_url
    app.config["PROXY_TIMEOUT"] = proxy_timeout

    def _all_dags() -> list:
        if not dags_dir.exists():
            return []
        return [
            extract_dag_metadata(p, repo_root)
            for p in sorted(dags_dir.rglob("*.py"))
            if not p.name.startswith("_")
        ]

    @app.route("/")
    def index():
        return render_template(
            "index.html",
            dags=_all_dags(),
            repo_root=str(repo_root),
            airflow_webserver_url=airflow_webserver_url,
        )

    @app.route("/dag/<dag_id>")
    def dag_detail(dag_id):
        for meta in _all_dags():
            if meta.dag_id == dag_id:
                return render_template(
                    "dag.html",
                    dag=meta,
                    airflow_webserver_url=airflow_webserver_url,
                )
        abort(404)

    @app.route("/source/<path:relative_path>")
    def source(relative_path):
        full = (repo_root / relative_path).resolve()
        if not str(full).startswith(str(repo_root.resolve())):
            abort(403)
        if not full.exists() or not full.is_file():
            abort(404)
        return Response(full.read_text(encoding="utf-8"), mimetype="text/plain")

    @app.route("/system")
    def system():
        return render_template("system.html")

    @app.route("/about")
    def about():
        return render_template("about.html")

    if airflow_webserver_url:
        _register_proxy(app, airflow_webserver_url, proxy_timeout)

    return app


def _register_proxy(app: Flask, upstream: str, timeout: float) -> None:
    """Mount ``/airflow/`` proxy routes that forward to ``upstream``.

    The two routes (with and without trailing path) let us tolerate either
    ``http://localhost:8080`` or ``http://localhost:8080/`` as the upstream
    URL without double-slashing.
    """
    base = _normalise_upstream_base(upstream)

    @app.route("/airflow/")
    def airflow_root_redirect():
        return Response(
            status=302,
            headers={"Location": "/airflow/home"},
        )

    @app.route("/airflow", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
    @app.route("/airflow/<path:upath>", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
    def airflow_proxy(upath: str = ""):
        return _proxy_request(base, upath, timeout)


def _normalise_upstream_base(upstream: str) -> str:
    """Strip any trailing path so we can append our own ``/<upath>`` reliably."""
    parts = urlsplit(upstream)
    return urlunsplit((parts.scheme, parts.netloc, "", "", ""))


def _proxy_request(base: str, upath: str, timeout: float) -> Response:
    """Forward ``request`` to ``base/<upath>`` and stream the response back."""
    target = f"{base}/{upath}" if upath else base.rstrip("/")
    if request.query_string:
        target = f"{target}?{request.query_string.decode()}"

    fwd_headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in _HOP_BY_HOP
    }
    fwd_headers["X-Forwarded-Proto"] = request.scheme
    fwd_headers["X-Forwarded-Host"] = request.host

    try:
        upstream_resp = httpx.request(
            method=request.method,
            url=target,
            params=None,
            headers=fwd_headers,
            content=request.get_data(),
            timeout=timeout,
            follow_redirects=False,
        )
    except httpx.RequestError as exc:
        return Response(
            f"Upstream airflow webserver unreachable: {exc}",
            status=502,
            mimetype="text/plain",
        )

    passthrough_headers = {
        k: v
        for k, v in upstream_resp.headers.items()
        if k.lower() not in _HOP_BY_HOP
    }
    return Response(
        upstream_resp.content,
        status=upstream_resp.status_code,
        headers=passthrough_headers,
    )


if __name__ == "__main__":  # pragma: no cover
    create_app().run(debug=True, port=5000)
