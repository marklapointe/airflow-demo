"""Flask app for the DAG-explorer web UI.

Routes:
  GET  /                  — index of all DAGs, each card with a mini graph
  GET  /dag/<dag_id>      — per-DAG detail with the rich activity graph
  GET  /source/<relpath>  — raw DAG source file (for "view source" links)
  GET  /system            — the whole project's architecture diagram
  GET  /about             — what this UI is and how extraction works

The UI uses Mermaid.js (CDN) for graph rendering. No JavaScript build pipeline.

Why a custom UI and not just `airflow webserver`? Two reasons:
  1. The Airflow webserver shows *runtime* state (what is currently running, log
     lines, XCom values). For a learning project, the *structure* of each DAG —
     its tasks, dependencies, what each task does — is the more valuable view.
  2. This UI works without Airflow installed. A reader can clone the repo,
     install only Flask, run `python main.py ui`, and explore all 12 DAGs.
"""
from __future__ import annotations

from pathlib import Path

from flask import Flask, abort, render_template, Response

from web.extractor import extract_dag_metadata


DEFAULT_REPO_ROOT: Path = Path(__file__).resolve().parents[1]
DEFAULT_DAGS_DIR: Path = DEFAULT_REPO_ROOT / "dags"


def create_app(
    repo_root: Path | None = None,
    dags_dir: Path | None = None,
) -> Flask:
    """Build a Flask app pointing at the given repo / dags directory.

    Both arguments default to the project layout (``<this file>/../dags``).
    Tests pass fixtures via these to avoid scanning the real repo.
    """
    repo_root = Path(repo_root or DEFAULT_REPO_ROOT)
    dags_dir = Path(dags_dir or DEFAULT_DAGS_DIR)

    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
        static_folder=str(Path(__file__).parent / "static"),
    )

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
        )

    @app.route("/dag/<dag_id>")
    def dag_detail(dag_id):
        for meta in _all_dags():
            if meta.dag_id == dag_id:
                return render_template("dag.html", dag=meta)
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

    return app


if __name__ == "__main__":  # pragma: no cover
    create_app().run(debug=True, port=5000)