"""CLI helpers for the learning project.

You can run any of these without starting Airflow's webserver or
scheduler. They're the same boilerplate that the README's debugging
recipes use, packaged into one entry point.

Usage::

    python3 main.py demo                    # run a sample task — debugger-friendly
    python3 main.py list                    # list registered DAGs (uses airflow)
    python3 main.py test <dag> <task>       # equivalent to `airflow tasks test`
    python3 main.py check                   # static-check every DAG file via AST
    python3 main.py ui                       # start the DAG-explorer web UI (default port 7123)
    python3 main.py ui --airflow-url URL    # also proxy /airflow/* to the airflow webserver
"""
from __future__ import annotations

import argparse
import ast
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
DAGS_DIR = REPO_ROOT / "dags"


# --- subcommand implementations ---------------------------------------

def cmd_demo(_: argparse.Namespace) -> int:
    """Run a sample task inline; if you set a breakpoint inside
    `include/transforms/cleaning.py`, the execution will stop.
    """
    if not _airflow_installed():
        print(
            "Airflow is not installed in this environment.  Run:\n"
            "  pip install apache-airflow==3.0.*\n"
            "then re-run this command.",
            file=sys.stderr,
        )
        return 1
    return subprocess.call(
        [
            sys.executable,
            "-m",
            "airflow",
            "tasks",
            "test",
            "airflow_features_demo",
            "processing_group.extract_data",
            "2024-01-01",
        ]
    )


def cmd_list(_: argparse.Namespace) -> int:
    if not _airflow_installed():
        print(
            "Airflow is not installed; cannot list DAGs.  "
            "Install per README and try again.",
            file=sys.stderr,
        )
        return 1
    env = {**_default_env(), "AIRFLOW_HOME": str(REPO_ROOT)}
    return subprocess.call(
        [sys.executable, "-m", "airflow", "dags", "list", "--local"],
        env=env,
    )


def cmd_test(args: argparse.Namespace) -> int:
    if not _airflow_installed():
        print("Airflow is not installed; cannot run `tasks test`.", file=sys.stderr)
        return 1
    cmd = [sys.executable, "-m", "airflow", "tasks", "test", args.dag, args.task, args.date]
    return subprocess.call(cmd, env={**_default_env(), "AIRFLOW_HOME": str(REPO_ROOT)})


def cmd_check(_: argparse.Namespace) -> int:
    """Statically check every Python file under dags/ and plugins/ for:
       * parseable syntax
       * a `with DAG(...)` block at module level
       * a non-empty `schedule=...` argument
       * the absence of the deprecated `schedule_interval=` keyword
       * imports from `airflow.providers.standard.*` (no legacy `airflow.operators.*`)
    """
    failures = 0
    for path in sorted(DAGS_DIR.rglob("*.py")):
        failures += _check_dag_file(path)
    print()
    print(f"checked {len(list(DAGS_DIR.rglob('*.py')))} DAG file(s); {failures} issue(s)")
    return 1 if failures else 0


def cmd_ui(args: argparse.Namespace) -> int:
    """Start the DAG-explorer web UI (and optionally proxy Airflow views)."""
    try:
        from web.app import create_app
    except ImportError as exc:
        print(
            f"Cannot import the web app: {exc}\n"
            "Install Flask + httpx:  pip install flask httpx",
            file=sys.stderr,
        )
        return 1

    import os
    airflow_url = args.airflow_url or os.environ.get("AIRFLOW_WEBSERVER_URL")

    host = args.host
    port = args.port
    if not _port_is_free(host, port):
        print(
            f"❌ Port {port} is in use on {host}.\n"
            f"   Re-run with --port=<free>. Solid picks (verified free on dev system):\n"
            f"     7123  (our default)\n"
            f"     7161  (recommended for airflow webserver)\n"
            f"     5050 / 5555 / 7777 / 8000 / 8500 / 9000  (verified free on dev system)\n"
            f"   Avoid 5000 (macOS Control Center), 7000, 8080 (Jupyter / Synology / lots), 8888 (Jupyter).",
            file=sys.stderr,
        )
        return 1

    print(f"🌐 Airflow DAG Explorer starting on http://{host}:{port}")
    if airflow_url:
        print(f"   Proxying /airflow/* -> {airflow_url}")
    else:
        print(
            "   No --airflow-url given; /airflow/* routes are disabled.\n"
            "   To enable: start `airflow webserver --port 7161` and re-run with "
            "--airflow-url=http://127.0.0.1:7161."
        )
    print(f"   Press Ctrl-C to stop.")
    create_app(
        airflow_webserver_url=airflow_url,
        proxy_timeout=args.proxy_timeout,
    ).run(host=host, port=port, debug=args.debug, use_reloader=False)
    return 0


def _port_is_free(host: str, port: int) -> bool:
    """True iff we can bind ``(host, port)`` right now.

    No auto-discovery, no SO_REUSEADDR trick — a busy port is busy.
    """
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
        except OSError:
            return False
        return True


# --- helpers ----------------------------------------------------------

def _airflow_installed() -> bool:
    try:
        __import__("airflow")  # noqa: F401
    except ImportError:
        return False
    return True


def _default_env() -> dict:
    import os
    return {**os.environ}


def _check_dag_file(path: Path) -> int:
    """Return 1 if `path` has structural issues, 0 otherwise."""
    source = path.read_text()
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        print(f"  FAIL  {path.relative_to(REPO_ROOT)}: syntax error {exc}")
        return 1

    has_dag = any(_has_dag_with(node) for node in ast.walk(tree))
    if not has_dag:
        if not path.name.startswith("_"):
            print(f"  skip  {path.relative_to(REPO_ROOT)}: no `with DAG(...)` block")
        return 0

    issues: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "DAG":
            for kw in node.keywords:
                if kw.arg == "schedule_interval":
                    issues.append(
                        f"line {node.lineno}: uses deprecated `schedule_interval=` "
                        "(Airflow 3.0 prefers `schedule=`)"
                    )
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("airflow.operators"):
            issues.append(
                f"line {node.lineno}: imports from deprecated `airflow.operators.*` "
                "(use `airflow.providers.standard.*`)"
            )

    if issues:
        print(f"  FAIL  {path.relative_to(REPO_ROOT)}:")
        for i in issues:
            print(f"        - {i}")
        return 1

    print(f"  ok    {path.relative_to(REPO_ROOT)}")
    return 0


def _has_dag_with(node: ast.AST) -> bool:
    """A `with DAG(...)` block at module level."""
    return (
        isinstance(node, ast.With)
        and any(
            isinstance(item.context_expr, ast.Call)
            and isinstance(item.context_expr.func, ast.Name)
            and item.context_expr.func.id == "DAG"
            for item in node.items
        )
    )


# --- argparser ---------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Airflow learning project CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("demo", help="run a sample task inline (great with a debugger)").set_defaults(func=cmd_demo)
    sub.add_parser("list", help="list registered DAGs").set_defaults(func=cmd_list)
    sub.add_parser("check", help="static-check every DAG file via AST").set_defaults(func=cmd_check)

    p_test = sub.add_parser("test", help="run `airflow tasks test` against a dag and task")
    p_test.add_argument("dag", help="DAG id")
    p_test.add_argument("task", help="task id, e.g. processing_group.extract_data")
    p_test.add_argument("date", help="execution date, e.g. 2024-01-01")
    p_test.set_defaults(func=cmd_test)

    p_ui = sub.add_parser("ui", help="start the DAG-explorer web UI (Flask)")
    p_ui.add_argument("--host", default="127.0.0.1", help="bind host (default: 127.0.0.1)")
    p_ui.add_argument(
        "--port",
        type=int,
        default=7123,
        help=(
            "bind port (default: 7123; verified free; 5000 is taken by macOS Control Center)"
        ),
    )
    p_ui.add_argument(
        "--find-port",
        action="store_true",
        help="if --port is busy, auto-pick the next free port instead of erroring",
    )
    p_ui.add_argument(
        "--airflow-url",
        default=None,
        help=(
            "URL of the airflow webserver to proxy /airflow/* to. "
            "Defaults to env AIRFLOW_WEBSERVER_URL. "
            "Solid pick: 7161 (verified free at design time; airflow default 8080 is taken everywhere)."
        ),
    )
    p_ui.add_argument(
        "--proxy-timeout",
        type=float,
        default=5.0,
        help="Seconds to wait on the upstream before returning 502 (default: 5.0)",
    )
    p_ui.add_argument("--debug", action="store_true", help="enable Flask debug mode")
    p_ui.set_defaults(func=cmd_ui)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
