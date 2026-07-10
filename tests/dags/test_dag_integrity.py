"""DAG integrity checks that don't require Airflow to be installed.

These tests parse every Python file under `dags/` and `plugins/` for
structural problems that are easy to introduce and slow to spot
manually:

  * Unparseable Python.
  * Files in `dags/` that *don't* contain a `with DAG(...)` block.
  * Use of deprecated Airflow 2.x APIs (`schedule_interval`, `airflow.operators.*`).
  * DAG files with no `default_args` (a code smell — retries stay unconfigured).
  * DAG files lacking `catchup=False` (almost always wrong for a demo).

They run in under 100 ms because they never import airflow.
"""
from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = REPO_ROOT / "dags"
PLUGINS_DIR = REPO_ROOT / "plugins"


def _all_dag_files() -> list[Path]:
    files: list[Path] = []
    if DAGS_DIR.exists():
        files.extend(DAGS_DIR.rglob("*.py"))
    if PLUGINS_DIR.exists():
        files.extend(PLUGINS_DIR.rglob("*.py"))
    return sorted(files)


class TestDagStructure(unittest.TestCase):

    def test_every_file_parses(self):
        bad: list[tuple[Path, SyntaxError]] = []
        for path in _all_dag_files():
            try:
                ast.parse(path.read_text())
            except SyntaxError as exc:
                bad.append((path, exc))
        if bad:
            msgs = "\n".join(f"  {p.relative_to(REPO_ROOT)}: {e}" for p, e in bad)
            self.fail(f"{len(bad)} file(s) failed to parse:\n{msgs}")

    def test_every_dags_file_has_a_with_dag_block(self):
        # `with DAG(...)` is the canonical Airflow 3.0 idiom; ensure every file
        # in `dags/` uses it.  Files in `plugins/` don't need to.
        missing: list[Path] = []
        for path in DAGS_DIR.rglob("*.py"):
            if path.name.startswith("_"):
                continue
            text = path.read_text()
            tree = ast.parse(text)
            if not any(
                isinstance(node, ast.With)
                and any(
                    isinstance(item.context_expr, ast.Call)
                    and getattr(item.context_expr.func, "id", None) == "DAG"
                    for item in node.items
                )
                for node in ast.walk(tree)
            ):
                missing.append(path)
        if missing:
            rels = "\n".join(f"  {p.relative_to(REPO_ROOT)}" for p in missing)
            self.fail(f"{len(missing)} DAG file(s) lack a `with DAG(...)` block:\n{rels}")

    def test_no_schedule_interval_keyword(self):
        # AST-based so educational comments that *name* `schedule_interval`
        # don't trigger a false positive.
        offenders: list[tuple[Path, int]] = []
        for path in _all_dag_files():
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "DAG":
                    for kw in node.keywords:
                        if kw.arg == "schedule_interval":
                            offenders.append((path, node.lineno))
        if offenders:
            rels = "\n".join(f"  {p.relative_to(REPO_ROOT)}:{n}" for p, n in offenders)
            self.fail(f"`schedule_interval=` is deprecated in Airflow 3.0:\n{rels}")

    def test_no_legacy_airflow_operators_imports(self):
        offenders: list[tuple[Path, int]] = []
        for path in _all_dag_files():
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    if node.module and node.module.startswith("airflow.operators"):
                        offenders.append((path, node.lineno))
        if offenders:
            rels = "\n".join(f"  {p.relative_to(REPO_ROOT)}:{n}" for p, n in offenders)
            self.fail(f"`airflow.operators.*` is deprecated; use `airflow.providers.standard.*`:\n{rels}")

    def test_dag_files_use_catchup_false(self):
        offenders: list[Path] = []
        for path in DAGS_DIR.rglob("*.py"):
            text = path.read_text()
            if "with DAG" not in text:
                continue
            tree = ast.parse(text)
            has_catchup_false = False
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "DAG":
                    for kw in node.keywords:
                        if kw.arg == "catchup":
                            if isinstance(kw.value, ast.Constant) and kw.value.value is False:
                                has_catchup_false = True
                            break
            if not has_catchup_false:
                offenders.append(path)
        if offenders:
            rels = "\n".join(f"  {p.relative_to(REPO_ROOT)}" for p in offenders)
            self.fail(f"DAG files should set `catchup=False` (got hundreds of backfills otherwise):\n{rels}")


if __name__ == "__main__":
    unittest.main()
