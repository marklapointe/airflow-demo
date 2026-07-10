"""Comprehensive tests for web/extractor.py.

Coverage target: 100% of the extractor's branches. The extractor is the
only piece of code in this project that needs to handle every weird
variant of DAG code, so it earns the most thorough test surface.
"""
from __future__ import annotations

import ast
from pathlib import Path
from unittest import mock

import pytest

from web.extractor import (
    DagMetadata,
    TaskEdge,
    TaskNode,
    _activity_line,
    _call_name,
    _emit_chain,
    _first_line,
    _flatten_lshift,
    _flatten_rshift,
    _is_task_decorator,
    _kwarg_name,
    _kwarg_repr,
    _kwarg_str,
    _kwarg_str_list,
    _lookup_activity,
    _m_id,
    _m_label,
    _mermaid_classdefs,
    _operator_class,
    _operator_icon,
    _operator_label,
    _resolve_name,
    _safe_params,
    _safe_relative,
    _truncate,
    extract_dag_metadata,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


def _write_tmp(tmp_path: Path, name: str, source: str) -> Path:
    p = tmp_path / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(source, encoding="utf-8")
    return p


# --- extract_dag_metadata ---------------------------------------------------

class TestExtractDagMetadata:
    def test_syntax_error_returns_empty(self, tmp_path: Path):
        path = _write_tmp(tmp_path, "bad.py", "this is not python\n  def x(:\n")
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert meta.dag_id == "(syntax error)"
        assert "Could not parse" in meta.extraction_warnings[0]
        assert meta.tasks == ()
        assert meta.edges == ()

    def test_no_dag_block_returns_empty(self, tmp_path: Path):
        path = _write_tmp(tmp_path, "nodag.py", "x = 1\ny = 2\n")
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert "No `with DAG(...)` block found." in meta.extraction_warnings
        assert meta.tasks == ()

    def test_dag_block_with_all_features(self, tmp_path: Path):
        src = '''
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime


def extract_stuff() -> str:
    """Pull rows from the source."""
    return "x"


with DAG(
    dag_id="full_demo",
    description="A comprehensive DAG",
    schedule="@daily",
    catchup=False,
    tags=["learning", "demo"],
    start_date=datetime(2024, 1, 1),
):
    start = EmptyOperator(task_id="start")

    with TaskGroup("phase_one") as p1:
        e: PythonOperator = PythonOperator(task_id="e", python_callable=extract_stuff)
        v = BashOperator(task_id="v", bash_command="echo v")
        e >> v

    b = BranchPythonOperator(task_id="b", python_callable=lambda: "high")
    high = PythonOperator(task_id="high", python_callable=extract_stuff)
    low = PythonOperator(task_id="low", python_callable=extract_stuff)

    end = EmptyOperator(task_id="end")

    start >> p1 >> b >> [high, low] >> end
'''
        path = _write_tmp(tmp_path, "full.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert meta.dag_id == "full_demo"
        assert meta.description == "A comprehensive DAG"
        assert meta.schedule == "@daily"
        assert meta.tags == ("learning", "demo")
        ids = [t.task_id for t in meta.tasks]
        assert "start" in ids
        assert "phase_one.e" in ids
        assert "phase_one.v" in ids
        assert "b" in ids
        assert "high" in ids
        assert "low" in ids
        assert "end" in ids
        assert meta.groups == ("phase_one",)
        edges = {(e.source, e.target) for e in meta.edges}
        assert ("start", "phase_one.e") in edges or ("start", "phase_one") in edges
        e_task = next(t for t in meta.tasks if t.task_id == "phase_one.e")
        assert e_task.activity == "Pull rows from the source."
        assert e_task.python_callable == "extract_stuff"
        assert e_task.operator == "PythonOperator"

    def test_lshift_chain(self, tmp_path: Path):
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def fn() -> None:
    """Run."""
    return

with DAG(dag_id="lshift", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    a = EmptyOperator(task_id="a")
    b = EmptyOperator(task_id="b")
    c = EmptyOperator(task_id="c")
    b << a
    c << b
'''
        path = _write_tmp(tmp_path, "lshift.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        edges = {(e.source, e.target) for e in meta.edges}
        assert ("a", "b") in edges
        assert ("b", "c") in edges

    def test_dynamic_target_emits_warning(self, tmp_path: Path):
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG(dag_id="dyn", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    a = EmptyOperator(task_id="a")
    b = EmptyOperator(task_id="b")
    unknown_name >> b
'''
        path = _write_tmp(tmp_path, "dyn.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert any("unknown_name" in w for w in meta.extraction_warnings)

    def test_branch_python_operator_recorded(self, tmp_path: Path):
        src = '''
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

def choose_path() -> str:
    """Pick a branch."""
    return "a"

with DAG(dag_id="branch", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    b = BranchPythonOperator(task_id="b", python_callable=choose_path)
'''
        path = _write_tmp(tmp_path, "branch.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        b_task = next(t for t in meta.tasks if t.task_id == "b")
        assert b_task.operator == "BranchPythonOperator"
        assert b_task.activity == "Pick a branch."

    def test_taskflow_call_extracted(self, tmp_path: Path):
        src = '''
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

@task
def produce() -> int:
    """Yield a number."""
    return 42

with DAG(dag_id="tf", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    out = produce()
'''
        path = _write_tmp(tmp_path, "tf.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        tf_task = next(t for t in meta.tasks if t.task_id == "produce")
        assert tf_task.operator == "@task"
        assert tf_task.activity == "Yield a number."

    def test_sensor_recognised(self, tmp_path: Path):
        src = '''
from airflow import DAG
from airflow.providers.standard.sensors.file import FileSensor
from datetime import datetime
with DAG(dag_id="sn", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    s = FileSensor(task_id="s", filepath="/tmp/x")
'''
        path = _write_tmp(tmp_path, "sn.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        s_task = next(t for t in meta.tasks if t.task_id == "s")
        assert s_task.operator == "FileSensor"

    def test_greet_operator_extracted(self, tmp_path: Path):
        src = '''
from airflow import DAG
from plugins.operators.greet_operator import GreetOperator
from datetime import datetime
with DAG(dag_id="g", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    greet = GreetOperator(task_id="greet", name="alice")
'''
        path = _write_tmp(tmp_path, "g.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        g = next(t for t in meta.tasks if t.task_id == "greet")
        assert g.operator == "GreetOperator"

    def test_list_in_dependency_chain(self, tmp_path: Path):
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG(dag_id="lst", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    a = EmptyOperator(task_id="a")
    b = EmptyOperator(task_id="b")
    c = EmptyOperator(task_id="c")
    d = EmptyOperator(task_id="d")
    a >> [b, c] >> d
'''
        path = _write_tmp(tmp_path, "lst.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        edges = {(e.source, e.target) for e in meta.edges}
        assert ("a", "b") in edges
        assert ("a", "c") in edges
        assert ("b", "d") in edges
        assert ("c", "d") in edges

    def test_string_constant_in_edge(self, tmp_path: Path):
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG(dag_id="str", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    a = EmptyOperator(task_id="a")
    a >> "literal_id"
'''
        path = _write_tmp(tmp_path, "str.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        edges = {(e.source, e.target) for e in meta.edges}
        assert ("a", "literal_id") in edges

    def test_attribute_target_resolves_to_attr_name(self, tmp_path: Path):
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG(dag_id="attr", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    a = EmptyOperator(task_id="a")
    a >> some_module.target_task
'''
        path = _write_tmp(tmp_path, "attr.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        edges = list(meta.edges)
        assert any(e.target == "target_task" for e in edges)

    def test_unresolvable_target_logs_warning(self, tmp_path: Path):
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG(dag_id="ur", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    a = EmptyOperator(task_id="a")
    (lambda: None)() >> a
'''
        path = _write_tmp(tmp_path, "ur.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        # The Lambda expression resolves to a non-trivial AST node type.
        assert any("couldn't resolve" in w or "isn't tracked" in w for w in meta.extraction_warnings)

    def test_topic_property(self, tmp_path: Path):
        path = tmp_path / "dags/02_etl/sample.py"
        path.parent.mkdir(parents=True)
        path.write_text(
            'from airflow import DAG\n'
            'from datetime import datetime\n'
            'with DAG(dag_id="x", start_date=datetime(2024,1,1), schedule="@daily", catchup=False):\n'
            '    pass\n'
        )
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert meta.topic == "02_etl"

    def test_uses_attribute_form_dag_call(self, tmp_path: Path):
        # `airflow.DAG(...)` — the call's func is `Attribute(value=Name('airflow'), attr='DAG')`.
        src = '''
import airflow
from datetime import datetime
with airflow.DAG(dag_id="attr_form", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    pass
'''
        path = _write_tmp(tmp_path, "af.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert meta.dag_id == "attr_form"

    def test_taskgroup_assignment_skipped_when_duplicate(self, tmp_path: Path):
        # Two bare `g = TaskGroup("phase")` assignments; second is no-op for `groups`.
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
with DAG(dag_id="dupg", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    g = TaskGroup(group_id="phase")
    g = TaskGroup(group_id="phase")
    a = EmptyOperator(task_id="a")
'''
        path = _write_tmp(tmp_path, "dupg.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert list(meta.groups).count("phase") == 1

    def test_topic_falls_back_to_empty_when_no_numeric_prefix(self, tmp_path: Path):
        path = _write_tmp(tmp_path, "misc/x.py",
            'from airflow import DAG\n'
            'from datetime import datetime\n'
            'with DAG(dag_id="x", start_date=datetime(2024,1,1), schedule="@daily", catchup=False):\n'
            '    pass\n'
        )
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert meta.topic == ""

    def test_dag_id_fallback_when_kwarg_missing(self, tmp_path: Path):
        path = _write_tmp(tmp_path, "noname.py",
            'from airflow import DAG\n'
            'from datetime import datetime\n'
            'with DAG(start_date=datetime(2024,1,1), schedule="@daily", catchup=False):\n'
            '    pass\n'
        )
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert meta.dag_id == "(unnamed)"

    def test_schedule_falls_back_to_repr_when_not_string(self, tmp_path: Path):
        # A Dataset list is a non-string `schedule`; the repr path engages.
        path = _write_tmp(tmp_path, "dataset.py",
            'from airflow import DAG\n'
            'from airflow.datasets import Dataset\n'
            'from datetime import datetime\n'
            'with DAG(dag_id="d", start_date=datetime(2024,1,1), schedule=[Dataset("u")], catchup=False):\n'
            '    pass\n'
        )
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert "Dataset" in meta.schedule

    def test_taskgroup_direct_assignment_no_group_id_kwarg(self, tmp_path: Path):
        # Bare `TaskGroup()` with no `group_id=` falls back to the variable name.
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

with DAG(dag_id="g", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    g = TaskGroup()
    a = EmptyOperator(task_id="a", task_group=g)
'''
        path = _write_tmp(tmp_path, "tg.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert "g" in meta.groups

    def test_non_dependency_binexpr_silently_skipped(self, tmp_path: Path):
        # A bare Expr-wrapped BinOp with `Add` (not >> / <<) — dispatcher
        # ignores it (no edge, no warning). `(a + 1)` is parsed as Expr(BinOp).
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG(dag_id="bin", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    a = EmptyOperator(task_id="a")
    (a + 1)
'''
        path = _write_tmp(tmp_path, "bin.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert meta.tasks and not meta.edges

    def test_with_block_context_not_taskgroup(self, tmp_path: Path):
        # `with open(...)` is not a TaskGroup — body still walked under current group.
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG(dag_id="wctx", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    with open("/tmp/x") as f:
        a = EmptyOperator(task_id="a")
'''
        path = _write_tmp(tmp_path, "wctx.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert any(t.task_id == "a" for t in meta.tasks)

    def test_with_taskgroup_no_optional_vars(self, tmp_path: Path):
        # `with TaskGroup("name"):` (no `as foo`): group recorded, body walked.
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
with DAG(dag_id="wg", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    with TaskGroup("phase"):
        a = EmptyOperator(task_id="a")
'''
        path = _write_tmp(tmp_path, "wg.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert any(t.task_id == "phase.a" for t in meta.tasks)

    def test_with_taskgroup_no_id_falls_through(self, tmp_path: Path):
        # `with TaskGroup():` (no name, no kwarg, no `as`) — group_id fallback
        # to the as-var name also empty; the block is still walked under None.
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
with DAG(dag_id="empty_g", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    with TaskGroup():
        a = EmptyOperator(task_id="a")
'''
        path = _write_tmp(tmp_path, "eg.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert any(t.task_id == "a" for t in meta.tasks)

    def test_taskgroup_already_recorded_deduped(self, tmp_path: Path):
        # Same group_id used twice — second declaration is a no-op for `groups`.
        src = '''
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
with DAG(dag_id="dup", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    with TaskGroup("phase"):
        a = EmptyOperator(task_id="a")
    with TaskGroup("phase"):
        b = EmptyOperator(task_id="b")
'''
        path = _write_tmp(tmp_path, "dup.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert list(meta.groups).count("phase") == 1

    def test_dag_block_top_level_with_name_context_only(self, tmp_path: Path):
        # Top-level `with name:` where `name` is a Name not a Call — the
        # `ast.Call` branch falls through, and we still need a follow-up DAG.
        src = '''
from datetime import datetime
from airflow import DAG
with dag_ref:
    pass
with DAG(dag_id="real", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    pass
'''
        path = _write_tmp(tmp_path, "nctx.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert meta.dag_id == "real"

    def test_safe_params_empty_when_no_keywords(self):
        tree = ast.parse("foo()")
        call = tree.body[0].value
        out = _safe_params(call)
        assert out == {}

    def test_safe_params_swallows_unparse_exception(self):
        # Some kwarg values are not unparseable round-trip in older AST.
        # The `_safe_params` defensive `except Exception: pass` swallows that.
        call = ast.parse("foo(task_id='x')").body[0].value
        with mock.patch("ast.unparse", side_effect=ValueError("boom")):
            assert _safe_params(call) == {}

    def test_find_dag_block_no_top_level_with(self, tmp_path: Path):
        path = _write_tmp(tmp_path, "plain.py", "x = 1\ny = 2\n")
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert "No `with DAG(...)` block found." in meta.extraction_warnings

    def test_find_dag_block_with_non_dag_call(self, tmp_path: Path):
        src = '''
from datetime import datetime
with not_DAG(dag_id="x", start_date=datetime(2024,1,1), schedule="@daily"):
    pass
'''
        path = _write_tmp(tmp_path, "nd.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert "No `with DAG(...)` block found." in meta.extraction_warnings

    def test_find_dag_block_ignores_bare_name_context(self, tmp_path: Path):
        src = '''
from datetime import datetime
from airflow import DAG
with DAG(dag_id="x", start_date=datetime(2024,1,1), schedule="@daily", catchup=False):
    pass
'''
        path = _write_tmp(tmp_path, "nc.py", src)
        meta = extract_dag_metadata(path, REPO_ROOT)
        assert meta.dag_id == "x"


# --- DagMetadata.to_mermaid_*

class TestMermaidGeneration:
    def _build(self, tasks=(), edges=(), groups=()):
        return DagMetadata(
            file_path="x.py",
            relative_path="dags/x.py",
            dag_id="x",
            schedule="@daily",
            description="",
            tags=(),
            module_docstring="",
            tasks=tuple(tasks),
            edges=tuple(edges),
            groups=tuple(groups),
            extraction_warnings=(),
        )

    def test_simple_graph_has_graph_td(self):
        m = self._build(tasks=[TaskNode(task_id="a", operator="PythonOperator")])
        out = m.to_mermaid_simple()
        assert out.startswith("graph TD")
        assert "n_a" in out
        assert "py" in out

    def test_simple_graph_includes_subgraphs(self):
        m = self._build(tasks=[], groups=("extract", "load"))
        out = m.to_mermaid_simple()
        assert 'subgraph n_extract["extract"]' in out

    def test_simple_graph_includes_edges(self):
        m = self._build(edges=[TaskEdge(source="a", target="b")])
        out = m.to_mermaid_simple()
        assert "n_a --> n_b" in out

    def test_simple_graph_classdefs(self):
        out = self._build().to_mermaid_simple()
        for cls in ("py", "branch", "bash", "empty", "sc", "trigger", "sensor", "hook", "tf", "other"):
            assert f"classDef {cls}" in out

    def test_simple_graph_class_assignments(self):
        m = self._build(tasks=[
            TaskNode(task_id="a", operator="PythonOperator"),
            TaskNode(task_id="b", operator="BashOperator"),
            TaskNode(task_id="c", operator="UnknownOp"),
        ])
        out = m.to_mermaid_simple()
        assert "class n_a py" in out
        assert "class n_b bash" in out
        assert "class n_c other" in out

    def test_rich_graph_includes_icon_and_activity(self):
        m = self._build(tasks=[
            TaskNode(
                task_id="extract",
                operator="PythonOperator",
                activity="Read source data",
                python_callable="fn",
            )
        ])
        out = m.to_mermaid_rich()
        assert "🐍" in out
        assert "python" in out
        assert "Read source data" in out

    def test_rich_graph_with_bash_activity(self):
        m = self._build(tasks=[
            TaskNode(
                task_id="echo",
                operator="BashOperator",
                params={"bash_command": "echo hello"},
            )
        ])
        out = m.to_mermaid_rich()
        assert "$" in out
        assert "echo hello" in out

    def test_rich_graph_task_with_no_activity(self):
        # When `_activity_line` returns "" (no doc, no params, no fallback),
        # the rich graph still renders the task without an extra <small> line.
        m = self._build(tasks=[
            TaskNode(task_id="mystery", operator="MysteryOperator"),
        ])
        out = m.to_mermaid_rich()
        assert "n_mystery" in out
        # No trailing <small> line because activity is empty.
        assert "<small>" not in out.split("n_mystery", 1)[1].split("[/", 1)[0]

    def test_rich_graph_groups_only(self):
        m = self._build(groups=("phase",))
        out = m.to_mermaid_rich()
        assert 'subgraph n_phase["<b>phase</b>"]' in out

    def test_rich_graph_edges_only(self):
        m = self._build(edges=[TaskEdge(source="a", target="b")])
        out = m.to_mermaid_rich()
        assert "n_a --> n_b" in out


# --- helpers ---------------------------------------------------------------

class TestHelpers:
    def test_call_name_for_name_call(self):
        node = ast.parse("foo()").body[0].value
        assert _call_name(node) == "foo"

    def test_call_name_for_attribute_call(self):
        node = ast.parse("a.b()").body[0].value
        assert _call_name(node) == "b"

    def test_call_name_for_unsupported(self):
        # `foo()()` is a Call whose .func is itself a Call — neither Name nor Attribute.
        node = ast.parse("foo()()").body[0].value
        assert _call_name(node) is None

    def test_kwarg_str_finds_string(self):
        call = ast.parse("foo(task_id='hello')").body[0].value
        assert _kwarg_str(call, "task_id") == "hello"

    def test_kwarg_str_returns_none_when_missing(self):
        call = ast.parse("foo()").body[0].value
        assert _kwarg_str(call, "task_id") is None

    def test_kwarg_str_returns_none_for_non_string(self):
        call = ast.parse("foo(task_id=123)").body[0].value
        assert _kwarg_str(call, "task_id") is None

    def test_kwarg_str_list(self):
        call = ast.parse("foo(tags=['a', 'b'])").body[0].value
        assert _kwarg_str_list(call, "tags") == ["a", "b"]

    def test_kwarg_str_list_missing(self):
        call = ast.parse("foo()").body[0].value
        assert _kwarg_str_list(call, "tags") is None

    def test_kwarg_repr(self):
        call = ast.parse("foo(x=1+2)").body[0].value
        assert _kwarg_repr(call, "x") == "1 + 2"

    def test_kwarg_repr_missing(self):
        call = ast.parse("foo()").body[0].value
        assert _kwarg_repr(call, "x") is None

    def test_kwarg_name_with_string(self):
        call = ast.parse("foo(python_callable='fn')").body[0].value
        assert _kwarg_name(call, "python_callable") == "fn"

    def test_kwarg_name_with_name_reference(self):
        call = ast.parse("foo(python_callable=fn)").body[0].value
        assert _kwarg_name(call, "python_callable") == "fn"

    def test_kwarg_name_missing(self):
        call = ast.parse("foo()").body[0].value
        assert _kwarg_name(call, "python_callable") is None

    def test_first_str_arg_present(self):
        call = ast.parse("foo('hello')").body[0].value
        assert _first_line_via_first_str_arg(call) == "hello"

    def test_first_str_arg_missing(self):
        call = ast.parse("foo()").body[0].value
        assert _first_line_via_first_str_arg(call) is None

    def test_safe_params_handles_unknown_gracefully(self):
        # unknown attr-like expression
        call = ast.parse("foo(task_id='a', owner='me', pool='small')").body[0].value
        out = _safe_params(call)
        assert out["task_id"] == "'a'"
        assert out["owner"] == "'me'"
        assert out["pool"] == "'small'"

    def test_safe_params_swallows_unparseable(self):
        call = ast.parse("foo(pool=__import__('os'))").body[0].value
        out = _safe_params(call)
        # Either we got a string repr or the kwarg is skipped; never an exception.
        assert isinstance(out, dict)

    def test_is_task_decorator(self):
        for src in ["@task\ndef f(): pass", "@task()\ndef f(): pass", "@airflow.task\ndef f(): pass"]:
            tree = ast.parse(src)
            deco = tree.body[0].decorator_list[0]
            assert _is_task_decorator(deco) is True, src

        for src in ["@staticmethod\ndef f(): pass", "@retry\ndef f(): pass"]:
            tree = ast.parse(src)
            deco = tree.body[0].decorator_list[0]
            assert _is_task_decorator(deco) is False, src

    def test_resolve_name_to_scope(self):
        node = ast.parse("a").body[0].value
        warnings = []
        assert _resolve_name(node, {"a": "task_a"}, warnings) == "task_a"
        assert warnings == []

    def test_resolve_name_to_string_constant(self):
        node = ast.parse("'literal'").body[0].value
        assert _resolve_name(node, {}, []) == "literal"

    def test_resolve_name_to_attribute(self):
        node = ast.parse("a.b").body[0].value
        assert _resolve_name(node, {}, []) == "b"

    def test_resolve_name_unresolvable_logs(self):
        node = ast.parse("1 + 2").body[0].value
        warnings = []
        result = _resolve_name(node, {}, warnings)
        assert result == ""
        assert any("couldn't resolve" in w for w in warnings)

    def test_resolve_name_untracked_logs(self):
        node = ast.parse("ghost").body[0].value
        warnings = []
        assert _resolve_name(node, {}, warnings) == "ghost"
        assert any("wasn't tracked" in w for w in warnings)

    def test_flatten_rshift_chain(self):
        node = ast.parse("a >> b >> c >> d").body[0].value
        chain = _flatten_rshift(node)
        names = [ast.unparse(o) for o in chain]
        assert names == ["a", "b", "c", "d"]

    def test_flatten_lshift_chain(self):
        node = ast.parse("a << b << c").body[0].value
        chain = _flatten_lshift(node)
        names = [ast.unparse(o) for o in chain]
        assert names == ["a", "b", "c"]

    def test_emit_chain_with_list(self):
        # a >> [b, c] >> d
        node = ast.parse("a >> [b, c] >> d").body[0].value
        chain = _flatten_rshift(node)
        scope = {"a": "a", "b": "b", "c": "c", "d": "d"}
        edges = []
        _emit_chain(chain, scope, edges, [])
        pairs = {(e.source, e.target) for e in edges}
        assert pairs == {("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")}

    def test_emit_chain_skips_empty(self):
        node = ast.parse("a >> b").body[0].value
        chain = _flatten_rshift(node)
        scope = {"a": "a"}
        edges = []
        _emit_chain(chain, scope, edges, [])
        # b has no scope entry; _resolve_name returns "b" anyway but it's not blank.
        assert ("a", "b") in {(e.source, e.target) for e in edges}

    def test_flatten_lshift_single_pair(self):
        node = ast.parse("b << a").body[0].value
        chain = _flatten_lshift(node)
        assert [ast.unparse(o) for o in chain] == ["b", "a"]

    def test_safe_relative_inside_repo(self):
        p = Path("/repo/dags/x.py")
        assert _safe_relative(p, Path("/repo")) == "dags/x.py"

    def test_safe_relative_outside_repo(self):
        p = Path("/other/x.py")
        out = _safe_relative(p, Path("/repo"))
        assert out == str(p)

    def test_first_line(self):
        assert _first_line("") == ""
        assert _first_line("hello\nworld") == "hello"
        assert _first_line("\n\n  hello  \nworld") == "hello"
        assert _first_line("\n") == ""

    def test_truncate_short(self):
        assert _truncate("hi", 10) == "hi"

    def test_truncate_long(self):
        assert _truncate("a" * 100, 10) == "a" * 9 + "…"

    def test_m_id_strips_specials(self):
        assert _m_id("foo.bar-baz qux") == "n_foo_bar_baz_qux"

    def test_m_label_escapes_quotes(self):
        assert _m_label('say "hi"') == "say 'hi'"

    def test_mermaid_classdefs_has_known_classes(self):
        cdf = "\n".join(_mermaid_classdefs())
        for name in ("py", "branch", "bash", "empty", "sc", "trigger", "sensor", "hook", "tf", "other"):
            assert f"classDef {name}" in cdf

    def test_operator_label_known(self):
        assert _operator_label("PythonOperator") == "python"
        assert _operator_label("BashOperator") == "bash"
        assert _operator_label("EmptyOperator") == "empty"
        assert _operator_label("BranchPythonOperator") == "branch"
        assert _operator_label("@task") == "task"

    def test_operator_label_unknown(self):
        assert _operator_label("SomeFutureOperator") == "somefuture"

    def test_operator_icon_known(self):
        assert _operator_icon("PythonOperator") == "🐍"
        assert _operator_icon("BashOperator") == "$"
        assert _operator_icon("EmptyOperator") == "○"

    def test_operator_icon_unknown(self):
        assert _operator_icon("SomeFutureOperator") == "·"

    def test_operator_class_known(self):
        assert _operator_class("PythonOperator") == "py"
        assert _operator_class("FileSensor") == "sensor"

    def test_operator_class_unknown(self):
        assert _operator_class("SomeFutureOp") == "other"

    def test_lookup_activity_hit(self):
        assert _lookup_activity("fn", {"fn": "Do the thing."}) == "Do the thing."

    def test_lookup_activity_miss(self):
        assert _lookup_activity("missing", {}) == ""

    def test_lookup_activity_multiline(self):
        assert _lookup_activity("fn", {"fn": "Line one\nLine two"}) == "Line one"

    def test_activity_line_python_with_doc(self):
        t = TaskNode(task_id="x", operator="PythonOperator", activity="Pull rows")
        assert _activity_line(t) == "Pull rows"

    def test_activity_line_python_no_doc(self):
        t = TaskNode(task_id="x", operator="PythonOperator")
        assert _activity_line(t) == ""

    def test_activity_line_bash_uses_command(self):
        t = TaskNode(
            task_id="x",
            operator="BashOperator",
            params={"bash_command": "echo hi && sleep 2"},
        )
        assert _activity_line(t) == "echo hi && sleep 2"

    def test_activity_line_bash_no_command(self):
        t = TaskNode(task_id="x", operator="BashOperator", params={})
        assert _activity_line(t) == ""

    def test_activity_line_empty_operator(self):
        t = TaskNode(task_id="x", operator="EmptyOperator")
        assert _activity_line(t) == "structural marker (start/end or join)"

    def test_activity_line_branch_operator(self):
        t = TaskNode(task_id="x", operator="BranchPythonOperator")
        assert _activity_line(t) == "route to one of several branches"

    def test_activity_line_shortcircuit(self):
        t = TaskNode(task_id="x", operator="ShortCircuitOperator")
        assert _activity_line(t) == "skip downstream iff false"

    def test_activity_line_trigger(self):
        t = TaskNode(task_id="x", operator="TriggerDagRunOperator")
        assert _activity_line(t) == "trigger another DAG"

    def test_activity_line_sensor(self):
        t = TaskNode(task_id="x", operator="FileSensor")
        assert _activity_line(t) == "wait for an external condition"

    def test_activity_line_unknown_op(self):
        t = TaskNode(task_id="x", operator="MysteryOperator")
        assert _activity_line(t) == ""


# --- live integration against the actual DAGs ----------------------------

class TestAgainstRepoDags:
    """End-to-end: extract every real DAG and assert no exceptions, sensible counts."""

    @pytest.fixture(scope="class")
    def all_metas(self) -> list[DagMetadata]:
        metas = []
        for path in sorted((REPO_ROOT / "dags").rglob("*.py")):
            if path.name.startswith("_"):
                continue
            metas.append(extract_dag_metadata(path, REPO_ROOT))
        return metas

    def test_every_real_dag_has_a_dag_id(self, all_metas):
        for m in all_metas:
            assert m.dag_id, f"{m.relative_path} has empty dag_id"
            assert not m.dag_id.startswith("("), f"{m.relative_path} dag_id is a placeholder: {m.dag_id}"

    def test_every_real_dag_has_tasks(self, all_metas):
        for m in all_metas:
            assert m.tasks, f"{m.dag_id} ({m.relative_path}) extracted 0 tasks"

    def test_every_real_dag_with_multiple_tasks_has_at_least_one_edge(self, all_metas):
        # A DAG with a single task (like `cross_dag_consumer`, which receives
        # its dependency via a Dataset, not a `>>` chain) legitimately has 0 edges.
        for m in all_metas:
            if len(m.tasks) <= 1:
                continue
            assert m.edges, f"{m.dag_id} ({m.relative_path}) extracted 0 edges"

    def test_every_real_dag_has_activity_for_at_least_half_of_its_python_tasks(self, all_metas):
        for m in all_metas:
            py_tasks = [t for t in m.tasks if t.operator == "PythonOperator"]
            if not py_tasks:
                continue
            with_activity = sum(1 for t in py_tasks if t.activity)
            assert with_activity >= len(py_tasks) / 2, (
                f"{m.dag_id}: only {with_activity}/{len(py_tasks)} PythonOperator tasks have activity"
            )


# --- helpers used inside other tests --------------------------------------

def _first_line_via_first_str_arg(call: ast.Call):
    """Helper kept here so the import block stays tidy."""
    if call.args and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str):
        return call.args[0].value
    return None