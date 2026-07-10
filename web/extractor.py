"""DAG metadata extractor — read a DAG file via AST and return a structured description.

Why AST and not import? Importing a DAG triggers Airflow's DAG-bag discovery plus
any side effects defined in the file. AST is side-effect-free and works on any
Python file, even when Airflow is not installed (which is exactly when most
readers will hit the web UI).

The extractor is *best-effort*: if a pattern is too dynamic to be discovered
statically (e.g. TaskFlow ``.expand()``, branch returns that resolve to
downstream task_ids), the extractor logs a warning and the UI shows the
source for that task group.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TaskNode:
    task_id: str
    operator: str
    group: str | None = None
    line: int = 0
    params: dict[str, Any] = field(default_factory=dict)
    activity: str = ""             # docstring of the python_callable, if any
    python_callable: str = ""      # name of the function that backs this task


@dataclass(frozen=True)
class TaskEdge:
    source: str
    target: str


@dataclass(frozen=True)
class DagMetadata:
    file_path: str
    relative_path: str
    dag_id: str
    schedule: str
    description: str
    tags: tuple[str, ...]
    module_docstring: str
    tasks: tuple[TaskNode, ...]
    edges: tuple[TaskEdge, ...]
    groups: tuple[str, ...]
    extraction_warnings: tuple[str, ...]

    @property
    def topic(self) -> str:
        """Derive the topic folder (01_basics, 02_etl, ...) from the path."""
        parts = Path(self.relative_path).parts
        for p in parts:
            if "_" in p and p[:2].isdigit():
                return p
        return ""

    def to_mermaid_simple(self) -> str:
        """Compact graph for index cards: just task_id + operator."""
        out: list[str] = ["graph TD"]
        for t in self.tasks:
            tid = _m_id(t.task_id)
            op = _operator_label(t.operator)
            out.append(f'    {tid}["<b>{_m_label(t.task_id)}</b><br/><small>{op}</small>"]')
        for g in self.groups:
            out.append(f'    subgraph {_m_id(g)}["{g}"]')
            out.append(f'    direction LR')
            out.append(f'    end')
        for e in self.edges:
            out.append(f'    {_m_id(e.source)} --> {_m_id(e.target)}')
        out.extend(_mermaid_classdefs())
        for t in self.tasks:
            out.append(f'    class {_m_id(t.task_id)} {_operator_class(t.operator)}')
        return "\n".join(out)

    def to_mermaid_rich(self) -> str:
        """Detailed graph for the per-DAG page: icon, task_id, operator, activity.

        Activity comes from the ``python_callable`` docstring (truncated to one
        line). For non-Python tasks, the operator-specific param is shown
        (``bash_command`` for BashOperator, etc.).
        """
        out: list[str] = ["graph TD"]
        for t in self.tasks:
            tid = _m_id(t.task_id)
            icon = _operator_icon(t.operator)
            op_short = _operator_label(t.operator)
            lines = [f"{icon} <b>{_m_label(t.task_id)}</b>", f"<i>{op_short}</i>"]
            sub = _activity_line(t)
            if sub:
                lines.append(f"<small>{_m_label(sub)}</small>")
            out.append(f'    {tid}["{"<br/>".join(lines)}"]')
        for g in self.groups:
            out.append(f'    subgraph {_m_id(g)}["<b>{_m_label(g)}</b>"]')
            out.append(f'    direction LR')
            out.append(f'    end')
        for e in self.edges:
            out.append(f'    {_m_id(e.source)} --> {_m_id(e.target)}')
        out.extend(_mermaid_classdefs())
        for t in self.tasks:
            out.append(f'    class {_m_id(t.task_id)} {_operator_class(t.operator)}')
        return "\n".join(out)


# --- public entry point ----------------------------------------------

def extract_dag_metadata(path: Path, repo_root: Path) -> DagMetadata:
    text = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError as exc:
        return _empty(
            path,
            repo_root,
            dag_id="(syntax error)",
            warnings=(f"Could not parse {path.name}: {exc}",),
        )

    module_doc = ast.get_docstring(tree) or ""
    dag_block, dag_call = _find_dag_block(tree)
    if dag_block is None or dag_call is None:
        return _empty(
            path,
            repo_root,
            warnings=("No `with DAG(...)` block found.",),
            module_doc=module_doc,
        )

    # Track TaskFlow-decorated functions anywhere in the file. We scan the
    # entire tree because DAGs often define @task functions inside the
    # `with DAG(...)` body or inside a nested TaskGroup, not at module scope.
    tf_funcs: set[str] = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and any(_is_task_decorator(d) for d in node.decorator_list)
    }

    # Build a {fn_name: docstring} map for any PythonOperator that references
    # a function via python_callable=<name>. We scan the *entire* tree because
    # some DAGs nest their callables inside the `with DAG(...)` body rather
    # than at module scope.
    fn_docs: dict[str, str] = {
        node.name: (ast.get_docstring(node) or "")
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    }

    dag_id = _kwarg_str(dag_call, "dag_id") or "(unnamed)"
    schedule = _kwarg_str(dag_call, "schedule") or _kwarg_repr(dag_call, "schedule") or ""
    description = _kwarg_str(dag_call, "description") or ""
    tags = tuple(_kwarg_str_list(dag_call, "tags") or ())

    tasks: list[TaskNode] = []
    edges: list[TaskEdge] = []
    groups: list[str] = []
    warnings: list[str] = []

    scope: dict[str, str] = {}
    _walk(
        block=dag_block.body,
        group=None,
        scope=scope,
        tf_funcs=tf_funcs,
        fn_docs=fn_docs,
        tasks=tasks,
        edges=edges,
        groups=groups,
        warnings=warnings,
    )

    return DagMetadata(
        file_path=str(path),
        relative_path=_safe_relative(path, repo_root),
        dag_id=dag_id,
        schedule=schedule,
        description=description,
        tags=tags,
        module_docstring=module_doc,
        tasks=tuple(tasks),
        edges=tuple(edges),
        groups=tuple(groups),
        extraction_warnings=tuple(warnings),
    )


def _safe_relative(path: Path, repo_root: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


# --- walker ---------------------------------------------------------

def _walk(
    *,
    block: list[ast.stmt],
    group: str | None,
    scope: dict[str, str],
    tf_funcs: set[str],
    fn_docs: dict[str, str],
    tasks: list[TaskNode],
    edges: list[TaskEdge],
    groups: list[str],
    warnings: list[str],
) -> None:
    for stmt in block:
        # `name = Operator(...)`  OR  `name: Type = Operator(...)`
        if (
            (isinstance(stmt, ast.Assign) and len(stmt.targets) == 1)
            or isinstance(stmt, ast.AnnAssign)
        ):
            if isinstance(stmt, ast.Assign):
                tgt = stmt.targets[0]
                value = stmt.value
            else:
                tgt = stmt.target
                value = stmt.value
            if isinstance(tgt, ast.Name) and isinstance(value, ast.Call):
                call = value
                op_name = _call_name(call)
                if op_name and (op_name.endswith("Operator") or op_name.endswith("Sensor")):
                    task_id = _kwarg_str(call, "task_id") or tgt.id
                    full_id = f"{group}.{task_id}" if group else task_id
                    callable_name = _kwarg_name(call, "python_callable")
                    activity = _lookup_activity(callable_name, fn_docs) if callable_name else ""
                    tasks.append(
                        TaskNode(
                            task_id=full_id,
                            operator=op_name,
                            group=group,
                            line=call.lineno,
                            params=_safe_params(call),
                            activity=activity,
                            python_callable=callable_name or "",
                        )
                    )
                    scope[tgt.id] = full_id
                    continue
                if op_name == "TaskGroup":
                    grp_id = _kwarg_str(call, "group_id") or _first_str_arg(call) or tgt.id
                    if grp_id and grp_id not in groups:
                        groups.append(grp_id)
                    scope[tgt.id] = grp_id
                    continue
                if (
                    isinstance(call.func, ast.Name)
                    and call.func.id in tf_funcs
                ):
                    tf_id = call.func.id
                    full_id = f"{group}.{tf_id}" if group else tf_id
                    tasks.append(
                        TaskNode(
                            task_id=full_id,
                            operator="@task",
                            group=group,
                            line=call.lineno,
                            activity=fn_docs.get(tf_id, ""),
                        )
                    )
                    scope[tgt.id] = full_id
                    continue

        # `a >> b >> c`  or  `a << b`  or `[a, b] >> c`  (incl. nested lists)
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.BinOp):
            op_node = stmt.value
            if isinstance(op_node.op, ast.RShift):
                _emit_chain(_flatten_rshift(op_node), scope, edges, warnings)
                continue
            if isinstance(op_node.op, ast.LShift):
                # LShift means "right runs first", so reverse before emitting.
                _emit_chain(
                    list(reversed(_flatten_lshift(op_node))),
                    scope,
                    edges,
                    warnings,
                )
                continue

        # `with TaskGroup("name") as g: ...`
        if isinstance(stmt, ast.With):
            for item in stmt.items:
                ctx = item.context_expr
                if isinstance(ctx, ast.Call) and _call_name(ctx) == "TaskGroup":
                    grp_id = (
                        _kwarg_str(ctx, "group_id")
                        or _first_str_arg(ctx)
                        or (item.optional_vars.id if isinstance(item.optional_vars, ast.Name) else "")
                    )
                    if grp_id and grp_id not in groups:
                        groups.append(grp_id)
                    if isinstance(item.optional_vars, ast.Name):
                        scope[item.optional_vars.id] = grp_id
                    _walk(
                        block=stmt.body,
                        group=grp_id or None,
                        scope=scope,
                        tf_funcs=tf_funcs,
                        fn_docs=fn_docs,
                        tasks=tasks,
                        edges=edges,
                        groups=groups,
                        warnings=warnings,
                    )
                    break
            else:
                _walk(
                    block=stmt.body,
                    group=group,
                    scope=scope,
                    tf_funcs=tf_funcs,
                    fn_docs=fn_docs,
                    tasks=tasks,
                    edges=edges,
                    groups=groups,
                    warnings=warnings,
                )
            continue

        # If we hit something we don't recognise, just continue.
        # The extraction is best-effort; warnings stay empty unless we
        # explicitly notice a dynamic pattern.


# --- helpers ---------------------------------------------------------

def _empty(
    path: Path,
    repo_root: Path,
    *,
    dag_id: str = "(no DAG)",
    warnings: tuple[str, ...] = (),
    module_doc: str = "",
) -> DagMetadata:
    try:
        rel = path.relative_to(repo_root)
    except ValueError:
        rel = path  # test fixtures may live outside the repo
    return DagMetadata(
        file_path=str(path),
        relative_path=str(rel),
        dag_id=dag_id,
        schedule="",
        description="",
        tags=(),
        module_docstring=module_doc,
        tasks=(),
        edges=(),
        groups=(),
        extraction_warnings=warnings,
    )


def _find_dag_block(tree: ast.Module) -> tuple[ast.With | None, ast.Call | None]:
    for node in tree.body:
        if isinstance(node, ast.With):
            for item in node.items:
                ctx = item.context_expr
                if isinstance(ctx, ast.Call):
                    func = ctx.func
                    if isinstance(func, ast.Name) and func.id == "DAG":
                        return node, ctx
                    if isinstance(func, ast.Attribute) and func.attr == "DAG":
                        # e.g. airflow.sdk.DAG(...) — accept it.
                        return node, ctx
    return None, None


def _call_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _kwarg_str(call: ast.Call, name: str) -> str | None:
    for kw in call.keywords:
        if kw.arg == name and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
            return kw.value.value
    return None


def _kwarg_name(call: ast.Call, name: str) -> str | None:
    """Resolve a kwarg to either a string literal or a Name reference.

    Used for things like ``python_callable=my_func`` where the value is a
    function reference, not a literal string.
    """
    for kw in call.keywords:
        if kw.arg != name:
            continue
        v = kw.value
        if isinstance(v, ast.Constant) and isinstance(v.value, str):
            return v.value
        if isinstance(v, ast.Name):
            return v.id
    return None


def _kwarg_str_list(call: ast.Call, name: str) -> list[str] | None:
    for kw in call.keywords:
        if kw.arg == name and isinstance(kw.value, ast.List):
            return [
                elt.value
                for elt in kw.value.elts
                if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
            ]
    return None


def _kwarg_repr(call: ast.Call, name: str) -> str | None:
    """Best-effort string representation of a kwarg for display."""
    for kw in call.keywords:
        if kw.arg == name:
            return ast.unparse(kw.value)
    return None


def _first_str_arg(call: ast.Call) -> str | None:
    if call.args and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str):
        return call.args[0].value
    return None


def _safe_params(call: ast.Call) -> dict[str, str]:
    """Pull out a small set of operator kwargs as strings (for display only)."""
    out: dict[str, str] = {}
    for kw in call.keywords:
        if kw.arg in {"task_id", "owner", "pool", "trigger_rule", "retries", "catchup"}:
            try:
                out[kw.arg] = ast.unparse(kw.value)
            except Exception:
                pass
    return out


def _is_task_decorator(d: ast.expr) -> bool:
    if isinstance(d, ast.Name) and d.id == "task":
        return True
    if isinstance(d, ast.Attribute) and d.attr == "task":
        return True
    if isinstance(d, ast.Call) and _call_name(d) == "task":
        return True
    return False


def _flatten_rshift(node: ast.BinOp) -> list[ast.expr]:
    """Flatten a left-associative `a >> b >> c` into `[a, b, c]`."""
    ops: list[ast.expr] = []
    while isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
        ops.append(node.right)
        node = node.left
    ops.append(node)
    ops.reverse()
    return ops


def _flatten_lshift(node: ast.BinOp | ast.expr) -> list[ast.expr]:
    """Flatten ``a << b << c`` (left-assoc) into ``[a, b, c]`` (in source order).

    Direction is **not** baked in here: this returns operands as written.
    Callers that want edges must reverse the chain for LShift semantics, since
    ``a << b`` means ``b -> a`` (right operand runs first).
    """
    items: list[ast.expr] = []

    def _walk(n: ast.expr) -> None:
        if isinstance(n, ast.BinOp) and isinstance(n.op, ast.LShift):
            _walk(n.left)
            _walk(n.right)
        else:
            items.append(n)

    _walk(node)
    return items


def _emit_chain(
    chain: list[ast.expr],
    scope: dict[str, str],
    edges: list[TaskEdge],
    warnings: list[str],
) -> None:
    """Emit edges for a chain of operands `[a, b, c, d]` meaning a->b->c->d.

    Lists anywhere in the chain expand: `[a, b] >> c` becomes a->c, b->c; and
    `a >> [b, c]` becomes a->b, a->c.
    """
    resolved: list[list[str] | str] = []
    for op in chain:
        if isinstance(op, ast.List):
            resolved.append([_resolve_name(e, scope, warnings) for e in op.elts])
        else:
            resolved.append(_resolve_name(op, scope, warnings))

    for i in range(len(resolved) - 1):
        src = resolved[i]
        dst = resolved[i + 1]
        sources = src if isinstance(src, list) else [src]
        targets = dst if isinstance(dst, list) else [dst]
        for s in sources:
            for t in targets:
                if s and t:
                    edges.append(TaskEdge(source=s, target=t))


def _resolve_name(
    node: ast.expr,
    scope: dict[str, str],
    warnings: list[str],
) -> str:
    if isinstance(node, ast.Name):
        if node.id in scope:
            return scope[node.id]
        warnings.append(
            f"line {node.lineno}: edge references `{node.id}` which wasn't tracked; "
            "may be a dynamic or branch-returned task id"
        )
        return node.id
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Attribute):
        return node.attr
    warnings.append(f"line {node.lineno}: couldn't resolve edge endpoint of type {type(node).__name__}")
    return ""


def _m_id(s: str) -> str:
    """Mermaid node ids must be alphanumeric with underscores."""
    return "n_" + "".join(c if c.isalnum() else "_" for c in s)


def _m_label(s: str) -> str:
    return s.replace('"', "'")


# --- activity lookup -------------------------------------------------

def _lookup_activity(callable_name: str, fn_docs: dict[str, str]) -> str:
    """Resolve a `python_callable=` name to its function docstring (one line)."""
    doc = fn_docs.get(callable_name, "")
    return _first_line(doc)


def _activity_line(t: TaskNode) -> str:
    """What to show under a task's name in the graph. Truncated to ~80 chars."""
    if t.activity:
        return _first_line(t.activity)
    if t.operator == "BashOperator":
        cmd = t.params.get("bash_command", "")
        if cmd:
            return _truncate(cmd, 80)
    if t.operator == "EmptyOperator":
        return "structural marker (start/end or join)"
    if t.operator == "BranchPythonOperator":
        return "route to one of several branches"
    if t.operator == "ShortCircuitOperator":
        return "skip downstream iff false"
    if t.operator == "TriggerDagRunOperator":
        return "trigger another DAG"
    if t.operator.endswith("Sensor"):
        return "wait for an external condition"
    return ""


def _first_line(s: str) -> str:
    for line in s.splitlines():
        line = line.strip()
        if line:
            return _truncate(line, 80)
    return ""


def _truncate(s: str, n: int) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


# --- operator presentation ------------------------------------------

_OPERATOR_PRESETS: dict[str, dict[str, str]] = {
    "PythonOperator":        {"icon": "🐍", "label": "python",  "cls": "py"},
    "BranchPythonOperator":  {"icon": "🌿", "label": "branch",  "cls": "branch"},
    "EmptyOperator":         {"icon": "○",  "label": "empty",   "cls": "empty"},
    "BashOperator":          {"icon": "$",  "label": "bash",    "cls": "bash"},
    "ShortCircuitOperator":  {"icon": "⊘",  "label": "shortc.", "cls": "sc"},
    "TriggerDagRunOperator": {"icon": "⚡", "label": "trigger", "cls": "trigger"},
    "FileSensor":            {"icon": "📁", "label": "sensor",  "cls": "sensor"},
    "TimeSensorAsync":       {"icon": "⏰", "label": "sensor",  "cls": "sensor"},
    "TimeDeltaSensorAsync":  {"icon": "⏰", "label": "sensor",  "cls": "sensor"},
    "DayOfWeekSensor":       {"icon": "📅", "label": "sensor",  "cls": "sensor"},
    "SqliteHook":            {"icon": "🗄", "label": "hook",    "cls": "hook"},
    "@task":                 {"icon": "ƒ",  "label": "task",    "cls": "tf"},
}


def _operator_label(op: str) -> str:
    preset = _OPERATOR_PRESETS.get(op, {"label": op.replace("Operator", "").lower()})
    return preset["label"]


def _operator_icon(op: str) -> str:
    preset = _OPERATOR_PRESETS.get(op, {"icon": "·"})
    return preset["icon"]


def _operator_class(op: str) -> str:
    preset = _OPERATOR_PRESETS.get(op, {"cls": "other"})
    return preset["cls"]


def _mermaid_classdefs() -> list[str]:
    """The classDef directives that go at the end of a Mermaid graph."""
    return [
        "    classDef py fill:#dbeafe,stroke:#2563eb,color:#1e3a8a;",
        "    classDef branch fill:#ffedd5,stroke:#ea580c,color:#7c2d12;",
        "    classDef bash fill:#dcfce7,stroke:#16a34a,color:#14532d;",
        "    classDef empty fill:#f3f4f6,stroke:#9ca3af,color:#374151;",
        "    classDef sc fill:#fce7f3,stroke:#be185d,color:#831843;",
        "    classDef trigger fill:#ede9fe,stroke:#7c3aed,color:#4c1d95;",
        "    classDef sensor fill:#fef9c3,stroke:#ca8a04,color:#713f12;",
        "    classDef hook fill:#cffafe,stroke:#0891b2,color:#164e63;",
        "    classDef tf fill:#e0e7ff,stroke:#4f46e5,color:#312e81;",
        "    classDef other fill:#e5e7eb,stroke:#6b7280,color:#1f2937;",
    ]