"""greet_operator.py — The canonical "Hello world" of custom Airflow operators.

This module is a deliberately tiny, didactic subclass of ``BaseOperator``.
It exists to teach three things in one place:

1. **Idiomatic subclassing.**  How to extend ``BaseOperator``: accept
   operator-specific kwargs, validate them up-front, then forward the rest
   of the kwargs to ``super().__init__`` so Airflow's standard wiring
   (``task_id``, ``owner``, ``retries``, ``dag``, ...) keeps working.

2. **Param validation that fails fast.**  The constructor rejects unknown
   languages with a ``ValueError`` *before* the operator is ever scheduled.
   That keeps parse-time errors parse-time errors, instead of letting them
   surface as confusing task-instance failures at run-time.

3. **An explicit ``xcom_push``.**  ``execute`` writes a single string to
   XCom under the well-known key ``"greeting"`` via
   ``context["ti"].xcom_push(key=..., value=...)``.  Downstream tasks can
   then pull it with ``ti.xcom_pull(task_ids=..., key="greeting")`` or via
   the usual Jinja ``{{ ti.xcom_pull(...) }}`` template.

What NOT to copy into a real operator:

* This module's interface is intentionally **boring**.  Production
  operators typically expose ``template_fields`` so callers can use Jinja
  substitution, accept a ``BaseHook`` for I/O, raise domain-specific
  exceptions (``AirflowException`` subclasses), support multi-output XCom
  via ``XComArg``, and document idempotency.
* The validation here is a single ``frozenset`` membership check.  Real
  operators often need richer pre-flight logic — connections to test,
  paths to stat, schemas to validate — and that belongs in ``execute`` or
  a dedicated hook, not in ``__init__``.
* The greeting is rendered as a one-line string.  Treat it as the
  smallest possible example; do not lift it as a design pattern.

A note on the import path: Airflow 3.x deprecates
``airflow.models.baseoperator.BaseOperator`` in favour of
``airflow.sdk.bases.operator.BaseOperator``.  Both still resolve, and this
project pins the legacy path on purpose so the example doubles as a
migration marker — when ``airflow.models.baseoperator`` finally goes away,
this file is the obvious place to update first.
"""

from __future__ import annotations

from typing import Any

from airflow.models.baseoperator import BaseOperator

# Module-level lookup table for the language -> template mapping.  Kept as a
# private frozenset so the validation in __init__ is a single, O(1) set
# membership test rather than a chain of ``==`` comparisons.
_SUPPORTED_LANGUAGES: frozenset[str] = frozenset({"en", "fr", "es", "de"})

# One-line, locale-appropriate greeting templates.  Each template uses
# ``str.format`` with a single ``name`` placeholder; punctuation lives
# inside the template, not in the caller, so the operator stays small.
_GREETINGS: dict[str, str] = {
    "en": "Hello, {name}!",
    "fr": "Bonjour, {name} !",
    "es": "¡Hola, {name}!",
    "de": "Hallo, {name}!",
}


class GreetOperator(BaseOperator):
    """Render a one-line greeting in ``language`` and push it to XCom.

    Parameters
    ----------
    name:
        Who to greet.  Interpolated into the language-specific template
        with ``str.format``; no further validation is performed, so callers
        may pass any string (including ones containing format-syntax
        characters — they will render verbatim because we only substitute
        the ``name`` placeholder).
    loud:
        When ``True`` the rendered greeting is upper-cased before being
        logged and pushed to XCom.  Defaults to ``False``.
    language:
        ISO-639-1 language code.  Must be one of ``"en"``, ``"fr"``,
        ``"es"``, ``"de"`` — anything else raises ``ValueError`` at
        construction time so the failure shows up at DAG parse, not at
        task run.
    **kwargs:
        Forwarded verbatim to ``BaseOperator.__init__``.  Accepts the usual
        Airflow keyword arguments (``task_id``, ``dag``, ``owner``,
        ``retries``, ...).

    XCom
    ----
    On success the rendered greeting is published to XCom under the
    well-known key ``"greeting"``.  Downstream tasks can pull it with
    either ``ti.xcom_pull(task_ids="<this_task_id>", key="greeting")`` or
    the Jinja template ``{{ ti.xcom_pull(task_ids='<this_task_id>',
    key='greeting') }}``.
    """

    def __init__(
        self,
        *,
        name: str,
        loud: bool = False,
        language: str = "en",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        # Fail-fast validation.  A frozenset membership test is O(1) and
        # gives us a single, greppable place to extend the supported set.
        if language not in _SUPPORTED_LANGUAGES:
            raise ValueError(
                f"Unsupported language: {language!r}. "
                f"Expected one of {sorted(_SUPPORTED_LANGUAGES)}."
            )

        self.name = name
        self.loud = loud
        self.language = language

    def execute(self, context: dict[str, Any]) -> None:
        """Render the greeting, log it, and publish it to XCom.

        The returned value is intentionally ``None`` — we publish the
        greeting under the explicit key ``"greeting"`` rather than letting
        Airflow auto-push a second copy under ``"return_value"``.  Keeping
        a single, well-named XCom row makes downstream ``xcom_pull`` calls
        unambiguous.
        """
        template: str = _GREETINGS[self.language]
        greeting: str = template.format(name=self.name)
        if self.loud:
            greeting = greeting.upper()

        # ``self.log`` is the Airflow-provided logger; ``.info`` puts the
        # line at INFO level so it lands in the task log without ceremony.
        self.log.info("Greeting rendered: %s", greeting)

        # Explicit XCom push — the canonical way to publish a value under
        # a known key.  ``context["ti"]`` is the TaskInstance for this run.
        context["ti"].xcom_push(key="greeting", value=greeting)