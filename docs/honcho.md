# Honcho Integration

This project uses the **Honcho** MCP server to persist architectural
decisions across work sessions. The intent is that, weeks from now, you
can ask Honcho "why doesn't this use SubDagOperator?" and get back the
same answer the architect gave when the project was first written.

## Peer model

| Peer                  | Role                                                              |
|-----------------------|-------------------------------------------------------------------|
| `airflow-architect`   | The architect's voice. Conclusions store the project's commitments. |
| `user`                | You. Read by the architect peer to anticipate what you need.       |

## Session model

| Session                              | Purpose                                                              |
|--------------------------------------|----------------------------------------------------------------------|
| `airflow-learning-project-redesign`  | The full session for the redesign work. Re-use for follow-up questions. |

## What lives in Honcho

* **Conclusions** (per-peer facts) — the invariants, contracts, and
  commitments the project makes. These are short, declarative, and
  searchable.
* **Peer card** — a compact "what is this project?" summary, useful when
  a new agent joins.
* **Conversation** — the messages between peers, kept for context
  continuity. The architect peer observes itself, so this builds up
  automatically as work proceeds.

## Inspecting Honcho

Use the Honcho CLI skill (`/honcho-cli`). Useful commands:

```
honcho workspace inspect
honcho peer card airflow-architect
honcho peer search "design pattern" --target airflow-architect
```

## When to update Honcho

Add a new conclusion when:

* You make a decision that explains a *what* with a *why*. ("We use
  `INSERT OR IGNORE` because retries must be safe.")
* You add a new invariant. ("The CleanedOrder is narrower than
  OrderRecord.")
* You reject a pattern. ("We don't use SubDagOperator.")

If a change touches an existing conclusion, delete and re-add it; Honcho
versions are append-only.

## What Honcho is NOT

* It's not a replacement for `docs/`. Long-form rationale belongs in
  Markdown files because they're searchable on the filesystem.
* It's not a source of truth at runtime — Airflow itself never reads
  Honcho. The authoritative source is the DAG file.
