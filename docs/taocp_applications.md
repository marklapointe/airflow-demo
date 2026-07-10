# TAOCP Applications — What "rigor" means here

This file is short on purpose. It exists to make one idea explicit: the
rigor in this project is *Knuth-style* — careful definitions, stated
invariants, and analyses that hold up under change.  Not "lots of
docstrings" or "lots of comments".

The decisions are recorded in `docs/design_decisions.md` and tracked in
Honcho. This file is the *why*, not the *what*.

---

## The four moves, mapped to this repo

Knuth's *The Art of Computer Programming* repeatedly emphasises four
moves: define the data, define the invariant, prove the algorithm, and
test the boundary. Each appears in this repo.

### 1. Define the data — `include/domain/__init__.py`

Knuth spends the first fifty pages of TAOCP on data representation. We do
the same: the *first* thing this repo defines is the record set, not the
operators. Once `OrderRecord` exists, every downstream function is
constrained by what `OrderRecord` allows.

A new contributor who wants to add a column has to:

1. add the field to the dataclass,
2. update the `CsvSource.REQUIRED_*` columns tuple,
3. update the sample data,
4. update the test fixture.

That sequence forces the contributor to think about the data as a
*contract* between layers — exactly Knuth's move.

### 2. State the invariant — module docstrings

A module docstring at the top of `include/transforms/__init__.py` lists
the order of the cleaning pipeline and the reasoning. Anyone wondering
"why do we drop cancelled orders before checking qty?" can answer it
without reading the code.

`include/domain/__init__.py` adds the language "narrower projection" and
"shape invariant". Those are intentional; they import the vocabulary of
*algebraic data types* (Wirth, Knuth §2.3) into the Python codebase.

### 3. Prove the algorithm — pinned test fixtures

The test `TestAgainstSampleCsv::test_total_revenue` asserts a specific
sum (24,282 cents). The test file pins 10 rows in, 7 cleaned out. If you
change the cleaning pipeline in a way that breaks these numbers, you
catch it before deploying.

This is the *behavioural* spec — Knuth's "proof by test fixture" idea
applied to ETL.

### 4. Test the boundary — `tests/unit/test_*.py`

We test:

* Happy path (well-formed input).
* Each rejection criterion alone (negative qty, negative price, bad
  status).
* Each rejection criterion combined (the "every kind at once"
  invariant).
* Errors at the source boundary (`SourceUnavailable`, `SchemaMismatch`).

The combination is exhaustive over the cheap dimensions. We don't test
all 2^10 combinations — but we test the ones that *change the
invariants*.

---

## What we don't do (and why)

### We don't profile-first

You can. Knuth says measure before optimising. For a learning project
the operational scale is "fits on a laptop", so we trust that our
constant memory streaming patterns keep RAM bounded. Document the choice
in code so anyone scaling up has a starting point: see
`include/transforms/__init__.py`'s "Streams > lists" note.

### We chase 100% coverage on the hot paths, not the DAGs

We pin coverage to **100%** on the modules that own contracts:

* `include/domain/__init__.py` — typed records
* `include/io/__init__.py` — CSV / SQLite adapters
* `include/transforms/__init__.py` — pure cleaning pipeline
* `web/extractor.py` — static DAG metadata extractor
* `web/app.py` — Flask UI

DAG files themselves are integration-tested only — they're mostly
composition and the meaningful contracts live in `include/`. Test
energy goes there. The web UI is also covered by **Playwright**
end-to-end browser tests (`tests/e2e/`) that assert Mermaid actually
renders SVG in real Chromium, not just that the HTML contains a
`mermaid` class.

### We don't formalise the algebra

A literate Knuth-style treatment would express the cleaning pipeline as

```
clean_orders = to_cleaned ∘ price_is_sane
                       ∘ require_positive_qty
                       ∘ drop_unknown_status
```

with proofs that each function is total on its input. We don't write
that down for every pipeline — but the *naming* (`to_cleaned` as a
projection; `clean_orders` as the composition) makes it discoverable.

---

## Cheat sheet: when the rigour feels heavy

If you find yourself thinking "this is a lot of process for a 100-line
DAG", that's expected. The cost is paid once and amortised across every
DAG you write afterwards — most of which will look like
`dags/02_etl/csv_to_warehouse.py`: a thin orchestration shell around an
already-tested `include/` module. The system *gets out of your way* once
the contracts are stable.
