# Milestone 2 Grade

**Team 20**

| Category | Score | Max |
|----------|-------|-----|
| Pipeline Functionality | 6 | 6 |
| Parameterization | 4 | 6 |
| Code Quality | 4 | 6 |
| Project Structure | 3 | 3 |
| DESIGN.md | 2 | 3 |
| **Total** | **19** | **24** |

---

## Pipeline Functionality — 6/6

- Default run completes cleanly, producing `outputs/summary.csv`, `outputs/detail.parquet`, and `outputs/chart.html`.
- Holdout (extended DB, ~144k rows) runs without errors; row counts scale correctly.
- Parameters (`--payment_installment 3 --seller_city "rio de janeiro" --shipping_price_rating moderate`) all change output results.
- Loguru structured logging throughout; `FileNotFoundError` raised if DB is missing.

---

## Parameterization — 4/6

**Strengths:**
- Five CLI parameters defined: `--payment_installment` (int), `--shipping_price_rating` (str), `--seller_city` (str), `--chart_limit` (int), `--review_rating` (str).
- `--db` allows an alternate database filename.
- Parameters are passed as SQL positional params (`$1`) — safe, no f-string injection.
- `--chart_limit` has meaningful effect on the chart output.

**Deductions:**
- `--review_rating` and `get_product_reviews()` are defined but `pipeline()` never calls `get_product_reviews()` — the parameter has no effect at runtime.
- No input validation on string params (e.g., `--shipping_price_rating invalid` silently returns 0 rows rather than raising an error).
- Shallow validation: no choices/enum enforcement on any string argument.

---

## Code Quality — 4/6

**Strengths:**
- `loguru` used throughout `pipeline.py` with appropriate INFO/ERROR levels.
- `pathlib.Path` used consistently for all file paths.
- Type hints on nearly all function signatures.
- Docstrings on most functions.
- Specific exceptions: `FileNotFoundError`, `RuntimeError`, `SystemExit(1)`.

**Deductions:**
- `validation.py` uses stdlib `logging` instead of `loguru`, creating a mixed logging setup.
- `validation.py` executes `data_path("olist.duckdb")` and `duckdb.connect()` as module-level side effects with a hardcoded filename — bypasses the `--db` parameter and will fail on import if the DB is absent.
- `data_not_null.sql` and `data_row_counts.sql` are static queries; the validation loop iterates `KEY_COLUMNS` and `MIN_ROW_COUNTS` but always runs the same SQL regardless of the `table` variable — results are meaningless (same join count returned for each iteration).
- `__init__.py` contains a dead `main()` stub (`print("Hello from wvu-ieng-331-m2-20!")`) that is never called (entry point correctly points to `pipeline.py`).
- `get_seller_consumer_location` in `queries.py` lacks a docstring.

---

## Project Structure — 3/3

- `src/wvu_ieng_331_m2_20/` package layout with `__init__.py`, `pipeline.py`, `queries.py`, `validation.py`.
- `sql/` directory with all SQL files external to Python code.
- `pyproject.toml` with `[project.scripts]` entry point, proper dependencies, and dev group.
- `uv.lock` and `.python-version` present.
- `README.md` with How to Run, Parameters table, Outputs, and Validation Checks sections.

---

## DESIGN.md — 2/3

**Strengths:**
- Covers five sections: Parameter Flow, SQL Parameterization, Validation Logic, Error Handling, Scaling & Adaptation.
- SQL Parameterization shows actual raw SQL with `$1` placeholder and explains why params are used over f-strings.
- Validation Logic section explains each check and its failure behavior.
- Error Handling names specific exception types and explains the rationale.
- Scaling section answers both scaling and new-output prompts concretely.

**Deductions:**
- Content is largely copy-pasted from README.md (validation checks section is identical).
- No external references or citations.
- Several significant typos and grammatical errors throughout.
- Parameter Flow only covers `--shipping_price_rating`; other parameters not traced.
