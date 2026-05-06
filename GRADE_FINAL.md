# Final Deliverable Grade

**Team 20 (Conner Erich - solo)**

| Criterion | Score | Max |
|-----------|------:|----:|
| Deliverable Quality | 4 | 6 |
| Visualizations | 6 | 6 |
| Pipeline Integration | 6 | 6 |
| Analytical Narrative | 4 | 6 |
| **Total (rubric portion)** | **20** | **24** |

Video walkthrough graded separately.

(Note: existing GRADE.md is from M2; this file holds the final grade.)

## Deliverable Quality (4/6)

`outputs/report.html` is a single self-contained HTML report with title, intro, three numbered chart sections, and a conclusion. Charts are inlined Altair visualisations (no broken iframes or missing references). The polish issues hold this back: multiple typos throughout the prose ("secifically", "infromation", "the cost the is incured", "anaylsis", "afforable", "ver"), and the outputs go to `outputs/` (plural) rather than the spec's `output/`. README has corresponding typos. The structure is right but the report does not read as professional.

## Visualizations (6/6)

Three Altair charts covering the required types:

- **Payment Types by Customer Count** (bar, categorical) - sorted by count, with installments stacked.
- **Top 500 Products by Shipping Cost** (bar, categorical) - price-per-density ratio.
- **Top 10 Products Ordered Over Time by City** (line with points, temporal) - tracks demand curves over time.

All have titles, axes, and accompanying explanatory paragraphs. Required types covered.

## Pipeline Integration (6/6)

`uv run wvu-ieng-331-final-20` after `uv sync` runs the full pipeline end-to-end with defaults: validation, queries, and report generation. Tested with the extended database; pipeline ran cleanly. Note: the outputs go to `outputs/` (plural) rather than the spec's `output/`, and the names are correct (summary.csv, detail.parquet, chart.html, report.html). Minor structural deviation but the pipeline itself runs correctly.

## Analytical Narrative (4/6)

Each chart has an explanatory paragraph noting what it shows and one or two observations (credit card / boleto dominance, top 500 products ship for under $1, top products are one-time unique purchases). The conclusion paragraph summarises observations but does not propose recommendations or actions: it repeats that cash flow comes from unique product shipping and that customers pay reliably. No "what should the business do about this" framing. Combined with the typos throughout the narrative, this fits the "thin" description in the rubric.
