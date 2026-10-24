# Genie evaluation protocol

Three runs of the same 25 questions (`benchmarks.csv`). Each run changes one thing, so any accuracy change has
a cause you can name.

| Run | Genie agent | Data sources | Instructions |
|---|---|---|---|
| **A** baseline | `Retail (tables)` | `gold.fact_order_line`, `gold.fact_return`, `gold.dim_customer`, `gold.dim_product`, `gold.dim_date` (with the column comments the pipeline sets) | none |
| **B** semantic layer | `Retail (metric views)` | `semantic.sales_metrics`, `semantic.returns_metrics` | none |
| **C** after fixes | same agent as B | same as B | only the fixes from the library below that a **dev** failure in run B called for |

Rules:

- **Tune on dev only.** Read the failures of the 15 dev questions, apply fixes, rerun. Never look at why a holdout
  question failed before run C is finished. Holdout accuracy is the honest number.
- **Every "Bad" gets a manual review** and one failure type (below). The benchmark compares result sets exactly, so a
  correct answer with an extra column or different rounding scores "Bad": tag it `scoring_artifact`.
- Report two numbers per run and split: **strict** (benchmark "Good") and **reviewed** (Good + scoring artifacts).

## Setting up the benchmarks in Genie

1. Create the agent (Genie → New), add the data sources for the run, and pick the SQL warehouse.
2. **Benchmarks** tab → **Add benchmark** for each row of `benchmarks.csv`: paste `question` and, as the
   SQL answer, `ground_truth_sql`.
3. If the agent refuses ground-truth SQL on tables that aren't its data sources (run B/C), add the five gold tables
   to the agent as well and note it in the report. The metric views remain the documented path.
4. **Run all benchmarks.** For each question record the verdict and, for "Bad", the SQL Genie generated, in
   `runs/run_<A|B|C>.csv`.
5. Compare the numbers Genie returned with `expected_answers.csv`. The data is deterministic, so they must match.

## Failure types

| Type | Meaning | Example |
|---|---|---|
| `wrong_join` | Joined on the wrong key or path | `fact.customer_id = dim_customer.customer_id` matches every customer version and double-counts |
| `wrong_metric_definition` | Right data, wrong formula | revenue as net sales; AOV as AVG(line amount) |
| `wrong_grain` | Counted lines instead of orders, or averaged ratios | `COUNT(*)` on the line-level fact |
| `wrong_time_filter` | Wrong period or date column | return rate by return date instead of order date |
| `unresolved_ambiguity` | Picked a reading without a definition to go on | "top customers" by order count |
| `missing_metadata` | Needed a value mapping or description that wasn't there | "Germany" vs `country = 'DE'` |
| `scoring_artifact` | Correct answer, but the result set differs in shape or rounding | extra column, rounded to 2 decimals |

## Fix library (apply only what a dev failure calls for, and record which failure it fixes)

**Glossary (general instructions)**
- "Sales", "how much did we sell": `net_sales` (after discounts, before returns).
- "Revenue": `net_revenue` (net sales minus refunds on those orders).
- "Return rate": `return_rate` = returned units / units sold, for orders placed in the period (by order date).
- "Refunds in <period>": `returns_metrics.refunds_paid` (by the date the return was received).
- "Basket size": `units_per_order`. "Average order value": `aov`.
- "Top customers": ranked by `net_revenue`, unless another measure is named.
- "First half" = January to June; "Q1" = January to March.

**Join notes (only relevant for run A, where Genie sees tables)**
- Join `fact_order_line` and `fact_return` to `dim_customer` on `customer_key` only, never on `customer_id`:
  `dim_customer` has one row per version of a customer.
- Join to `dim_product` on `product_key`. Products missing from the catalogue have `product_key = 'UNKNOWN'`.
- Count orders with `COUNT(DISTINCT order_id)`; the fact table has one row per order line.

**Value mappings**
- Countries are ISO codes: Germany = `DE`, France = `FR`, Netherlands = `NL`, Austria = `AT`, Switzerland = `CH`.
- Membership tiers: `Basic`, `Plus`, `Premium` (tier at order time).

**Example SQL (add as a trusted example only if a dev failure needs it)**
```sql
-- Return rate by category for orders placed in a period
SELECT category, MEASURE(return_rate) AS return_rate
FROM retail.semantic.sales_metrics
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-03-31'
GROUP BY category
```
