# Genie evaluation report

The accuracy tables come from `python -m tools.score_runs` over the reviewed run sheets in `genie/runs/`. Every row
there has Genie's verdict, a failure type for each "Bad", Genie's SQL and a note.

## Question

Can business users get correct answers from Genie over this lakehouse? Which failures does a governed semantic
layer (Metric Views) remove, and which need extra instructions?

## Setup

| | |
|---|---|
| Questions | 25 fixed questions (`genie/benchmarks.csv`): 6 KPI, 5 breakdown, 5 returns, 5 ambiguous, 4 join-sensitive |
| Split | 15 dev (used for tuning), 10 holdout (see the caveat under "Did the fixes generalise?") |
| Ground truth | SQL on the gold tables, written independently of the metric views; expected values in `genie/expected_answers.csv` |
| Trap check | For 14 questions, the most plausible wrong reading was run too and gives a different answer (`tests/test_benchmarks.py`) |
| Scoring | Built-in Genie benchmarks (exact result-set match), then a manual review of every "Bad" |
| Mode | Genie's default Agent mode, the same in all three runs |
| Run A | Agent `Retail (tables)` on the 5 gold tables, no instructions. 2026-10-25 13:36:37 |
| Run B | Agent `Retail (metric views)` on the 2 metric views, no instructions. 2026-10-25 14:23:43 |
| Run C | Run B's agent plus one general instruction, chosen from the run B dev failures. 2026-10-25 15:01:50 |

**Gold tables in runs B and C.** A Genie agent rejects benchmark ground-truth SQL on tables that aren't among its
sources. To keep the ground truth independent of the metric views, the five gold tables were added to the run B
agent as well. None of Genie's 50 queries in runs B and C used them: every one reads `retail.semantic.*`.

**The run C instruction**, verbatim: *"Return only the rows the question asks for. If it asks for the top, highest or
lowest item, or names one country, category or period, return just that row, not a ranked list of all of them.
Return ratios and shares, such as return rate or share of sales, as decimals between 0 and 1, not as
percentages."*

## Results

| Run | Split | Strict | Reviewed |
|---|---|---|---|
| A | dev | 9/15 (60%) | 13/15 (87%) |
| A | holdout | 5/10 (50%) | 7/10 (70%) |
| A | **all** | 14/25 (56%) | 20/25 (80%) |
| B | dev | 13/15 (87%) | 15/15 (100%) |
| B | holdout | 7/10 (70%) | 10/10 (100%) |
| B | **all** | 20/25 (80%) | 25/25 (100%) |
| C | dev | 14/15 (93%) | 15/15 (100%) |
| C | holdout | 8/10 (80%) | 10/10 (100%) |
| C | **all** | 22/25 (88%) | 25/25 (100%) |

| Failure type | A | B | C |
|---|---|---|---|
| missing_metadata | 0 | 0 | 0 |
| scoring_artifact | 6 | 5 | 3 |
| unresolved_ambiguity | 3 | 0 | 0 |
| wrong_grain | 0 | 0 | 0 |
| wrong_join | 0 | 0 | 0 |
| wrong_metric_definition | 2 | 0 | 0 |
| wrong_time_filter | 0 | 0 | 0 |

**Strict** = Genie's benchmark verdict. **Reviewed** = also counts answers that were correct but scored "Bad"
because of their shape: a percentage instead of a decimal, extra rows or columns, a timestamp instead of a date
(`scoring_artifact`).

**Answer to the question.** On the raw gold tables Genie got 5 of 25 questions genuinely wrong, all on business
definitions. On the metric views it answered all 25 correctly. The remaining "Bad" verdicts are about answer shape,
and one instruction removed some of those.

## What the semantic layer fixed by itself (A → B)

Every real error in run A disappeared in run B, without any instructions:

| Question | Run A failure | Why it passed in B |
|---|---|---|
| B01 (dev) net revenue, July 2026 | `wrong_metric_definition`: `SUM(net_amount)` without refunds, 3,860,353.18 instead of 3,520,103.47 | `MEASURE(net_revenue)` |
| B19 (holdout) "our revenue", Q2 2026 | `wrong_metric_definition`: net sales, 12,025,280.84, exactly the recorded wrong reading; correct is 10,997,554.48 | "revenue" is a synonym of `net_revenue` |
| B20 (dev) top 10 customers, 2026 | `unresolved_ambiguity`: ranked by net sales; same customers, different order | ranked by `MEASURE(net_revenue)`, whose description calls it the company's definition of revenue |
| B21 (holdout) average basket size, July 2026 | `unresolved_ambiguity`: order value, 315.67, the recorded wrong reading; correct is 2.73 units | "basket size" is a synonym of `units_per_order` |
| B13 (holdout) return rate by category, Q1 2026 | `unresolved_ambiguity`: filtered out the `Unknown` category on its own, although the table and column comments describe it | kept all 9 categories (still scored "Bad" in B for returning percentages) |

**The traps that didn't fire.** The grain traps (B02, B03) and the join traps (B22, B23, B24) were answered correctly
already in run A. Genie counted `COUNT(DISTINCT order_id)` and joined the fact to `dim_customer` on `customer_key`,
the SCD2-safe key, without being told. Joining on `customer_id` would have matched every customer version: for B22 the
Basic tier would have shown €2.35M instead of €1.88M. There was no join error in any run, so none could be fixed.

## Failures found on dev in run B and how they were fixed (B → C)

Run B had no real errors. Its two dev "Bad" verdicts were answer-shape problems, so the fix library's glossary, join
notes, value mappings and example SQL were not needed. One general instruction (above) targeted the shape.

| Question | Failure type | What Genie did | Fix applied | Run C |
|---|---|---|---|---|
| B11 (dev) country with the highest AOV, Q2 2026 | `scoring_artifact` | Ranked all five countries; Austria (322.83) on top | Output-shape instruction | Good: `ORDER BY aov DESC LIMIT 1` |
| B25 (dev) share of net sales from unknown products, 2026 | `scoring_artifact` | `* 100` percentage plus a row for catalogued products | Output-shape instruction | Still "Bad": now exactly 0.0024558847536176, but the extra row remains |

## Did the fixes generalise? (holdout)

Holdout strict accuracy went from 7/10 in run B to 8/10 in run C. The instruction fixed two holdout questions of the
same kind as the dev failures:

- **B24** (Germany, Q1 2026): run B listed every country; run C filtered to `country = 'DE'` and returned one value.
- **B13** (return rate by category): run B returned percentages; run C used `MEASURE(return_rate)` as a decimal.

It did not fix **B16** ("which category had the highest return rate"), which still lists every category. **B08**
(orders per month), which passed in runs A and B, failed in run C: the counts are all correct, but this time Genie
used `date_trunc`, which returns a timestamp, where the ground truth has a date. That is run-to-run variation, not
the instruction. Reviewed accuracy stayed at 10/10 on holdout, so nothing got worse in substance.

**Caveat: the holdout wasn't sealed.** The protocol says not to look at why a holdout question failed before run C.
In practice every "Bad" was reviewed right after each run, holdout included. The run C instruction was aimed at the
dev failures B11 and B25, but its clause "or names one country, category or period" also describes holdout B24,
which had been reviewed by then. So the B → C holdout gain is weak evidence of generalisation. The A → B result
involved no tuning at all and isn't affected.

## Remaining failures

All three remaining "Bad" verdicts in run C are correct answers in a different shape:

- **B08**: monthly counts correct; month as a timestamp instead of a date.
- **B16**: Clothing (0.211979) on top, but every category listed.
- **B25**: the share is exactly right as a decimal, but a second row for catalogued products is added.

They were not fixed further. The next instructions would be about satisfying an exact-match scorer ("never add
comparison rows", "cast months to DATE"), not about getting answers right. And B08 and B16 are holdout questions,
so tuning on them would spoil the holdout number.

## Limitations

- **Small sample.** 25 questions: one question is 4 percentage points. Treat differences of one or two questions as noise.
- **Exact matching.** The benchmark compares result sets exactly. Correct answers with different rounding, an extra
  row or column, or a different type score "Bad", hence the manual review and the two accuracy columns.
- **Non-determinism.** Each configuration ran once, so the variation wasn't measured directly. There are signs of it:
  B08 passed in runs A and B and returned a timestamp month in run C, and B13 handled the `Unknown` category
  differently between runs. A second run of configuration C would put a number on it.
- **Gold tables visible in runs B and C.** They were needed for the ground truth. Genie didn't use them, but a
  different question set might tempt it to.
- **No conversation context.** Benchmark questions run as new conversations. Follow-up questions aren't tested.
- **Synthetic data** with clean semantics; real catalogues have messier names and more columns.
