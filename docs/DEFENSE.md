# Defending the CV entry

> **Governed Retail AI/BI Lakehouse** 2026 | Databricks, PySpark, Lakeflow, Unity Catalog
> Built incremental Bronze, Silver and Gold retail pipelines in PySpark and Lakeflow with data-quality checks,
> routing failed records to quarantine tables. Defined revenue, AOV and return-rate KPIs in Unity Catalog Metric
> Views, built an AI/BI dashboard on them, and evaluated Genie on a fixed 25-question set, fixing the join and
> metric-definition errors it exposed.

Rule: every phrase points to something in the repo. If a row below has no evidence yet, the phrase isn't ready.

## Claim → evidence

| Phrase | Evidence | Status |
|---|---|---|
| incremental Bronze, Silver and Gold | `pipelines/transformations/*.py`, `screenshots/incremental_rows_per_update.png` (update 2 writes 0 rows) | TODO |
| in PySpark and Lakeflow | `pipelines/utilities/transforms.py` (pure PySpark, unit-tested), `from pyspark import pipelines as dp` | done |
| data-quality checks | `pipelines/utilities/rules.py`, `docs/dq_rules.md`, `screenshots/expectations.png` | TODO screenshot |
| routing failed records to quarantine tables | `silver.*_quarantine` + `screenshots/validate_pass.png`: quarantine = injected defects, exactly | TODO |
| revenue, AOV and return-rate KPIs in Metric Views | `semantic/sales_metrics.sql`, `docs/metric_definitions.md`, `screenshots/metric_view.png` | TODO screenshot |
| AI/BI dashboard on them | `dashboards/retail_kpis.lvdash.json`, `screenshots/dashboard_*.png` | TODO |
| evaluated Genie on a fixed 25-question set | `genie/benchmarks.csv`, `genie/runs/run_*.csv`, `screenshots/genie_run_*.png` | TODO |
| fixing the join and metric-definition errors it exposed | `docs/evaluation_report.md` (before/after per question) | TODO. If no join error showed up, change "join and metric-definition" to the failure types that did. |

## Numbers to know

| | |
|---|---|
| Data | Fictional general retailer, 20,000 customers, 1,440 products, 8 categories, 2025-09-01 to 2026-09-14 |
| Volume | 161,300 order records → 343,151 order lines, 31,730 returns, ~57 MB, 15 landing batches |
| Bad rows quarantined | 3,105 order lines (4 rules), 155 returns (2 rules): equal to the injected counts |
| Duplicates removed | 2,757 order lines (incl. one re-sent file), 169 returns |
| Orphans kept and flagged | 983 order lines (Unknown product), 95 returns |
| Genie accuracy (holdout) | Run A: TODO · Run B: TODO · Run C: TODO |
| Best example of a fixed error | TODO |

## The 60-second version

"It's a lakehouse for a fictional online retailer on Databricks Free Edition. A seeded generator delivers a year of
backfill and then daily files with deliberately injected defects, and writes down exactly what it injected. A
Lakeflow pipeline loads the files incrementally with Auto Loader, validates them in silver, where bad rows go to
quarantine tables with the rule they broke, and builds a star schema in gold. Because I know the ground truth, I
could prove the quarantine caught exactly the injected defects. On top I defined the KPIs once as Metric Views:
revenue, AOV, return rate. The dashboard and Genie use those. Then I benchmarked Genie on 25 fixed questions, with
a dev/holdout split so I wasn't just tuning to the test. Moving from raw tables to Metric Views took accuracy from
TODO to TODO, and the remaining errors were TODO."

## Questions you will get

### Incremental processing

**How is it incremental?**
Bronze tables use Auto Loader (`cloudFiles`). It records every file it has processed in its checkpoint, so each
pipeline update reads only files that arrived since the last one. The validation and quarantine tables are streaming
tables reading bronze as a stream, so they only process new rows. Customers and products use AUTO CDC, which applies
only new change records. De-duplicated silver and all of gold are materialized views: on serverless the engine
refreshes them incrementally when the query allows it and recomputes them otherwise. The event log shows which
(`03_incremental_proof.sql`, query C). Proof: rerunning without new files writes 0 rows (query B) and adds no
commits to the bronze tables.

**Why streaming tables in bronze/silver but materialized views in gold?**
Ingestion and validation are append-only: each record needs processing once, which is what a stream does. Gold joins
facts to dimensions that change (customer history) and attaches returns that arrive weeks after the order. A
streaming join would freeze whatever the dimension looked like when the line was processed and miss late returns. A
materialized view is always equal to its query over the current inputs, and the engine decides how much it can
refresh incrementally. De-duplication is a materialized view too; see the next question for why.

**A return arrives 25 days after the order. What happens?**
It lands in a later batch and flows through the returns stream into `silver.returns`. The next refresh of
`fact_order_line` re-aggregates returns onto the original line. The return rate for that order month goes up after
the fact. That's the cohort-maturity effect the dashboard marks.

**What went wrong while you built it?** *(the best story in the project: tell it)*
The first version removed re-sends in a streaming table with a 3-day watermark. Silver reconciled perfectly, but
the validation notebook failed "net sales preserved silver → gold". The gold fact was exactly one pipeline update
behind: after landing batch 1 it still had the backfill's 325,070 rows, and after batches 2–7 it had 325,866, the
previous update's total.

I worked through it from the evidence:
- The event log showed the fact was planned 21 seconds *after* the new silver rows were committed.
- `DESCRIBE HISTORY` showed the stateful stream writing a second, empty watermark commit every update.
- The pipeline graph showed the dependency was registered, so it wasn't a missing edge.
- The views that refreshed incrementally were current; only the full recompute read the old snapshot.

I removed the streaming state and de-duplicated in a materialized view that keeps the first arrival of each content
hash, then re-verified with new batches (TODO: fill in the result of the full refresh + batches 8–14 rerun).
Side benefit: re-sends are caught however late they come, not just within 3 days. Lesson: reconcile every layer
against ground truth, not just the layer you changed.

### Data quality

**How do you know the checks work?**
The generator writes a manifest of every defect it injected, per rule and batch. `04_validate.py` compares it with
the quarantine tables: the counts are equal exactly, not approximately. The same reconciliation runs locally in
`tests/test_end_to_end.py`, as part of the local test suite.

**Drop, warn or fail: how did you decide?**
Unusable row (no key, impossible quantity) → drop and quarantine. Valid row with a missing reference (unknown
product) → keep, map to an unknown member, flag, warn, because dropping it would lose real revenue. Output that is
wrong whatever the input (duplicate key in gold) → fail the update, because that's a bug or a conflict a person must
look at. Table in `docs/dq_rules.md`.

**Why de-duplicate by content hash and not by order_id?**
Business-key de-duplication silently picks a winner when two *different* records share a key. Hashing the content
removes only true copies, and keeping the first arrival means the row still points to the file it originally came
in. Conflicting versions both reach gold, where `unique_order_line_id` (`expect_or_fail`) stops the update.

**Why are expectations on a separate private table before de-duplication?**
So the event log counts every bad row that arrived, including the bad rows inside a re-sent file. Then the
manifest, the expectation metrics and the quarantine counts all agree.

**What happens to quarantined rows?**
They keep the broken rules, source file and batch. `dq.issue_summary` feeds the dashboard's data-quality page. They
aren't reprocessed automatically: the fix is upstream re-delivery, which flows through like any new record.

**An order line references a product that isn't in the catalogue. Isn't keeping it wrong?**
The sale happened, so dropping it would make revenue disagree with finance. It goes under an "Unknown product"
member and is flagged. Because the fact is a materialized view, if the product shows up in the catalogue later the
line re-links to it on the next refresh automatically.

### Modelling

**How does "sales by membership tier" stay correct when customers upgrade?**
Customers are SCD type 2 via AUTO CDC (`__START_AT`/`__END_AT`). The fact joins each line to the version valid at
`order_ts` and stores that version's `customer_key`. Joining on `customer_id` would match every version and
double-count: the benchmark's trap for B22 shows Basic-tier August sales inflating from €1.88M to €2.35M.

**Why two facts?**
Different date bases. Sales and return rate are by order date; refunds paid are by return date. One table can't
honestly answer both.

### Metric Views

**What is a Metric View and why not a normal view?**
A Unity Catalog object defined in YAML: a source, joins, dimensions and measures. You query it with `MEASURE()` at
any grain. A normal view fixes the grain, so a ratio like AOV is computed per row and then gets averaged, which is
wrong. A metric view computes `SUM(net)/COUNT(DISTINCT order)` at whatever grain the query asks for. It also
carries synonyms and descriptions that Genie reads, and it's governed like a table: grants, lineage.

**Define return rate.**
Returned units ÷ units sold, for orders placed in the period. By order date, so it answers "of what we sold in
July, how much came back". Units rather than value, so categories with very different prices are comparable. The
value-based rate exists separately.

**Is AOV before or after returns?**
Before: net sales per order at checkout. Revenue after returns is `net_revenue`.

### Governance

**What does "governed" mean here?**
- Least-privilege grants by layer: analysts read `gold`, `semantic` and `dq` only.
- No personal data in gold.
- Email masking through a function that checks group membership.
- Tags marking where PII lives.
- Column descriptions written by the pipeline itself.
- Lineage from the metric views back to the files.
- One definition of each KPI.

**Limitations?**
Free Edition has no account console or SCIM: TODO, whichever way groups worked out. Unity Catalog column masks on
pipeline-owned tables have to be declared inside the pipeline, so I used a masking view.

### Genie evaluation

**How did you evaluate it?**
- 25 fixed questions in five categories, with ground-truth SQL on the gold tables, written independently of the
  metric views.
- 15 dev questions for tuning, 10 holdout questions I didn't look at until the end.
- Three runs: tables only, metric views, metric views plus fixes.
- Every "Bad" reviewed by hand and tagged with one failure type.
- For the ambiguous and join questions, I checked that the wrong reading really gives a different number;
  otherwise the question wouldn't test anything.

**Give an example of an error and the fix.** TODO, from `evaluation_report.md`. Prepare two: one join, one metric
definition.

**What are the limits of that evaluation?**
- 25 questions means one question is 4 points.
- Exact result matching produces false failures, hence the manual review.
- Genie isn't deterministic.
- Benchmarks don't test follow-up questions.
- The data is synthetic.

### Platform and production

**What would you change for production?**
- Deploy with Asset Bundles and run the pytest suite in CI.
- Separate dev and prod catalogs.
- Put the key checks in their own job task to gate downstream consumers.
- SQL alerts on `dq.issue_summary`.
- Groups synced from the identity provider.
- Column masks declared in the pipeline.
- Liquid clustering on the facts.
- Data-quality monitoring on the gold tables.

**What did Free Edition force on you?** TODO. Write down what actually happened, e.g. one pipeline, 2X-Small
warehouse, quota.

## Be upfront about

- The data is synthetic. Say so first; the point is that it lets you prove the DQ results against ground truth.
- The dashboard and the Genie agents were built in the UI; their exports and the run sheets are in the repo.
- How the project was built, including any AI assistance, if asked. What makes it defensible is that you can
  explain, run and change every part of it.
