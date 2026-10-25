# Runbook: building it in Databricks Free Edition

Follow the steps in order. Each step lists what to screenshot, saved into `screenshots/` under the given name.
When something fails, copy the full error message and the step number back to Claude.

Everything runs on serverless compute and the one 2X-Small SQL warehouse that Free Edition provides. The whole
dataset is about 57 MB, well inside the daily quota.

---

## Step 0 · Accounts (5 min)

1. Sign up for **Databricks Free Edition** (databricks.com → Free Edition). Optional: verify with LinkedIn
   in the account settings for higher usage limits.
2. Push this repo to GitHub (public), so the workspace can clone it.

## Step 1 · Smoke test (day 1, 20 min). Do this before anything else.

1. **Workspace → Create → Git folder**, paste the GitHub URL. The repo appears under your user folder.
2. Open `notebooks/00_smoke_test.sql`, attach it to **Serverless Starter Warehouse**, and run the cells in order.
   Create the two groups when the notebook tells you to.
3. Fill in the results:

| Check | Result | Notes |
|---|---|---|
| Runtime version | | |
| CREATE CATALOG | | |
| Metric view (basic) | | |
| Metric view (window measure) | | |
| Groups + GRANT | | |
| Genie | | |

**If the basic metric view fails, stop and tell Claude.** The CV line has to change before going further.

## Step 2 · Setup (2 min)

Run `notebooks/01_setup.sql` (SQL warehouse). It creates catalog `retail`, the schemas and the volume
`retail.raw.landing`.

## Step 3 · Land the backfill (2 min)

Open `notebooks/02_land_batch.py`, attach **Serverless** compute, set the widget `batches` = `0`, run all.
Expect `batch_000` with 329,660 order lines, of which 2,963 are bad and 1,627 are duplicates.

## Step 4 · Create the pipeline (10 min)

> **Free Edition allows only a little serverless compute at once.** Before every pipeline run, terminate the
> notebooks' serverless sessions (notebook toolbar → Serverless ▾ → › → Terminate). Otherwise the run fails with
> `RESOURCE_EXHAUSTED: You've hit the limit for serverless compute`.

1. **Jobs & Pipelines → ETL pipeline.** Databricks immediately creates a draft ("New Pipeline <date>") in
   `workspace.default` with an empty `my_transformation.py`.
2. Open **Settings (gear) → JSON**, replace everything with the block below (put your own pipeline `id` back and
   your email in both paths), and **Save**:

   ```json
   {"id": "<keep the id shown>", "pipeline_type": "WORKSPACE", "name": "retail_lakehouse",
    "libraries": [{"glob": {"include": "/Workspace/Users/<you>/governed-retail-lakehouse/pipelines/transformations/**"}}],
    "catalog": "retail", "schema": "bronze", "continuous": false, "development": false,
    "photon": true, "channel": "CURRENT", "serverless": true,
    "root_path": "/Workspace/Users/<you>/governed-retail-lakehouse/pipelines",
    "configuration": {"landing_root": "/Volumes/retail/raw/landing",
                      "calendar_start": "2025-01-01", "calendar_end": "2026-12-31"}}
   ```

   The root folder is on the Python path, which is how `from utilities import ...` resolves. The draft's
   `my_transformation.py` is now outside the pipeline ("Excluded") and can be deleted.
3. **Run pipeline.** The first run takes about 4 minutes.

Screenshot: the pipeline graph after a green run → `pipeline_graph.png`.

## Step 5 · Validate the backfill (2 min)

Run `notebooks/04_validate.py` (serverless). Everything should say **PASS**. The metric-view checks are skipped
until step 8.

## Step 6 · Prove it's incremental (15 min)

1. **Run the pipeline again without landing anything.** This is update 2: nothing new should be read.
2. Land batch 1 (`batches` = `1`), terminate the notebook session, run the pipeline. This is update 3.
3. Land batches 2-7 (`batches` = `2-7`), terminate the session, run the pipeline. Batch 7 re-sends batch 6's orders
   file: the duplicates must disappear in silver and the quarantine must grow by batch 6's bad rows again.
4. Run `04_validate.py`: all PASS. In particular, "Net sales preserved silver -> gold" proves gold caught up in the
   same update (see "Why de-duplication is a materialized view" in `docs/dq_rules.md`).
5. Run `notebooks/03_incremental_proof.sql`. In query B, `retail.gold.fact_order_line` must grow in the same update
   as `retail.silver.order_lines`.
6. Land batches 8-14 (`batches` = `8-14`), run the pipeline, run `04_validate.py` again: all PASS.

Screenshots: query B (rows per flow per update) → `incremental_rows_per_update.png`; query C (MV refresh
technique) → `mv_refresh_technique.png`; query D (expectations) → `expectations.png`; validate result →
`validate_pass.png`.

After all 15 batches the totals must be:

| Table | Rows |
|---|---|
| `silver.order_lines` | 337,289 |
| `silver.order_lines_quarantine` | 3,105 |
| `silver.returns` | 31,406 |
| `silver.returns_quarantine` | 155 |
| `gold.fact_order_line` where `is_orphan_product` | 983 |
| `gold.fact_return` where `is_orphan_return` | 95 |

## Step 7 · Check gold against the expected answers (5 min)

Run three ground-truth queries from `genie/benchmarks.csv` in the SQL editor (e.g. B01, B07, B22) and compare
with `genie/expected_answers.csv`. The data is deterministic, so they must match exactly. If they don't, the
Databricks pipeline behaves differently from the local one: tell Claude which query differs.

## Step 8 · Metric views (5 min)

Run `semantic/sales_metrics.sql` and `semantic/returns_metrics.sql` (SQL warehouse), then `04_validate.py` again:
the metric-view checks must now PASS too.

Screenshot: Catalog Explorer → `retail.semantic.sales_metrics` → Overview (measures and synonyms) → `metric_view.png`.

## Step 9 · Governance (20 min)

1. Run `governance/masks.sql`, `governance/grants.sql`, `governance/comments_tags.sql`. Note which tag cells
   needed the UI fallback.
2. Invite a second email address (Settings → Identity and access → Users), add it to `retail_analysts`, give the
   group **Can use** on the SQL warehouse. Sign in as that user in a private window and run the four test
   queries at the bottom of `grants.sql`.

Screenshots: lineage graph from the metric view down to bronze → `lineage.png`; the analyst's
`PERMISSION_DENIED` on `silver.customers` → `analyst_denied.png`; masked emails → `masked_emails.png`.

## Step 10 · Dashboard (1 hour)

Follow `dashboards/BUILD.md`. Export to `dashboards/retail_kpis.lvdash.json`, screenshot each page.

## Step 11 · Genie evaluation (2-3 hours)

Follow `genie/instructions.md`: run A (tables), run B (metric views), fixes on dev failures only, run C.
Fill `genie/runs/run_A.csv`, `run_B.csv`, `run_C.csv`, then locally:

```bash
.venv/bin/python -m tools.score_runs
```

Paste the output into `docs/evaluation_report.md` and write up each fixed failure there.
Screenshots: benchmark results page for each run → `genie_run_A.png`, `genie_run_B.png`, `genie_run_C.png`.

## Step 12 · Write-up

1. Fill in the TODOs in `docs/evaluation_report.md` and the "Results" and "Lessons learned" sections of `README.md`,
   in your own words.
2. Fill in the numbers in `docs/DEFENSE.md` and read it until you can answer every question without looking.
3. Commit the screenshots, the dashboard export and the run sheets. Push.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'utilities'` | Pipeline root folder not set to `pipelines/` | Step 4.3 |
| `PATH_NOT_FOUND /Volumes/retail/raw/landing` | Setup not run, or a different catalog name | Step 2 |
| Gold MV fails with a schema mismatch | Declared schema and DataFrame differ | Paste the error to Claude |
| `DELTA_...` or expectation `unique_*` failed | A gold key check fired | Paste the failing table to Claude: this means a bug |
| Compute shut down for the day, or SQL warehouse "Clusters are failing to launch" | Free Edition daily quota | Wait until tomorrow; nothing is lost |
| `RESOURCE_EXHAUSTED: You've hit the limit for serverless compute` | Notebook sessions still attached | Terminate them (Serverless ▾ → › → Terminate), run again |
| Run all does nothing; the dot next to Serverless stays hollow ("Activate compute") | Other notebooks still hold serverless sessions | Terminate those sessions, then Run all again |
| "Attached cluster is terminated" when running a notebook | Its previous session ended | Select resource → **General compute** (Serverless) for Python notebooks, or SQL Warehouse for SQL ones → Start, attach and run |
| Gold totals one update behind silver (`04_validate`: "Net sales preserved" FAIL) | The old streaming de-dup (fixed in commit "Move exact-duplicate removal into materialized views") | Pull the Git folder, then **Full refresh all** |
| Error that a table changed from streaming table to materialized view | Dataset type changed in the code | Pipeline → Run pipeline ▾ → **Full refresh all** |
| `CREATE CATALOG` not permitted | Free Edition restriction | Use catalog `workspace`: replace `retail.` everywhere and set the pipeline's default catalog |
