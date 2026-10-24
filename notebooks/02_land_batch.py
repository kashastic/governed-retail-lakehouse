# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Land a batch
# MAGIC
# MAGIC Writes generator batches into the landing volume, the way an upstream system would deliver files.
# MAGIC The data is rebuilt from a fixed seed every time, so the same batch always has the same content.
# MAGIC
# MAGIC | `batches` widget | Lands |
# MAGIC |---|---|
# MAGIC | `next` (default) | the first batch not landed yet |
# MAGIC | `0` | the one-year backfill (2025-09-01 .. 2026-08-31) |
# MAGIC | `1-3` | daily batches 1 to 3 (2026-09-01 .. 2026-09-03) |
# MAGIC | `all` | everything (backfill + 14 daily batches) |
# MAGIC
# MAGIC Batch 7 also re-sends batch 6's orders file, to test de-duplication.
# MAGIC After landing, run the pipeline (Jobs & Pipelines → `retail_lakehouse` → Run pipeline).

# COMMAND ----------

dbutils.widgets.text("batches", "next")
dbutils.widgets.text("landing_root", "/Volumes/retail/raw/landing")

# COMMAND ----------

import json
import os
import sys

repo_root = os.path.abspath("..")          # this notebook lives in <repo>/notebooks
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from generator import Config, build_world, landed_batches, plan_batches, write_batch
from generator.__main__ import select_batches

landing_root = dbutils.widgets.get("landing_root")
cfg = Config()
batches = plan_batches(cfg)
already = landed_batches(landing_root)
wanted = select_batches(dbutils.widgets.get("batches"), already, len(batches))
print(f"Already landed: {already or 'none'}")
print(f"Landing now:    {[batches[i].batch_id for i in wanted] or 'nothing'}")

# COMMAND ----------

world = build_world(cfg)
summary = []
for i in wanted:
    m = write_batch(world, batches[i], landing_root)
    summary.append({
        "batch_id": m["batch_id"],
        "days": f'{m["first_day"]} .. {m["last_day"]}',
        "order_lines": m["orders"]["order_lines_arrived"],
        "bad_lines": sum(m["orders"]["rule_failures"].values()),
        "duplicate_lines": m["orders"]["duplicate_lines"],
        "orphan_lines": m["orders"]["orphan_product_lines"],
        "returns": m["returns"]["records_arrived"],
        "bad_returns": sum(m["returns"]["rule_failures"].values()),
        "orphan_returns": m["returns"]["orphan_returns"],
    })

if summary:
    display(spark.createDataFrame(summary))

# COMMAND ----------

# MAGIC %md What is in the volume now:

# COMMAND ----------

display(spark.sql(f"LIST '{landing_root}/_manifest'"))
