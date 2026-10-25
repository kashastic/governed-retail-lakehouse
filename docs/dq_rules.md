# Data-quality rules and failure handling

Every rule has one action, chosen by asking: *is the row unusable, usable but suspicious, or a sign that the
pipeline itself is wrong?*

| Action | When | Mechanism | Where the rows go |
|---|---|---|---|
| **Drop + quarantine** | The row can't be used: no key, impossible value | `expect_all_or_drop` on a private validated table + a quarantine table driven by the same rules dict | `silver.*_quarantine`, with `failed_rules` |
| **Remove silently, count** | Exact copy of a row already received (re-send) | content hash; a materialized view keeps the first arrival of each hash | nowhere: counted by reconciliation |
| **Keep + flag (warn)** | The row is valid but a reference is missing | left join to unknown member + `expect` (warn) | gold, with `is_orphan_*` = true |
| **Fail the update** | The output is wrong whatever the input was | `expect_or_fail` on a key-check view | the update stops; nothing downstream refreshes from it |

## Rules

| Feed | Rule | Condition | Action | Why |
|---|---|---|---|---|
| order lines | `valid_order_id` | `order_id IS NOT NULL` | drop + quarantine | No order to attach the line to |
| order lines | `valid_customer_id` | `customer_id IS NOT NULL` | drop + quarantine | Can't attribute the sale; guest checkout doesn't exist in this shop |
| order lines | `valid_order_ts` | `order_ts IS NOT NULL` (unparseable → NULL) | drop + quarantine | No period to report it in |
| order lines | `valid_product_id` | `product_id IS NOT NULL` | drop + quarantine | No product at all (an *unknown* id is handled below) |
| order lines | `valid_quantity` | `quantity BETWEEN 1 AND 50` | drop + quarantine | 0 or negative is impossible; > 50 per line is a bulk-order system error in this shop |
| order lines | `valid_unit_price` | `unit_price > 0` | drop + quarantine | Free or negative prices would distort revenue and AOV |
| returns | `valid_return_id` | `return_id IS NOT NULL` | drop + quarantine | Can't de-duplicate or trace it |
| returns | `valid_order_reference` | `order_id`, `line_no` not null | drop + quarantine | Can't attach the return to anything |
| returns | `valid_return_ts` | parseable timestamp | drop + quarantine | No period |
| returns | `valid_return_quantity` | `quantity > 0` | drop + quarantine | Impossible value |
| returns | `valid_refund_amount` | `refund_amount >= 0` | drop + quarantine | Negative refund is a sign error upstream |
| customers / products CDC | `valid_*_id`, `valid_change_ts`, `valid_operation`, `valid_list_price` | not null / known operation / price > 0 | drop (expectation on the CDC view) | AUTO CDC needs a key and a sequence to order changes |
| order lines (gold) | `known_product` | product exists in catalogue | keep + flag, `UNKNOWN` product | Revenue is real; the catalogue is late or wrong |
| order lines (gold) | `known_customer_version` | a customer version covers the order time | keep + flag | Never happens in this data; would mean CDC arrived late |
| order lines (gold) | `returned_within_sold_quantity` | `returned_quantity <= quantity` | warn | Over-returns point to duplicate returns upstream |
| returns (gold) | `known_order_line` | the order line exists | keep + flag, unknown members | The refund was paid; finance still needs it |
| gold keys | `unique_order_line_id`, `unique_return_id`, `unique_customer_key` | no duplicate keys | **fail** | Two different records for one key, or a join fan-out: a bug or a conflict a person must resolve |

## Why exact duplicates are removed but key conflicts fail

De-duplication is by **content hash**, not by business key. An exact re-send (same order line, same values) is
harmless and removed. Two *different* records claiming the same `order_id` + `line_no` are not removed. Both
reach gold, and `unique_order_line_id` fails the update. Silently keeping "the latest" would hide an upstream bug.

## Why expectations run before de-duplication

The private `order_lines_validated` table carries the expectations and de-duplication happens after it. The event
log therefore counts every bad row that *arrived*, including bad rows inside a re-sent file. That makes three
numbers agree exactly: the generator manifest, the expectation metrics and the quarantine row counts.

## Why de-duplication is a materialized view

The first version removed re-sends inside a streaming table with `dropDuplicatesWithinWatermark` (3-day watermark).
On Databricks, the gold fact then lagged exactly one pipeline update behind silver:

| Update | Landed | `silver.order_lines` committed | `gold.fact_order_line` wrote |
|---|---|---|---|
| 1 | backfill | 325,070 rows | 325,070 ✅ |
| 3 | batch 1 | +796 rows at 15:19:00 | 325,070 ❌ (planned 15:19:26) |
| 4 | batches 2–7 | +5,197 rows at 15:24:14 | 325,866 ❌ (planned 15:24:35) |

Silver was correct (the validation notebook reconciled it exactly), and the pipeline graph listed silver as an input
to the fact. Still, the fact's full recompute read the snapshot from before the update, even though the new rows had
been committed 20 seconds earlier. The materialized views refreshed incrementally (`fact_return`, `dim_customer`)
were current. The stateful stream also wrote a second, empty commit at the end of every update (the watermark
batch), which nothing else in the pipeline does.

The fix removes all streaming state: the validated table stays an append-only stream with the expectations, and a
materialized view keeps the first arrival of each content hash (`row_number()` over the hash, ordered by load time
and file). Side benefit: re-sends are now caught however late they arrive, not only within 3 days.
`notebooks/04_validate.py` is the regression check. "Net sales preserved silver -> gold" fails if gold falls behind
silver again.

**Verified on Databricks (2026-10-25).** The two silver streaming tables had to be dropped once: a pipeline can't
change a dataset from streaming table to materialized view in place. After that, a full refresh, then ordinary
updates with new batches:

| Step | `silver.order_lines` | Net sales silver = gold | Orphan lines / returns in gold | `04_validate` |
|---|---|---|---|---|
| Full refresh (batches 0–7) | 331,063 | €49,983,899.72 | 964 / 93 | 11/11 PASS |
| Ordinary update with batch 8 (+839 valid lines) | 331,902 | €50,096,231.15 | 968 / 94 | 11/11 PASS |
| Ordinary update with batches 9–14 (all 15 batches) | 337,289 | €50,889,830.02 | 983 / 95 | 23/23 PASS, incl. the 12 metric-view checks |

In the batch-8 update `fact_order_line` was again a full recompute, the refresh type that read stale data before,
and this time it included the new rows in the same update. The de-duplication views refreshed incrementally.

## Ground truth after all 15 batches

| | Order lines | Returns |
|---|---|---|
| Arrived | 343,151 | 31,730 |
| Quarantined | 3,105 (`valid_order_id` 703, `valid_customer_id` 1,010, `valid_quantity` 1,019, `valid_unit_price` 373) | 155 (`valid_return_id` 52, `valid_return_quantity` 103) |
| Exact duplicates removed | 2,757 (incl. the re-sent batch 6 file) | 169 |
| In silver | 337,289 | 31,406 |
| Kept but flagged as orphans | 983 | 95 |

Reconciliation: `arrived = silver + quarantined + duplicates`. Checked by `notebooks/04_validate.py` in Databricks
and `tests/test_end_to_end.py` locally. On Databricks (2026-10-25) every number in this table matched exactly,
per rule as well as in total.

## Known limits

- Quarantined rows are not reprocessed automatically. A fix upstream re-delivers the record, which flows through
  normally.
- The de-duplication view looks at all validated history on each refresh. At this size it costs seconds. At real
  scale it would be limited to a recent window per partition, or replaced by AUTO CDC keyed on the hash.
- Validation views don't block downstream tables inside the same pipeline. A failed key check stops the update,
  but to gate a separate consumer you'd put the checks in their own job task.
