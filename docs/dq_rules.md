# Data-quality rules and failure handling

Every rule has one action, chosen by asking: *is the row unusable, usable but suspicious, or a sign that the
pipeline itself is wrong?*

| Action | When | Mechanism | Where the rows go |
|---|---|---|---|
| **Drop + quarantine** | The row can't be used: no key, impossible value | `expect_all_or_drop` on a private validated table + a quarantine table driven by the same rules dict | `silver.*_quarantine`, with `failed_rules` |
| **Remove silently, count** | Exact copy of a row already received (re-send) | content hash + `dropDuplicatesWithinWatermark` (3-day watermark) | nowhere: counted by reconciliation |
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

## Ground truth after all 15 batches

| | Order lines | Returns |
|---|---|---|
| Arrived | 343,151 | 31,730 |
| Quarantined | 3,105 (`valid_order_id` 703, `valid_customer_id` 1,010, `valid_quantity` 1,019, `valid_unit_price` 373) | 155 (`valid_return_id` 52, `valid_return_quantity` 103) |
| Exact duplicates removed | 2,757 (incl. the re-sent batch 6 file) | 169 |
| In silver | 337,289 | 31,406 |
| Kept but flagged as orphans | 983 | 95 |

Reconciliation: `arrived = silver + quarantined + duplicates`. Checked by `notebooks/04_validate.py` in Databricks
and `tests/test_end_to_end.py` locally.

## Known limits

- Quarantined rows are not reprocessed automatically. A fix upstream re-delivers the record, which flows through
  normally.
- The watermark forgets hashes older than 3 days of event time. A copy re-sent more than 3 days later would pass
  de-duplication, and the gold key check would then fail the update. That is the intended backstop.
- Validation views don't block downstream tables inside the same pipeline. A failed key check stops the update,
  but to gate a separate consumer you'd put the checks in their own job task.
