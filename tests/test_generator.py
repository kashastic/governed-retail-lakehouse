"""The generator is deterministic and its manifests tell the truth about its files."""
import hashlib
import json
from collections import Counter
from pathlib import Path

from generator import Config, build_world, plan_batches, write_batch
from generator.config import ORDER_DEFECT_RULES, RETURN_DEFECT_RULES
from utilities import rules


def _digest(root: Path) -> dict[str, str]:
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


def test_same_seed_gives_identical_files(world, tmp_path):
    batch = plan_batches(world.cfg)[3]
    write_batch(world, batch, tmp_path / "a")
    write_batch(build_world(Config()), batch, tmp_path / "b")
    assert _digest(tmp_path / "a") == _digest(tmp_path / "b")


def test_batch_plan_covers_backfill_and_increments():
    cfg = Config()
    batches = plan_batches(cfg)
    assert len(batches) == 1 + cfg.increment_days
    assert batches[0].first_day.isoformat() == "2025-09-01"
    assert batches[0].last_day.isoformat() == "2026-08-31"
    assert [b.first_day.isoformat() for b in batches[1:3]] == ["2026-09-01", "2026-09-02"]


def test_manifest_rule_names_match_pipeline_rules():
    assert set(ORDER_DEFECT_RULES.values()) <= set(rules.ORDER_LINE_RULES)
    assert set(RETURN_DEFECT_RULES.values()) <= set(rules.RETURN_RULES)


def _line_failure(order: dict, item: dict) -> str | None:
    """Independent re-implementation of the order-line rules, in plain Python."""
    if order["order_id"] is None:
        return "valid_order_id"
    if order["customer_id"] is None:
        return "valid_customer_id"
    if item["quantity"] is None or not 1 <= item["quantity"] <= 50:
        return "valid_quantity"
    if item["unit_price"] is None or item["unit_price"] <= 0:
        return "valid_unit_price"
    return None


def test_manifest_matches_files_including_resent_file(world, tmp_path):
    """Recount batches 6 and 7 from the files themselves. Batch 7 carries a re-sent copy of batch 6."""
    batches = plan_batches(world.cfg)
    seen: set[str] = set()
    for batch in batches[world.cfg.resent_batch: world.cfg.resent_batch + 2]:
        manifest = write_batch(world, batch, tmp_path)
        failures, duplicates, arrived = Counter(), 0, 0
        for path in sorted((tmp_path / "orders" / batch.batch_id).glob("*.json")):
            for raw in path.read_text().splitlines():
                order = json.loads(raw)
                for item in order["items"]:
                    arrived += 1
                    rule = _line_failure(order, item)
                    if rule:
                        failures[rule] += 1
                        continue
                    key = json.dumps([order["order_id"], order["customer_id"], order["order_ts"], item],
                                     sort_keys=True)
                    duplicates += key in seen
                    seen.add(key)
        stats = manifest["orders"]
        assert stats["order_lines_arrived"] == arrived
        assert {k: v for k, v in stats["rule_failures"].items() if v} == dict(failures)
        assert stats["duplicate_lines"] == duplicates
    assert len(list((tmp_path / "orders" / "batch_007").glob("*resent*"))) == 1


def test_returns_are_consistent_with_orders(world):
    end = world.end_date
    lines = {(o.order_id, l.line_no): (o, l) for o in world.orders for l in o.lines}
    for r in world.returns:
        if r.orphan:
            assert (r.order_id, r.line_no) not in lines
            continue
        order, line = lines[(r.order_id, r.line_no)]
        assert order.defect is None and line.defect is None
        assert 1 <= r.quantity <= line.quantity
        assert order.order_ts < r.return_ts and r.return_ts.date() <= end


def test_return_rates_follow_category_profile(world):
    sold, returned = Counter(), Counter()
    lines = {(o.order_id, l.line_no): l for o in world.orders for l in o.lines}
    for o in world.orders:
        if o.defect is None:
            for l in o.lines:
                if l.defect is None:
                    sold[l.product.category.name] += l.quantity
    for r in world.returns:
        if not r.orphan:
            returned[lines[(r.order_id, r.line_no)].product.category.name] += r.quantity
    rate = {c: returned[c] / sold[c] for c in sold}
    assert rate["Clothing"] > rate["Electronics"] > rate["Grocery"]
    assert 0.15 < rate["Clothing"] < 0.26 and rate["Grocery"] < 0.02


def test_dataset_size_is_in_expected_range(world):
    n_lines = sum(len(o.lines) for o in world.orders)
    assert 140_000 < len(world.orders) < 180_000
    assert 300_000 < n_lines < 380_000
    assert len(world.customers) == 20_000 and len(world.products) == 1_440
