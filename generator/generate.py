"""Seeded synthetic data for a fictional general online retailer.

The generator builds the whole world (products, customers, orders, returns) from
one seed, then slices it into landing batches: batch_000 is a one-year backfill
and every later batch is one day. Known defects are injected into the order and
return feeds, and each batch writes a manifest recording exactly what was
injected. The pipeline's data-quality results are checked against it.

Only the standard library is used, so the same code runs locally and in a
Databricks notebook (Free Edition has limited outbound internet, so nothing is
downloaded).
"""
from __future__ import annotations

import bisect
import csv
import io
import json
import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path

from .config import (CATEGORIES, COUNTRIES, FIRST_NAMES, LAST_NAMES, MEMBERSHIPS,
                     ORDER_DEFECT_RULES, RETURN_DEFECT_RULES, Category, Config)

TS_FORMAT = "%Y-%m-%dT%H:%M:%S"

WEEKDAY_FACTOR = (0.95, 0.95, 0.97, 1.00, 1.08, 1.15, 1.10)   # Monday .. Sunday
MONTH_FACTOR = {1: 0.85, 2: 0.85, 3: 0.95, 4: 1.00, 5: 1.00, 6: 0.95, 7: 0.95,
                8: 0.95, 9: 0.95, 10: 1.00, 11: 1.25, 12: 1.45}
SPECIAL_DAYS = {date(2025, 11, 28): 2.5, date(2025, 12, 1): 1.8}  # Black Friday, Cyber Monday
HOUR_WEIGHTS = (1, 1, 1, 1, 1, 2, 3, 5, 6, 6, 6, 6, 7, 6, 6, 6, 7, 8, 10, 11, 11, 9, 6, 3)
LINES_PER_ORDER = ((1, 0.42), (2, 0.27), (3, 0.15), (4, 0.09), (5, 0.05), (6, 0.02))
QTY_GROCERY = ((1, 0.50), (2, 0.30), (3, 0.12), (4, 0.08))
QTY_DEFAULT = ((1, 0.86), (2, 0.11), (3, 0.03))
DISCOUNTS = (0.10, 0.15, 0.20, 0.25)
CHANNELS = (("web", 0.55), ("app", 0.45))


# --------------------------------------------------------------------------- model

@dataclass
class Product:
    product_id: str
    name: str
    category: Category
    brand: str
    prices: list[tuple[datetime, float]]          # (effective from, list price), sorted
    _starts: list[datetime] = field(default_factory=list, repr=False)

    def price_at(self, ts: datetime) -> float:
        if not self._starts:
            self._starts = [p[0] for p in self.prices]
        return self.prices[bisect.bisect_right(self._starts, ts) - 1][1]


@dataclass
class Customer:
    customer_id: str
    first_name: str
    last_name: str
    email: str
    city: str
    country: str
    membership: str
    signup_ts: datetime
    weight: float
    # CDC events: (change_ts, operation, attribute snapshot)
    events: list[tuple[datetime, str, dict]] = field(default_factory=list)


@dataclass
class Line:
    line_no: int
    product: Product
    quantity: int
    unit_price: float
    discount_amount: float
    defect: str | None = None       # invalid_quantity | invalid_unit_price | orphan_product
    bad_value: object = None


@dataclass
class Order:
    order_id: str
    customer: Customer
    order_ts: datetime
    channel: str
    lines: list[Line]
    defect: str | None = None       # missing_order_id | missing_customer_id
    duplicated: bool = False

    @property
    def is_fully_clean(self) -> bool:
        return self.defect is None and all(l.defect is None for l in self.lines)


@dataclass
class Return:
    return_id: str
    order_id: str
    line_no: int
    product_id: str
    return_ts: datetime
    quantity: int
    reason: str
    refund_amount: float
    orphan: bool = False
    defect: str | None = None       # missing_return_id | invalid_return_quantity
    bad_value: object = None
    duplicated: bool = False


@dataclass(frozen=True)
class Batch:
    index: int
    batch_id: str
    first_day: date
    last_day: date                  # inclusive

    @property
    def is_backfill(self) -> bool:
        return self.index == 0


@dataclass
class World:
    cfg: Config
    products: list[Product]
    customers: list[Customer]
    orders: list[Order]
    returns: list[Return]

    @property
    def end_date(self) -> date:
        return self.cfg.start_date + timedelta(days=self.cfg.total_days - 1)


# --------------------------------------------------------------------------- helpers

def _pick(rng: random.Random, weighted: tuple[tuple[object, float], ...]):
    values, weights = zip(*weighted)
    return rng.choices(values, weights=weights)[0]


def _price(rng: random.Random, lo: float, hi: float) -> float:
    return max(0.99, round(rng.uniform(lo, hi)) - 0.01)


def _cum(weights: list[float]) -> list[float]:
    out, total = [], 0.0
    for w in weights:
        total += w
        out.append(total)
    return out


def plan_batches(cfg: Config) -> list[Batch]:
    backfill_end = cfg.start_date + timedelta(days=cfg.backfill_days - 1)
    batches = [Batch(0, "batch_000", cfg.start_date, backfill_end)]
    for i in range(1, cfg.increment_days + 1):
        day = backfill_end + timedelta(days=i)
        batches.append(Batch(i, f"batch_{i:03d}", day, day))
    return batches


# --------------------------------------------------------------------------- world

def _make_products(rng: random.Random, cfg: Config) -> list[Product]:
    start = datetime.combine(cfg.start_date, time()) - timedelta(days=1)
    window = (cfg.total_days - 1) * 86400
    products, n = [], 0
    for cat in CATEGORIES:
        for _ in range(cfg.products_per_category):
            n += 1
            brand = rng.choice(cat.brands)
            name = f"{brand} {rng.choice(cat.nouns)} {rng.choice('ABCDEFGHKMRSTX')}{rng.randint(10, 99)}"
            price = _price(rng, *cat.price_range)
            prices = [(start, price)]
            if rng.random() < cfg.price_change_rate:
                changed = datetime.combine(cfg.start_date, time()) + timedelta(seconds=rng.randrange(window))
                new_price = max(0.99, round(price * rng.uniform(0.75, 1.20)) - 0.01)
                prices.append((changed, new_price))
            products.append(Product(f"P{n:05d}", name, cat, brand, prices))
    return products


def _make_customers(rng: random.Random, cfg: Config) -> list[Customer]:
    start = datetime.combine(cfg.start_date, time())
    end = start + timedelta(days=cfg.total_days)
    signups = [start - timedelta(seconds=rng.randrange(3 * 365 * 86400)) for _ in range(cfg.initial_customers)]
    signups += [start + timedelta(seconds=rng.randrange(cfg.total_days * 86400)) for _ in range(cfg.new_customers)]
    signups.sort()

    customers = []
    for i, signup in enumerate(signups, start=1):
        country, _, cities = _pick(rng, tuple(((c, w, cs), w) for c, w, cs in COUNTRIES))
        membership = _pick(rng, tuple((m, share) for m, share, _ in MEMBERSHIPS))
        frequency = next(f for m, _, f in MEMBERSHIPS if m == membership)
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        c = Customer(f"C{i:06d}", first, last, f"{first}.{last}.{i}@example.com".lower(),
                     rng.choice(cities), country, membership, signup,
                     frequency * rng.lognormvariate(0, 0.9))
        c.events.append((signup, "INSERT", _customer_snapshot(c)))

        # Changes happen inside the data window, at least 30 days after signup.
        earliest = max(signup + timedelta(days=30), start)
        span = int((end - earliest).total_seconds())
        changes = []
        if span > 86400:
            if membership != "Premium" and rng.random() < cfg.upgrade_rate:
                nxt = "Plus" if membership == "Basic" else "Premium"
                changes.append((earliest + timedelta(seconds=rng.randrange(span)), "membership", nxt))
            if rng.random() < cfg.move_rate:
                if rng.random() < 0.2:
                    new_country, _, new_cities = rng.choice(COUNTRIES)
                else:
                    new_country, new_cities = country, next(cs for cc, _, cs in COUNTRIES if cc == country)
                changes.append((earliest + timedelta(seconds=rng.randrange(span)), "move",
                                (new_country, rng.choice(new_cities))))
        for ts, kind, value in sorted(changes, key=lambda x: x[0]):
            if kind == "membership":
                c.membership = value
            else:
                c.country, c.city = value
            c.events.append((ts, "UPDATE", _customer_snapshot(c)))
        customers.append(c)
    return customers


def _customer_snapshot(c: Customer) -> dict:
    return {"membership": c.membership, "city": c.city, "country": c.country}


def _orders_on(day: date, cfg: Config, rng: random.Random) -> int:
    t = (day - cfg.start_date).days
    factor = (WEEKDAY_FACTOR[day.weekday()] * MONTH_FACTOR[day.month]
              * SPECIAL_DAYS.get(day, 1.0) * (1 + 0.12 * t / 365))
    return int(round(cfg.base_orders_per_day * factor * rng.uniform(0.9, 1.1)))


def _make_orders(rng: random.Random, cfg: Config, customers: list[Customer],
                 products: list[Product]) -> list[Order]:
    signups = [c.signup_ts for c in customers]
    cust_cum = _cum([c.weight for c in customers])
    by_cat = {cat.name: [p for p in products if p.category.name == cat.name] for cat in CATEGORIES}
    # Zipf-like popularity inside each category.
    prod_cum = {name: _cum([1 / (r + 1) ** 1.1 for r in range(len(ps))]) for name, ps in by_cat.items()}
    cat_names = [c.name for c in CATEGORIES]
    cat_cum = _cum([c.line_share for c in CATEGORIES])
    hours = list(range(24))

    orders: list[Order] = []
    for t in range(cfg.total_days):
        day = cfg.start_date + timedelta(days=t)
        midnight = datetime.combine(day, time())
        eligible = bisect.bisect_left(signups, midnight)
        n = _orders_on(day, cfg, rng)
        buyers = rng.choices(customers[:eligible], cum_weights=cust_cum[:eligible], k=n)
        day_orders = []
        for customer in buyers:
            ts = midnight + timedelta(hours=rng.choices(hours, weights=HOUR_WEIGHTS)[0],
                                      seconds=rng.randrange(3600))
            n_lines = _pick(rng, LINES_PER_ORDER)
            lines, seen = [], set()
            for _ in range(n_lines):
                cat = rng.choices(cat_names, cum_weights=cat_cum)[0]
                product = rng.choices(by_cat[cat], cum_weights=prod_cum[cat])[0]
                if product.product_id in seen:
                    continue
                seen.add(product.product_id)
                qty = _pick(rng, QTY_GROCERY if cat == "Grocery" else QTY_DEFAULT)
                unit = product.price_at(ts)
                discount = 0.0
                if rng.random() < cfg.discount_rate:
                    discount = round(qty * unit * rng.choice(DISCOUNTS), 2)
                lines.append(Line(len(lines) + 1, product, qty, unit, discount))
            day_orders.append(Order("", customer, ts, _pick(rng, CHANNELS), lines))
        day_orders.sort(key=lambda o: o.order_ts)
        for o in day_orders:
            o.order_id = f"O{len(orders) + 1:07d}"
            orders.append(o)
    return orders


def _assign_order_defects(rng: random.Random, cfg: Config, orders: list[Order]) -> None:
    """Each defective row breaks exactly one rule, so manifest counts per rule are exact."""
    for o in orders:
        r = rng.random()
        if r < cfg.missing_order_id_rate:
            o.defect = "missing_order_id"
        elif r < cfg.missing_order_id_rate + cfg.missing_customer_id_rate:
            o.defect = "missing_customer_id"
        else:
            for line in o.lines:
                r2 = rng.random()
                if r2 < cfg.invalid_quantity_rate:
                    line.defect, line.bad_value = "invalid_quantity", rng.choice((0, -1, -2, 75, 99))
                elif r2 < cfg.invalid_quantity_rate + cfg.invalid_unit_price_rate:
                    line.defect, line.bad_value = "invalid_unit_price", rng.choice((0.0, -line.unit_price))
                elif r2 < cfg.invalid_quantity_rate + cfg.invalid_unit_price_rate + cfg.orphan_product_rate:
                    line.defect, line.bad_value = "orphan_product", f"P9{rng.randrange(10000):04d}"
        # Duplicates are drawn only from fully clean orders.
        if o.is_fully_clean and rng.random() < cfg.duplicate_order_rate:
            o.duplicated = True


def _make_returns(rng: random.Random, cfg: Config, orders: list[Order],
                  products: list[Product], end: date) -> list[Return]:
    returns: list[Return] = []
    for o in orders:
        if o.defect:
            continue
        for line in o.lines:
            if line.defect:          # only clean lines are returned
                continue
            cat = line.product.category
            p = cat.return_rate * (1.2 if line.quantity > 1 else 1.0)
            if rng.random() >= p:
                continue
            ts = o.order_ts + timedelta(days=rng.triangular(1, 30, 7))
            if ts.date() > end:      # not returned yet when the data ends
                continue
            qty = 1 if line.quantity == 1 else rng.randint(1, line.quantity)
            net_unit = (line.quantity * line.unit_price - line.discount_amount) / line.quantity
            returns.append(Return("", o.order_id, line.line_no, line.product.product_id, ts, qty,
                                  _pick(rng, cat.reasons), round(qty * net_unit, 2)))
            if rng.random() < cfg.orphan_return_rate:
                other = rng.choice(products)
                orphan_ts = min(ts + timedelta(hours=rng.randint(1, 6)),
                                datetime.combine(ts.date(), time(23, 59, 59)))
                returns.append(Return("", f"O9{rng.randrange(10**6):06d}", 1, other.product_id,
                                      orphan_ts, 1, _pick(rng, other.category.reasons),
                                      other.price_at(orphan_ts), orphan=True))
    returns.sort(key=lambda r: r.return_ts)
    for i, r in enumerate(returns, start=1):
        r.return_id = f"R{i:07d}"
    return returns


def _assign_return_defects(rng: random.Random, cfg: Config, returns: list[Return]) -> None:
    for r in returns:
        if r.orphan:
            continue
        x = rng.random()
        if x < cfg.missing_return_id_rate:
            r.defect = "missing_return_id"
        elif x < cfg.missing_return_id_rate + cfg.invalid_return_quantity_rate:
            r.defect, r.bad_value = "invalid_return_quantity", rng.choice((0, -1))
        elif rng.random() < cfg.duplicate_return_rate:
            r.duplicated = True


def build_world(cfg: Config | None = None) -> World:
    cfg = cfg or Config()
    end = cfg.start_date + timedelta(days=cfg.total_days - 1)
    rng = random.Random(cfg.seed)
    products = _make_products(rng, cfg)
    customers = _make_customers(rng, cfg)
    orders = _make_orders(rng, cfg, customers, products)
    _assign_order_defects(random.Random(f"{cfg.seed}-order-defects"), cfg, orders)
    returns = _make_returns(random.Random(f"{cfg.seed}-returns"), cfg, orders, products, end)
    _assign_return_defects(random.Random(f"{cfg.seed}-return-defects"), cfg, returns)
    return World(cfg, products, customers, orders, returns)


# --------------------------------------------------------------------------- serialisation

def order_record(o: Order) -> dict:
    items = []
    for line in o.lines:
        items.append({
            "line_no": line.line_no,
            "product_id": line.bad_value if line.defect == "orphan_product" else line.product.product_id,
            "quantity": line.bad_value if line.defect == "invalid_quantity" else line.quantity,
            "unit_price": line.bad_value if line.defect == "invalid_unit_price" else line.unit_price,
            "discount_amount": line.discount_amount,
        })
    return {
        "order_id": None if o.defect == "missing_order_id" else o.order_id,
        "customer_id": None if o.defect == "missing_customer_id" else o.customer.customer_id,
        "order_ts": o.order_ts.strftime(TS_FORMAT),
        "channel": o.channel,
        "currency": "EUR",
        "items": items,
    }


def return_record(r: Return) -> dict:
    return {
        "return_id": None if r.defect == "missing_return_id" else r.return_id,
        "order_id": r.order_id,
        "line_no": r.line_no,
        "product_id": r.product_id,
        "return_ts": r.return_ts.strftime(TS_FORMAT),
        "quantity": r.bad_value if r.defect == "invalid_return_quantity" else r.quantity,
        "reason": r.reason,
        "refund_amount": r.refund_amount,
    }


def _jsonl(records: list[dict]) -> str:
    return "".join(json.dumps(rec, separators=(",", ":")) + "\n" for rec in records)


def _csv(header: list[str], rows: list[list]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue()


def orders_jsonl(orders: list[Order]) -> str:
    """One JSON object per line. Duplicated orders are written again at the end of the file."""
    return _jsonl([order_record(o) for o in orders] + [order_record(o) for o in orders if o.duplicated])


def returns_jsonl(returns: list[Return]) -> str:
    return _jsonl([return_record(r) for r in returns] + [return_record(r) for r in returns if r.duplicated])


CUSTOMER_HEADER = ["customer_id", "email", "first_name", "last_name", "city", "country",
                   "membership", "signup_date", "operation", "change_ts"]
PRODUCT_HEADER = ["product_id", "product_name", "category", "brand", "list_price", "operation", "change_ts"]


def customers_csv(events: list[tuple[Customer, datetime, str, dict]]) -> str:
    rows = [[c.customer_id, c.email, c.first_name, c.last_name, snap["city"], snap["country"],
             snap["membership"], c.signup_ts.date().isoformat(), op, ts.strftime(TS_FORMAT)]
            for c, ts, op, snap in events]
    return _csv(CUSTOMER_HEADER, rows)


def products_csv(events: list[tuple[Product, datetime, str, float]]) -> str:
    rows = [[p.product_id, p.name, p.category.name, p.brand, f"{price:.2f}", op, ts.strftime(TS_FORMAT)]
            for p, ts, op, price in events]
    return _csv(PRODUCT_HEADER, rows)


# --------------------------------------------------------------------------- batches

def _in(batch: Batch, ts: datetime, backfill_includes_history: bool = False) -> bool:
    d = ts.date()
    if batch.is_backfill and backfill_includes_history:
        return d <= batch.last_day
    return batch.first_day <= d <= batch.last_day


def batch_orders(world: World, batch: Batch) -> list[Order]:
    return [o for o in world.orders if _in(batch, o.order_ts)]


def batch_returns(world: World, batch: Batch) -> list[Return]:
    return [r for r in world.returns if _in(batch, r.return_ts)]


def batch_customer_events(world: World, batch: Batch):
    return sorted(((c, ts, op, snap) for c in world.customers for ts, op, snap in c.events
                   if _in(batch, ts, backfill_includes_history=True)), key=lambda e: e[1])


def batch_product_events(world: World, batch: Batch):
    events = []
    for p in world.products:
        for i, (ts, price) in enumerate(p.prices):
            if _in(batch, ts, backfill_includes_history=True):
                events.append((p, ts, "INSERT" if i == 0 else "UPDATE", price))
    return sorted(events, key=lambda e: (e[1], e[0].product_id))


def _order_stats(orders: list[Order], resent: bool = False) -> dict:
    """Ground truth for one orders file, in exploded order lines."""
    arrived, duplicates, orphans = 0, 0, 0
    failures = Counter({rule: 0 for rule in ORDER_DEFECT_RULES.values()})
    for o in orders:
        copies = 2 if o.duplicated else 1
        for line in o.lines:
            arrived += copies
            defect = o.defect or (line.defect if line.defect != "orphan_product" else None)
            if defect:
                failures[ORDER_DEFECT_RULES[defect]] += copies
            elif resent:
                duplicates += copies     # every passing line in a re-sent file was already seen
            else:
                duplicates += copies - 1
                orphans += line.defect == "orphan_product"
    return {"records": sum(2 if o.duplicated else 1 for o in orders), "order_lines_arrived": arrived,
            "rule_failures": dict(failures), "duplicate_lines": duplicates, "orphan_product_lines": orphans}


def _return_stats(returns: list[Return]) -> dict:
    failures = Counter({rule: 0 for rule in RETURN_DEFECT_RULES.values()})
    for r in returns:
        if r.defect:
            failures[RETURN_DEFECT_RULES[r.defect]] += 1
    return {"records_arrived": len(returns) + sum(r.duplicated for r in returns),
            "rule_failures": dict(failures),
            "duplicate_records": sum(r.duplicated for r in returns),
            "orphan_returns": sum(r.orphan for r in returns)}


def _merge(a: dict, b: dict) -> dict:
    out = dict(a)
    for k, v in b.items():
        out[k] = _merge(a[k], v) if isinstance(v, dict) else a[k] + v
    return out


def write_batch(world: World, batch: Batch, root: str | Path) -> dict:
    """Write one batch's files under root and return its manifest (also written to root/_manifest)."""
    root = Path(root)
    files: dict[str, str] = {}
    orders = batch_orders(world, batch)

    if batch.is_backfill:
        months: dict[str, list[Order]] = {}
        for o in orders:
            months.setdefault(o.order_ts.strftime("%Y-%m"), []).append(o)
        for month, chunk in months.items():
            files[f"orders/{batch.batch_id}/orders_{month}.json"] = orders_jsonl(chunk)
    else:
        files[f"orders/{batch.batch_id}/orders.json"] = orders_jsonl(orders)
    order_stats = _order_stats(orders)

    if batch.index == world.cfg.resent_batch + 1:
        previous = plan_batches(world.cfg)[world.cfg.resent_batch]
        resent = batch_orders(world, previous)
        files[f"orders/{batch.batch_id}/orders_resent_{previous.batch_id}.json"] = orders_jsonl(resent)
        order_stats = _merge(order_stats, _order_stats(resent, resent=True))

    returns = batch_returns(world, batch)
    files[f"returns/{batch.batch_id}/returns.json"] = returns_jsonl(returns)
    cust_events = batch_customer_events(world, batch)
    files[f"customers/{batch.batch_id}/customers.csv"] = customers_csv(cust_events)
    prod_events = batch_product_events(world, batch)
    files[f"products/{batch.batch_id}/products.csv"] = products_csv(prod_events)

    manifest = {
        "batch_id": batch.batch_id,
        "first_day": batch.first_day.isoformat(),
        "last_day": batch.last_day.isoformat(),
        "seed": world.cfg.seed,
        "files": sorted(files),
        "orders": order_stats,
        "returns": _return_stats(returns),
        "customers": dict(Counter(op for _, _, op, _ in cust_events)),
        "products": dict(Counter(op for _, _, op, _ in prod_events)),
    }
    files[f"_manifest/{batch.batch_id}.json"] = json.dumps(manifest, indent=2) + "\n"

    for rel, content in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    return manifest


def landed_batches(root: str | Path) -> list[str]:
    manifest_dir = Path(root) / "_manifest"
    return sorted(p.stem for p in manifest_dir.glob("batch_*.json")) if manifest_dir.exists() else []
