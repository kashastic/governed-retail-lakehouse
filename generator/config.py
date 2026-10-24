"""Every number that shapes the synthetic dataset.

The generator, the tests and the docs all read from here, so a change in one
place shows up everywhere.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Category:
    name: str
    line_share: float                 # share of order lines in this category
    price_range: tuple[float, float]  # list price range in EUR
    return_rate: float                # probability that a clean line is (partly) returned
    reasons: tuple[tuple[str, float], ...]
    nouns: tuple[str, ...]
    brands: tuple[str, ...]


_FIT = (("size_or_fit", 0.55), ("changed_mind", 0.20), ("not_as_described", 0.15),
        ("damaged_in_transit", 0.05), ("wrong_item", 0.05))
_TECH = (("defective", 0.40), ("changed_mind", 0.25), ("not_as_described", 0.20),
         ("damaged_in_transit", 0.10), ("wrong_item", 0.05))
_GENERAL = (("changed_mind", 0.40), ("damaged_in_transit", 0.25), ("not_as_described", 0.20),
            ("wrong_item", 0.15))

CATEGORIES: tuple[Category, ...] = (
    Category("Clothing", 0.20, (12, 120), 0.22, _FIT,
             ("T-Shirt", "Jeans", "Hoodie", "Jacket", "Dress", "Sweater"),
             ("Northline", "Urbano", "Kestrel", "Mavi Lane", "Fjord & Co", "Basicwear")),
    Category("Shoes", 0.08, (35, 180), 0.19, _FIT,
             ("Sneaker", "Boot", "Sandal", "Loafer", "Running Shoe"),
             ("Stride", "Kestrel", "Pavement", "Alpenstep", "Runa")),
    Category("Electronics", 0.12, (20, 900), 0.08, _TECH,
             ("Headphones", "Speaker", "Smartwatch", "Tablet", "Charger", "Keyboard"),
             ("Voltix", "Sonara", "Pixelon", "Nordtech", "Ampere")),
    Category("Home & Kitchen", 0.15, (8, 250), 0.06, _GENERAL,
             ("Pan", "Knife Set", "Lamp", "Blender", "Towel Set", "Storage Box"),
             ("Hausgut", "Casa Lima", "Kitchenly", "Lumo", "Oakfield")),
    Category("Beauty", 0.10, (4, 70), 0.04, _GENERAL,
             ("Shampoo", "Face Cream", "Perfume", "Lip Balm", "Serum"),
             ("Glowa", "Pura", "Velvet Leaf", "Aura", "Botanix")),
    Category("Sports", 0.09, (10, 300), 0.07, _GENERAL,
             ("Yoga Mat", "Dumbbell", "Bike Light", "Water Bottle", "Tennis Racket"),
             ("Peakform", "Sprinto", "Alpenstep", "Velocity", "Coreline")),
    Category("Toys", 0.08, (6, 120), 0.05, _GENERAL,
             ("Puzzle", "Building Set", "Plush Toy", "Board Game", "RC Car"),
             ("Tinkerly", "Blokko", "Funfair", "Little Owl", "Playmore")),
    Category("Grocery", 0.18, (1.5, 25), 0.01, _GENERAL,
             ("Coffee", "Olive Oil", "Pasta", "Tea", "Chocolate", "Granola"),
             ("Bauernhof", "Mediterra", "Morning Co", "Cacao Nord", "Feldkorn")),
)

COUNTRIES: tuple[tuple[str, float, tuple[str, ...]], ...] = (
    ("DE", 0.45, ("Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt", "Stuttgart")),
    ("FR", 0.20, ("Paris", "Lyon", "Marseille", "Toulouse")),
    ("NL", 0.15, ("Amsterdam", "Rotterdam", "Utrecht")),
    ("AT", 0.10, ("Vienna", "Graz", "Linz")),
    ("CH", 0.10, ("Zurich", "Geneva", "Basel")),
)

MEMBERSHIPS: tuple[tuple[str, float, float], ...] = (
    # name, share at signup, relative order frequency
    ("Basic", 0.72, 1.0),
    ("Plus", 0.23, 1.8),
    ("Premium", 0.05, 3.0),
)

FIRST_NAMES = ("Anna", "Ben", "Clara", "David", "Elif", "Felix", "Greta", "Hugo", "Ines", "Jonas",
               "Karla", "Luca", "Mia", "Noah", "Olga", "Paul", "Rosa", "Sami", "Tara", "Yusuf")
LAST_NAMES = ("Becker", "Dubois", "Fischer", "Garcia", "Huber", "Jansen", "Keller", "Laurent",
              "Meyer", "Novak", "Peeters", "Richter", "Schmidt", "Vogel", "Weber", "Zimmermann")

# Rule names must match the pipeline's expectation names (pipelines/utilities/rules.py).
# Each injected defect breaks exactly one rule, so counts per rule are exact.
ORDER_DEFECT_RULES = {
    "missing_order_id": "valid_order_id",
    "missing_customer_id": "valid_customer_id",
    "invalid_quantity": "valid_quantity",
    "invalid_unit_price": "valid_unit_price",
}
RETURN_DEFECT_RULES = {
    "missing_return_id": "valid_return_id",
    "invalid_return_quantity": "valid_return_quantity",
}


@dataclass(frozen=True)
class Config:
    seed: int = 42
    start_date: date = date(2025, 9, 1)
    backfill_days: int = 365          # batch_000 covers 2025-09-01 .. 2026-08-31
    increment_days: int = 14          # batch_001 .. batch_014 cover 2026-09-01 .. 2026-09-14

    initial_customers: int = 12_000   # signed up before start_date
    new_customers: int = 8_000        # sign up during the data window
    products_per_category: int = 180
    base_orders_per_day: int = 380

    upgrade_rate: float = 0.18        # share of customers who move up one membership tier
    move_rate: float = 0.05           # share of customers who change city
    price_change_rate: float = 0.15   # share of products with a price change
    discount_rate: float = 0.18       # share of lines with a discount

    # Defects in the order feed (per order, then per line).
    missing_order_id_rate: float = 0.002
    missing_customer_id_rate: float = 0.003
    invalid_quantity_rate: float = 0.003
    invalid_unit_price_rate: float = 0.001
    orphan_product_rate: float = 0.003
    duplicate_order_rate: float = 0.005   # fully clean orders written twice in the same file

    # Defects in the return feed.
    missing_return_id_rate: float = 0.002
    invalid_return_quantity_rate: float = 0.003
    orphan_return_rate: float = 0.003     # extra returns that point to an order that doesn't exist
    duplicate_return_rate: float = 0.005

    # The orders file of this increment batch is sent again with the next batch.
    resent_batch: int = 6

    @property
    def total_days(self) -> int:
        return self.backfill_days + self.increment_days
