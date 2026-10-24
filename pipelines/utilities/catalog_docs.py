"""Column types and descriptions for the gold tables.

Each gold table is selected and cast from its spec, and the same spec becomes the
table's schema DDL with column comments. The DataFrame and the declared schema
therefore always match, and the comments land in Unity Catalog, where Catalog
Explorer, the dashboard and Genie can read them.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

Spec = list[tuple[str, str, str]]   # (column, type, comment)

TABLE_COMMENTS = {
    "dim_date": "Calendar dimension, one row per day.",
    "dim_customer": ("Customer dimension, SCD type 2: one row per version of a customer. "
                     "Join facts on customer_key to get the attributes valid at order time. "
                     "No direct identifiers (name, email): those stay in silver.customers."),
    "dim_product": ("Product dimension, current attributes (SCD type 1). Includes an 'Unknown product' "
                    "member (product_key = 'UNKNOWN') for order lines whose product is not in the catalogue."),
    "fact_order_line": ("Sales fact, one row per order line. Amounts in EUR. Returns are attributed to the "
                        "original order line, so recent order dates show fewer returns until their "
                        "30-day return window has passed."),
    "fact_return": ("Returns fact, one row per return, dated by when the return was received. "
                    "Returns that point to an unknown order line are kept and flagged (is_orphan_return)."),
}

DIM_DATE: Spec = [
    ("date_key", "INT", "Surrogate key, yyyymmdd."),
    ("date", "DATE", "Calendar date."),
    ("year", "INT", "Calendar year."),
    ("quarter", "INT", "Calendar quarter, 1-4."),
    ("month", "INT", "Calendar month, 1-12."),
    ("month_name", "STRING", "Month name, e.g. 'August'."),
    ("month_start", "DATE", "First day of the month."),
    ("week_start", "DATE", "Monday of the ISO week."),
    ("day_name", "STRING", "Weekday name, e.g. 'Monday'."),
    ("is_weekend", "BOOLEAN", "True on Saturday and Sunday."),
]

DIM_CUSTOMER: Spec = [
    ("customer_key", "BIGINT", "Surrogate key for one customer version. -1 is the unknown member."),
    ("customer_id", "STRING", "Business key of the customer, e.g. 'C000123'."),
    ("membership", "STRING", "Membership tier in this version: Basic, Plus or Premium."),
    ("city", "STRING", "City in this version."),
    ("country", "STRING", "ISO country code in this version: DE, FR, NL, AT or CH."),
    ("signup_date", "DATE", "Date the customer signed up."),
    ("valid_from", "TIMESTAMP", "Start of this version (inclusive)."),
    ("valid_to", "TIMESTAMP", "End of this version (exclusive). NULL for the current version."),
    ("is_current", "BOOLEAN", "True for the customer's current version."),
]

DIM_PRODUCT: Spec = [
    ("product_key", "STRING", "Key used by the facts. Equals product_id, or 'UNKNOWN'."),
    ("product_id", "STRING", "Business key of the product, e.g. 'P00042'."),
    ("product_name", "STRING", "Product name."),
    ("category", "STRING", "Product category, e.g. Clothing, Electronics, Grocery. 'Unknown' for the unknown member."),
    ("brand", "STRING", "Brand name."),
    ("current_list_price", "DECIMAL(10,2)", "Current list price in EUR. Order lines carry the price actually charged."),
]

FACT_ORDER_LINE: Spec = [
    ("order_line_id", "STRING", "Primary key: order_id and line_no, e.g. 'O0001234-2'."),
    ("order_id", "STRING", "Order the line belongs to. Count distinct order_id to count orders."),
    ("line_no", "INT", "Line number within the order."),
    ("order_ts", "TIMESTAMP", "When the order was placed."),
    ("order_date", "DATE", "Date the order was placed. Use this for sales and return-rate periods."),
    ("order_date_key", "INT", "Foreign key to dim_date.date_key."),
    ("customer_key", "BIGINT", "Foreign key to dim_customer: the customer version valid at order time."),
    ("customer_id", "STRING", "Business key of the customer."),
    ("product_key", "STRING", "Foreign key to dim_product. 'UNKNOWN' when the product is not in the catalogue."),
    ("source_product_id", "STRING", "Product id exactly as received in the order feed."),
    ("is_orphan_product", "BOOLEAN", "True when source_product_id is not in the product catalogue."),
    ("channel", "STRING", "Sales channel: web or app."),
    ("quantity", "INT", "Units sold on the line."),
    ("unit_price", "DECIMAL(10,2)", "Price per unit charged, before discount, in EUR."),
    ("gross_amount", "DECIMAL(12,2)", "quantity * unit_price, before discounts and returns."),
    ("discount_amount", "DECIMAL(12,2)", "Discount on the line in EUR."),
    ("net_amount", "DECIMAL(12,2)", "gross_amount - discount_amount: what the customer paid. Net sales."),
    ("returned_quantity", "INT", "Units of this line returned so far."),
    ("refund_amount", "DECIMAL(12,2)", "Amount refunded for this line so far, in EUR."),
    ("batch_id", "STRING", "Landing batch the line arrived in."),
]

FACT_RETURN: Spec = [
    ("return_id", "STRING", "Primary key of the return."),
    ("return_ts", "TIMESTAMP", "When the return was received."),
    ("return_date", "DATE", "Date the return was received. Use this for refunds in a period."),
    ("return_date_key", "INT", "Foreign key to dim_date.date_key."),
    ("order_line_id", "STRING", "Returned order line (order_id-line_no)."),
    ("order_id", "STRING", "Order of the returned line."),
    ("line_no", "INT", "Line number of the returned line."),
    ("order_date", "DATE", "Date the returned line was ordered. NULL for orphan returns."),
    ("product_key", "STRING", "Foreign key to dim_product. 'UNKNOWN' for orphan returns."),
    ("customer_key", "BIGINT", "Foreign key to dim_customer. -1 for orphan returns."),
    ("reason", "STRING", "Return reason code, e.g. size_or_fit, defective, changed_mind."),
    ("quantity", "INT", "Units returned."),
    ("refund_amount", "DECIMAL(12,2)", "Amount refunded in EUR."),
    ("days_to_return", "INT", "Days between order date and return date. NULL for orphan returns."),
    ("is_orphan_return", "BOOLEAN", "True when the return points to an order line that does not exist."),
    ("batch_id", "STRING", "Landing batch the return arrived in."),
]


def conform(df: DataFrame, spec: Spec) -> DataFrame:
    """Select and cast the spec's columns, in order."""
    return df.select(*[F.col(name).cast(dtype).alias(name) for name, dtype, _ in spec])


def schema_ddl(spec: Spec) -> str:
    def quote(text: str) -> str:
        return text.replace("\\", "\\\\").replace("'", "\\'")
    return ", ".join(f"{name} {dtype} COMMENT '{quote(comment)}'" for name, dtype, comment in spec)
