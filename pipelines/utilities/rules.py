"""Data-quality rules shared by the pipeline expectations and the quarantine tables.

One dict per feed drives both paths, so the rows dropped by `expect_all_or_drop`
are exactly the rows written to quarantine. Every rule is written so it can only
evaluate to TRUE or FALSE, never NULL.

The generator's manifest uses the same rule names (generator/config.py).
"""

ORDER_LINE_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_order_ts": "order_ts IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "valid_quantity": "quantity IS NOT NULL AND quantity BETWEEN 1 AND 50",
    "valid_unit_price": "unit_price IS NOT NULL AND unit_price > 0",
}

RETURN_RULES = {
    "valid_return_id": "return_id IS NOT NULL",
    "valid_order_reference": "order_id IS NOT NULL AND line_no IS NOT NULL",
    "valid_return_ts": "return_ts IS NOT NULL",
    "valid_return_quantity": "quantity IS NOT NULL AND quantity > 0",
    "valid_refund_amount": "refund_amount IS NOT NULL AND refund_amount >= 0",
}

CUSTOMER_CDC_RULES = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_change_ts": "change_ts IS NOT NULL",
    "valid_operation": "operation IN ('INSERT', 'UPDATE')",
}

PRODUCT_CDC_RULES = {
    "valid_product_id": "product_id IS NOT NULL",
    "valid_change_ts": "change_ts IS NOT NULL",
    "valid_operation": "operation IN ('INSERT', 'UPDATE')",
    "valid_list_price": "list_price IS NOT NULL AND list_price > 0",
}
