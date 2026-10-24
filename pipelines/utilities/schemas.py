"""Explicit schemas for the landing files.

Bronze reads with a fixed schema rather than inference, so a malformed value is
captured in `_rescued_data` instead of silently changing a column's type.
"""
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, StringType, StructField,
                               StructType)

ORDER_ITEM = StructType([
    StructField("line_no", IntegerType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("discount_amount", DoubleType()),
])

ORDERS = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_ts", StringType()),
    StructField("channel", StringType()),
    StructField("currency", StringType()),
    StructField("items", ArrayType(ORDER_ITEM)),
])

RETURNS = StructType([
    StructField("return_id", StringType()),
    StructField("order_id", StringType()),
    StructField("line_no", IntegerType()),
    StructField("product_id", StringType()),
    StructField("return_ts", StringType()),
    StructField("quantity", IntegerType()),
    StructField("reason", StringType()),
    StructField("refund_amount", DoubleType()),
])

CUSTOMERS_CDC = StructType([
    StructField("customer_id", StringType()),
    StructField("email", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("membership", StringType()),
    StructField("signup_date", StringType()),
    StructField("operation", StringType()),
    StructField("change_ts", StringType()),
])

PRODUCTS_CDC = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("brand", StringType()),
    StructField("list_price", DoubleType()),
    StructField("operation", StringType()),
    StructField("change_ts", StringType()),
])
