"""Bronze: raw landing files, loaded incrementally with Auto Loader.

Nothing is filtered here. Each row keeps its source file and load time, so any
later problem can be traced back to the file it came from. Auto Loader tracks
which files it has already read, so a pipeline update only reads new files.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities import schemas

LANDING_ROOT = spark.conf.get("landing_root", "/Volumes/retail/raw/landing")


def auto_loader(entity: str, fmt: str, schema):
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("pathGlobFilter", f"*.{fmt}")
              .option("rescuedDataColumn", "_rescued_data")
              .schema(schema))
    if fmt == "csv":
        reader = reader.option("header", "true")
    return (reader.load(f"{LANDING_ROOT}/{entity}/")
                  .select("*",
                          F.col("_metadata.file_path").alias("_source_file"),
                          F.current_timestamp().alias("_ingested_at")))


@dp.table(name="bronze.orders_raw",
          comment="Raw orders as received: one row per order, order lines nested in `items`.")
def orders_raw():
    return auto_loader("orders", "json", schemas.ORDERS)


@dp.table(name="bronze.returns_raw", comment="Raw returns as received.")
def returns_raw():
    return auto_loader("returns", "json", schemas.RETURNS)


@dp.table(name="bronze.customers_cdc_raw",
          comment="Raw customer change feed (INSERT and UPDATE rows). Contains personal data.")
def customers_cdc_raw():
    return auto_loader("customers", "csv", schemas.CUSTOMERS_CDC)


@dp.table(name="bronze.products_cdc_raw", comment="Raw product change feed (INSERT and UPDATE rows).")
def products_cdc_raw():
    return auto_loader("products", "csv", schemas.PRODUCTS_CDC)
