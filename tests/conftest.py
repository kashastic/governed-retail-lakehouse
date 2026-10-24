import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "pipelines"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (SparkSession.builder.master("local[4]")
               .appName("governed-retail-lakehouse-tests")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")      # Databricks default
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.sql.warehouse.dir", str(warehouse))
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def world():
    from generator import build_world
    return build_world()


@pytest.fixture(scope="session")
def landing(world, tmp_path_factory):
    """All 15 batches written to a temporary landing folder. Returns (root, manifests)."""
    from generator import plan_batches, write_batch

    root = tmp_path_factory.mktemp("landing")
    manifests = [write_batch(world, b, root) for b in plan_batches(world.cfg)]
    return str(root), manifests


@pytest.fixture(scope="session")
def local_tables(spark, landing):
    """The whole pipeline run locally; tables are available as silver.x, gold.x, dq.x."""
    from tools import local_pipeline

    root, manifests = landing
    local_pipeline.run(spark, root)
    return manifests
