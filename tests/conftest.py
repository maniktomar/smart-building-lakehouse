"""Shared fixtures: one local Spark session with Delta, and a fresh lakehouse per test."""

import pytest

from sbl.config import load_config
from sbl.lakehouse import Lakehouse
from sbl.spark import local_spark


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    session = local_spark(str(tmp_path_factory.mktemp("warehouse")), app_name="sbl-tests")
    session.conf.set("spark.sql.shuffle.partitions", "1")
    yield session
    session.stop()


@pytest.fixture
def cfg(tmp_path):
    """The real repo config, pointed at temporary folders."""
    return load_config("local", storage_root=str(tmp_path / "lakehouse"),
                       landing_root=str(tmp_path / "landing"))


@pytest.fixture
def lh(spark, cfg):
    return Lakehouse(spark, cfg.env)
