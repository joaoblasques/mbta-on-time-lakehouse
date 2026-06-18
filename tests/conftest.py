"""Shared pytest fixtures.

A single local Spark session is reused across the suite (cheap setup, mirrors the
corpus best-practice for testable Spark pipelines). pyspark is imported lazily inside
the fixture so collection doesn't require it until a test actually needs Spark.
"""

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("mbta-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
