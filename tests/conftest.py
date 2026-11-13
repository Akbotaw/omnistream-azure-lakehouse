"""Shared pytest fixtures for local PySpark unit testing."""
from collections.abc import Generator
import pytest
from pyspark.sql import SparkSession
from src.utils.spark_session import get_spark_session

@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Provide a single shared local SparkSession across the test session and stop it on teardown."""
    spark_session = get_spark_session("OmniStream-Unit-Tests")
    yield spark_session
    spark_session.stop()