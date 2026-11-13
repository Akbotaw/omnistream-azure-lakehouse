"""
SparkSession factory that works seamlessly both locally (with Delta Lake & Kafka jars)
and inside an Azure Databricks cluster runtime
"""

import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "OmniStream-Airbnb-Lakehouse") -> SparkSession:
    """Return an active Databricks SparkSession or build a local one with Delta and Kafka. """
    # If running inside Azure Databricks Runtime, grab the pre-configured session
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return SparkSession.builder.appName(app_name).getOrCreate()
    
    # Local development / CI testing configuration
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
    )

    # Include Kafka Structured Streaming connector alongside Delta Lake jars
    extra_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"]
    return configure_spark_with_delta_pip(
        builder, extra_packages=extra_packages
    ).getOrCreate()