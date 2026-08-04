from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

WIKI_REFERENCE_SCHEMA = StructType(
    [
        StructField("wiki_code", StringType(), False),
        StructField("language_name", StringType(), True),
        StructField("project_type", StringType(), True),
        StructField("is_closed", BooleanType(), True),
        StructField("_snapshot_fetched_at", TimestampType(), True),
        StructField("_bronze_loaded_at", TimestampType(), False),
    ]
)

@dp.materialized_view(
    name="bronze_wiki_reference",
    comment="Bronze table for Wikipedia reference data",
    schema=WIKI_REFERENCE_SCHEMA,
    table_properties={
        "quality": "bronze",
        "inferred_schema": "false",
        "layer": "bronze",
        "source_format": "json",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    cluster_by_auto=True
)
def bronze_recentchange():
    """
    Ingests Wikipedia recent changes data from Volume using Auto Loader.
    Processes both existing files and new files as they arrive.
    """
    path = "/Volumes/wiki-cdc-streaming/raw/wiki-cdc-streaming/dim_wiki_reference/"

    return (
        spark.read
            .format("json")
            .load(path)
            .withColumn("_snapshot_fetched_at", to_timestamp(col("_snapshot_fetched_at")))
            .withColumn("_bronze_loaded_at", current_timestamp())
    )