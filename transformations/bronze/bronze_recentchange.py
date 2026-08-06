from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_META_SCHEMA = StructType(
    [
        StructField("uri", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("id", StringType(), True),
        StructField("dt", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("stream", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", LongType(), True),
        StructField("offset", LongType(), True),
    ]
)

_LENGTH_SCHEMA = StructType(
    [
        StructField("old", LongType(), True),
        StructField("new", LongType(), True),
    ]
)

BRONZE_RECENTCHANGE_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("user", StringType(), True),
        StructField("bot", BooleanType(), True),
        StructField("wiki", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("server_url", StringType(), True),
        StructField("meta", _META_SCHEMA, True),
        StructField("length", _LENGTH_SCHEMA, True),
        StructField("_event_id", StringType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_producer_instance", StringType(), True),
        StructField("_extra_fields", StringType(), True),
        StructField("_schema_version", StringType(), True),
        # Partition columns (inferred from Hive-style paths)
        StructField("dt", DateType(), True),
        StructField("hour", IntegerType(), True),
        # Auto Loader rescued data column
        StructField("_rescued_data", StringType(), True),
        # Added timestamp column
        StructField("_bronze_loaded_at", TimestampType(), False),
    ]
)

@dp.table(
    name="bronze_recentchange",
    comment="Bronze table for Wikipedia recent changes data",
    schema=BRONZE_RECENTCHANGE_SCHEMA,
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "parquet",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    cluster_by_auto=True,
)
def bronze_recentchange():
    """
    Ingests Wikipedia recent changes data from Volume using Auto Loader.
    Processes both existing files and new files as they arrive.
    """
    path = "/Volumes/wiki-cdc-streaming/raw/wiki-cdc-streaming/raw/"

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            #.option("cloudFiles.useNotifications", "true")
            .load(path)
            .withColumn("_bronze_loaded_at", current_timestamp())
    )