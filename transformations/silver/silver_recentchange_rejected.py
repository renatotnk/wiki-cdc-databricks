from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# =============================================================================
# Schema Definition
# =============================================================================

_SILVER_BASE_SCHEMA = StructType(
    [
        # Core fields from bronze (carried through)
        StructField("id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("user", StringType(), True),
        StructField("bot", BooleanType(), True),
        StructField("wiki", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("server_url", StringType(), True),
        StructField("_event_id", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_producer_instance", StringType(), True),
        StructField("_extra_fields", StringType(), True),
        StructField("_schema_version", StringType(), True),
        # Flattened from bronze.meta struct
        StructField("meta_uri", StringType(), True),
        StructField("meta_request_id", StringType(), True),
        StructField("meta_id", StringType(), True),
        StructField("meta_dt", TimestampType(), False),  # NOT NULL
        StructField("meta_domain", StringType(), True),
        StructField("meta_stream", StringType(), True),
        StructField("meta_topic", StringType(), True),
        StructField("meta_partition", LongType(), True),
        StructField("meta_offset", LongType(), True),
        # Flattened from bronze.length struct
        StructField("length_old", LongType(), True),
        StructField("length_new", LongType(), True),
        # Enrichment columns
        StructField("_ingestion_latency_seconds", LongType(), True),
        StructField("_is_late_arrival", BooleanType(), False),  # NOT NULL
        StructField("_silver_loaded_at", TimestampType(), False),  # NOT NULL
        # Partition columns (same as bronze)
        StructField("dt", DateType(), True),
        StructField("hour", IntegerType(), True),
    ]
)

_SILVER_REJECTED_SCHEMA = StructType(
    _SILVER_BASE_SCHEMA.fields + [
        StructField("_dq_failure_reasons", ArrayType(StringType()), False)  # NOT NULL, never empty
    ]
)


# =============================================================================
# Rejected Rows Table
# =============================================================================

@dp.table(
    name="`wiki-cdc-streaming`.silver.silver_recentchange_rejected",
    comment="Rejected Wikipedia recent changes that failed one or more DQ checks. Includes failure reasons for analysis.",
    schema=_SILVER_REJECTED_SCHEMA,
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "subtype": "rejected",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    cluster_by_auto=True,
)
def silver_recentchange_rejected():
    """
    Streaming table containing rows that failed any of the 4 blocking DQ dimensions.
    
    Includes _dq_failure_reasons array (never empty) listing which dimensions failed:
    - completeness: _event_id, title, wiki, or meta_dt is null
    - validity: type not in allowed set, or title contains C0 control characters
    - accuracy: new-type event with non-zero/non-null old length, or future timestamp
    - consistency: dt partition doesn't match timestamp's UTC date
    
    Does NOT include:
    - Duplicate events dropped by silver_recentchange's deduplication
      (those are silently removed and not traceable here)
    - Timeliness violations (tracked via _is_late_arrival flag, not blocking)
    """
    # Read from staging view
    staging = spark.readStream.table("_silver_recentchange_staging")
    
    # Filter to invalid rows only (non-empty _dq_failure_reasons array)
    invalid_rows = staging.filter(F.size(F.col("_dq_failure_reasons")) > 0)
    
    return invalid_rows
