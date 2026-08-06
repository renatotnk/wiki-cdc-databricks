from pyspark import pipelines as dp
from pyspark.sql import functions as F
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


# =============================================================================
# Configuration
# =============================================================================

def _get_config(key: str, default: str) -> str:
    """Fetch pipeline configuration value with default fallback."""
    try:
        return spark.conf.get(key, default)
    except Exception:
        return default


# Configurable threshold for deduplication watermark
DEDUP_WATERMARK_DURATION = _get_config(
    "silver.dedup.watermark_duration", "2 hours"
)


# =============================================================================
# Schema Definition
# =============================================================================

_SILVER_BASE_SCHEMA = StructType(
    [
        # Core fields from bronze (carried through)
        StructField("id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),  # Will be NOT NULL after DQ filtering
        StructField("user", StringType(), True),
        StructField("bot", BooleanType(), True),
        StructField("wiki", StringType(), True),  # Will be NOT NULL after DQ filtering
        StructField("timestamp", TimestampType(), True),
        StructField("server_url", StringType(), True),
        StructField("_event_id", StringType(), True),  # Will be NOT NULL after DQ filtering
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


# =============================================================================
# Valid Rows Table
# =============================================================================

@dp.table(
    name="`wiki-cdc-streaming`.silver.silver_recentchange",
    comment="Validated Wikipedia recent changes with flattened schema and enrichments. Only rows passing all DQ checks.",
    schema=_SILVER_BASE_SCHEMA,
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    cluster_by_auto=True,
)
def silver_recentchange():
    """
    Streaming table containing only valid rows from bronze_recentchange.
    
    Applies:
    - 4 blocking DQ dimensions (completeness, validity, accuracy, consistency)
    - Streaming-safe deduplication on _event_id within watermark window
    - Change Data Feed enabled for downstream gold layer consumption
    
    NOTE: Dropped duplicates (from deduplication) are NOT traceable in
    silver_recentchange_rejected. They are silently dropped by the
    dropDuplicatesWithinWatermark operation. Only rows failing the 4 blocking
    DQ dimensions appear in the rejected table.
    """
    # Read from staging view
    staging = spark.readStream.table("_silver_recentchange_staging")
    
    # Filter to valid rows only (empty _dq_failure_reasons array)
    valid_rows = staging.filter(F.size(F.col("_dq_failure_reasons")) == 0)
    
    # Apply streaming-safe deduplication
    # Uses event timestamp (meta_dt) with watermark to bound state growth
    # Watermark duration is independent of timeliness threshold to avoid
    # late rows being dropped by state eviction before timeliness can flag them
    deduplicated = (
        valid_rows
        .withWatermark("meta_dt", DEDUP_WATERMARK_DURATION)
        .dropDuplicatesWithinWatermark(["_event_id"])
    )
    
    # Drop the DQ tracking column (not needed in final table)
    return deduplicated.drop("_dq_failure_reasons")
