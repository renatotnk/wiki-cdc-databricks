from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import unicodedata


# =============================================================================
# Unicode Normalization UDF
# =============================================================================

@F.udf(returnType=StringType())
def normalize_nfc(text):
    """Apply Unicode NFC normalization to text."""
    if text is None:
        return None
    return unicodedata.normalize('NFC', text)


# =============================================================================
# Configuration
# =============================================================================

def _get_config(key: str, default: str) -> str:
    """Fetch pipeline configuration value with default fallback."""
    try:
        return spark.conf.get(key, default)
    except Exception:
        return default


# Configurable thresholds
ACCURACY_FUTURE_TOLERANCE_SECONDS = int(_get_config(
    "silver.accuracy.future_tolerance_seconds", "300"
))
TIMELINESS_LATE_THRESHOLD_SECONDS = int(_get_config(
    "silver.timeliness.late_threshold_seconds", "600"
))


# =============================================================================
# Staging Temporary View: Preprocessing and DQ Validation
# =============================================================================

@dp.temporary_view(
    comment="Staging layer: flattens bronze data, normalizes text, and evaluates data quality dimensions"
)
def _silver_recentchange_staging():
    """
    Reads bronze_recentchange via Change Data Feed, flattens nested structs,
    applies Unicode NFC normalization, and evaluates all data quality dimensions.
    
    All rows from bronze are preserved here with DQ failure tracking for routing
    to either the valid or rejected table.
    """
    # Read from bronze with Change Data Feed enabled (do NOT use skipChangeCommits)
    # This ensures updates/deletes in bronze propagate to silver
    bronze = spark.readStream.table("bronze_recentchange")
    
    # Flatten nested structs and apply Unicode NFC normalization BEFORE any dedup/filtering
    flattened = (
        bronze
        # Flatten meta struct
        .withColumn("meta_uri", F.col("meta.uri"))
        .withColumn("meta_request_id", F.col("meta.request_id"))
        .withColumn("meta_id", F.col("meta.id"))
        .withColumn("meta_dt", F.col("meta.dt").cast("timestamp"))  # Cast string to timestamp
        .withColumn("meta_domain", F.col("meta.domain"))
        .withColumn("meta_stream", F.col("meta.stream"))
        .withColumn("meta_topic", F.col("meta.topic"))
        .withColumn("meta_partition", F.col("meta.partition"))
        .withColumn("meta_offset", F.col("meta.offset"))
        # Flatten length struct
        .withColumn("length_old", F.col("length.old"))
        .withColumn("length_new", F.col("length.new"))
        # Unicode NFC normalization (BEFORE dedup to treat equivalent forms as identical)
        .withColumn("title", normalize_nfc(F.col("title")))
        .withColumn("user", normalize_nfc(F.col("user")))
        # Convert Unix timestamp to proper timestamp type (curated data for consumers)
        .withColumn("timestamp", F.from_unixtime(F.col("timestamp")).cast("timestamp"))
        # Convert _ingested_at from string to timestamp
        .withColumn("_ingested_at", F.col("_ingested_at").cast("timestamp"))
        # Enrichment: ingestion latency
        .withColumn(
            "_ingestion_latency_seconds",
            F.unix_timestamp(F.col("_ingested_at"))
            - F.unix_timestamp(F.col("meta_dt"))
        )
        # Enrichment: timeliness flag (INFORMATIONAL ONLY, not blocking)
        .withColumn(
            "_is_late_arrival",
            F.col("_ingestion_latency_seconds") > F.lit(TIMELINESS_LATE_THRESHOLD_SECONDS)
        )
        # Silver load timestamp
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .drop("meta", "length", "_rescued_data", "_bronze_loaded_at")
    )
    
    # =============================================================================
    # Data Quality Validation: Build _dq_failure_reasons array
    # =============================================================================
    # Each blocking dimension adds its name to the array if the check fails.
    # Timeliness is NOT included here (informational only, tracked via _is_late_arrival).
    
    dq_checked = (
        flattened
        # Start with empty array
        .withColumn("_dq_failure_reasons", F.array())
        
        # 1. COMPLETENESS: _event_id, title, wiki, meta_dt must not be null
        .withColumn(
            "_dq_failure_reasons",
            F.when(
                F.col("_event_id").isNull()
                | F.col("title").isNull()
                | F.col("wiki").isNull()
                | F.col("meta_dt").isNull(),
                F.array_union(F.col("_dq_failure_reasons"), F.array(F.lit("completeness")))
            ).otherwise(F.col("_dq_failure_reasons"))
        )
        
        # 2. VALIDITY:
        #    - type must be in {edit, new, log, categorize}
        #    - title must not contain C0 control characters (0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F)
        #      Explicitly exclude tab (0x09), newline (0x0A), carriage return (0x0D)
        .withColumn(
            "_validity_type_check",
            ~F.col("type").isin(["edit", "new", "log", "categorize"])
        )
        .withColumn(
            "_validity_title_check",
            # Regex matches C0 controls: [\x00-\x08\x0B\x0C\x0E-\x1F]
            F.col("title").rlike(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
        )
        .withColumn(
            "_dq_failure_reasons",
            F.when(
                F.col("_validity_type_check") | F.col("_validity_title_check"),
                F.array_union(F.col("_dq_failure_reasons"), F.array(F.lit("validity")))
            ).otherwise(F.col("_dq_failure_reasons"))
        )
        .drop("_validity_type_check", "_validity_title_check")
        
        # 3. ACCURACY:
        #    - If type == "new", length_old must be null or 0
        #    - timestamp must not be more than ACCURACY_FUTURE_TOLERANCE_SECONDS ahead of processing time
        .withColumn(
            "_accuracy_length_check",
            (F.col("type") == "new")
            & F.col("length_old").isNotNull()
            & (F.col("length_old") != 0)
        )
        .withColumn(
            "_accuracy_timestamp_check",
            # timestamp is already converted to timestamp type
            F.col("timestamp")
            > (F.current_timestamp() + F.expr(f"INTERVAL {ACCURACY_FUTURE_TOLERANCE_SECONDS} SECONDS"))
        )
        .withColumn(
            "_dq_failure_reasons",
            F.when(
                F.col("_accuracy_length_check") | F.col("_accuracy_timestamp_check"),
                F.array_union(F.col("_dq_failure_reasons"), F.array(F.lit("accuracy")))
            ).otherwise(F.col("_dq_failure_reasons"))
        )
        .drop("_accuracy_length_check", "_accuracy_timestamp_check")
        
        # 4. CONSISTENCY:
        #    - dt partition value must equal the UTC calendar date derived from timestamp
        #    Timezone is set to UTC in session config, so to_date() uses UTC
        .withColumn(
            "_consistency_dt_check",
            F.col("dt") != F.to_date(F.col("timestamp"))
        )
        .withColumn(
            "_dq_failure_reasons",
            F.when(
                F.col("_consistency_dt_check"),
                F.array_union(F.col("_dq_failure_reasons"), F.array(F.lit("consistency")))
            ).otherwise(F.col("_dq_failure_reasons"))
        )
        .drop("_consistency_dt_check")
    )
    
    return dq_checked
