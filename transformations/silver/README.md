# Silver Layer: Wikipedia Recent Changes CDC Pipeline

## Overview

This silver layer implements data quality validation and enrichment for Wikipedia recent changes data flowing from the bronze layer. It produces two streaming tables:

1. **silver_recentchange** - Valid rows passing all data quality checks
2. **silver_recentchange_rejected** - Invalid rows with detailed failure reasons

## File Organization

Following the "1 dataset per file" convention, the implementation is split into three files:

```
transformations/silver/
├── _silver_recentchange_staging.py      # Temporary view for preprocessing
├── silver_recentchange.py                # Valid rows table
└── silver_recentchange_rejected.py       # Rejected rows table
```

### File Descriptions

* **`_silver_recentchange_staging.py`** - Temporary view that:
  - Reads from bronze via Change Data Feed
  - Flattens nested `meta` and `length` structs
  - Applies Unicode NFC normalization (before dedup)
  - Computes enrichment columns
  - Evaluates all 6 data quality dimensions
  - Builds `_dq_failure_reasons` array for routing

* **`silver_recentchange.py`** - Streaming table that:
  - Reads from staging view
  - Filters to valid rows (empty `_dq_failure_reasons`)
  - Applies streaming-safe deduplication on `_event_id`
  - Outputs CDF-enabled table for gold layer

* **`silver_recentchange_rejected.py`** - Streaming table that:
  - Reads from staging view
  - Filters to invalid rows (non-empty `_dq_failure_reasons`)
  - Outputs CDF-enabled table for analysis

## Architecture

### Data Flow

```
bronze_recentchange (CDF enabled)
    ↓
_silver_recentchange_staging (temporary view)
    ├─→ silver_recentchange (valid rows)
    └─→ silver_recentchange_rejected (invalid rows)
```

### Key Design Decisions

#### 1. Temporary View for Staging
A temporary view (`_silver_recentchange_staging`) handles all preprocessing in a single pass, ensuring consistency and allowing both output tables to read from the same source without duplicating logic.

#### 2. Data Quality Dimensions

**Blocking (4):** Rows failing these are routed to the rejected table
- **completeness**: `_event_id`, `title`, `wiki`, `meta_dt` must not be null
- **validity**: 
  - `type` ∈ {edit, new, log, categorize}
  - `title` must not contain C0 control chars (0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F)
- **accuracy**: 
  - If `type == "new"`, then `length_old` must be null or 0
  - `timestamp` must not be > 300s ahead of processing time (configurable)
- **consistency**: `dt` partition must equal UTC date derived from `timestamp`

**Non-blocking (2):**
- **uniqueness**: Streaming-safe deduplication on `_event_id` within watermark window
- **timeliness**: Informational flag `_is_late_arrival` when latency > 600s (configurable)

#### 3. Unicode Normalization
Both `title` and `user` fields undergo NFC (Canonical Decomposition followed by Canonical Composition) normalization BEFORE any deduplication or filtering. This ensures that events differing only in Unicode representation (e.g., "café" vs "café") are treated as identical.

#### 4. Deduplication Strategy
Uses `dropDuplicatesWithinWatermark` on `_event_id` with a 2-hour watermark (configurable). This approach:
- Bounds state growth (streaming-safe)
- Prevents indefinite memory accumulation
- **Trade-off**: Duplicates arriving outside the watermark window will NOT be detected

**IMPORTANT**: Dropped duplicates are NOT traceable in `silver_recentchange_rejected`. They are silently removed by Spark's deduplication operation and do not appear in either output table. Only rows failing the 4 blocking DQ dimensions appear in the rejected table.

#### 5. Watermark Independence
The deduplication watermark (default: 2 hours) is independent of the timeliness threshold (default: 600s / 10 minutes). This prevents genuinely late rows from being silently dropped by state eviction before the timeliness check can flag them.

#### 6. Change Data Feed
Both output tables have CDF enabled (`delta.enableChangeDataFeed = true`) to support:
- Propagation of bronze corrections/updates/deletes to silver
- Gold layer consumption of incremental changes
- Audit trail and time-travel queries

#### 7. Timezone Handling
All timestamp comparisons assume UTC timezone. The consistency check compares `dt` partition against the UTC calendar date derived from the Unix timestamp. Spark's session timezone should be set to UTC in pipeline configuration for consistency.

## Configuration

Thresholds are configurable via pipeline configuration (Spark conf):

```yaml
configuration:
  silver.accuracy.future_tolerance_seconds: "300"        # Default: 5 minutes
  silver.timeliness.late_threshold_seconds: "600"        # Default: 10 minutes
  silver.dedup.watermark_duration: "2 hours"             # Default: 2 hours
```

Set these in the pipeline settings under the `configuration` field.

## Schema

### silver_recentchange (valid rows)

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| id | long | Yes | Bronze id |
| type | string | Yes | Event type: edit, new, log, categorize |
| title | string | Yes | NFC-normalized page title |
| user | string | Yes | NFC-normalized username |
| bot | boolean | Yes | Bot flag |
| wiki | string | Yes | Wiki identifier |
| timestamp | long | Yes | Event timestamp (Unix epoch) |
| server_url | string | Yes | Wiki server URL |
| _event_id | string | Yes | Deduplication key |
| _ingested_at | string | Yes | Producer processing time |
| _producer_instance | string | Yes | Producer instance ID |
| _extra_fields | string | Yes | Extra fields JSON |
| _schema_version | string | Yes | Schema version |
| meta_uri | string | Yes | Event URI |
| meta_request_id | string | Yes | Request ID |
| meta_id | string | Yes | Event ID from meta |
| **meta_dt** | **timestamp** | **No** | Event publication time (NOT NULL) |
| meta_domain | string | Yes | Event domain |
| meta_stream | string | Yes | Event stream |
| meta_topic | string | Yes | Kafka topic |
| meta_partition | long | Yes | Kafka partition |
| meta_offset | long | Yes | Kafka offset |
| **length_old** | **long** | **Yes** | Old content length |
| **length_new** | **long** | **Yes** | New content length |
| **_ingestion_latency_seconds** | **long** | **Yes** | unix_timestamp(_ingested_at) - unix_timestamp(meta_dt) |
| **_is_late_arrival** | **boolean** | **No** | True if latency > threshold (NOT NULL) |
| **_silver_loaded_at** | **timestamp** | **No** | Silver processing timestamp (NOT NULL) |
| dt | date | Yes | Partition: date |
| hour | integer | Yes | Partition: hour |

### silver_recentchange_rejected (invalid rows)

Same schema as `silver_recentchange` plus:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **_dq_failure_reasons** | **array<string>** | **No** | Never-empty array of failed dimensions |

Example `_dq_failure_reasons` values: `["completeness"]`, `["validity", "accuracy"]`

## Implementation Constraints & Assumptions

### (a) Spark/Databricks API Constraints

1. **Expectations Not Used for Routing**: While the task suggested using `@dp.expect_or_drop` where idiomatic, this pattern is NOT idiomatic for routing invalid rows to a separate table. Expectations only support:
   - Warn (log violations, include rows)
   - Drop (discard rows permanently)
   - Fail (stop pipeline)
   
   Since we need to preserve and route invalid rows, we use explicit filtering with `_dq_failure_reasons` array instead.

2. **Streaming Deduplication**: Spark Structured Streaming's `dropDuplicates()` maintains unbounded state (grows indefinitely), making it unsuitable for production streaming. We use `dropDuplicatesWithinWatermark()` to bound state, accepting the trade-off that duplicates outside the watermark won't be detected.

3. **Change Data Feed Reading**: Reading from bronze does NOT require `skipChangeCommits` option. We want to process CDC events (updates/deletes) from bronze, so we read the raw CDF stream. Downstream consumers of silver tables may need `skipChangeCommits` if they can't handle CDC commits.

4. **Unicode Normalization**: Spark SQL's `normalize(str, 'NFC')` function is used for Unicode normalization. This is available in Spark 3.0+.

5. **Timezone**: Spark's `to_date()` and date/timestamp conversions use the session timezone. We rely on the pipeline's Spark session being configured with UTC timezone. Explicitly setting `spark.sql.session.timeZone = UTC` in pipeline configuration is recommended.

### (b) Watermark Decoupling

The deduplication watermark (`silver.dedup.watermark_duration`) is intentionally decoupled from the timeliness threshold (`silver.timeliness.late_threshold_seconds`). 

**Rationale**: If the watermark equals the late-arrival threshold (e.g., both 10 minutes), a row arriving at minute 11 would:
1. Have `_is_late_arrival = true` (correct)
2. Be dropped by state eviction before deduplication even sees it (incorrect)

By setting a longer watermark (default: 2 hours), we ensure late rows survive state eviction long enough to be:
- Flagged as late arrivals
- Deduplicated correctly
- Routed to the appropriate output table

The watermark should be set based on business requirements for:
- Maximum expected out-of-order arrival time
- Available memory for state storage
- Duplicate detection window requirements

### (c) Key Assumptions

1. **Bronze Schema Stability**: The implementation assumes the bronze schema matches the introspected schema:
   - `meta` is a struct with `dt` (string), plus other fields
   - `length` is a struct with `old` and `new` (both long)
   - `_event_id` exists and is unique within the deduplication window
   - `_ingested_at` is a parseable timestamp string

2. **Type Field Values**: The validity check assumes `type` is restricted to exactly: {edit, new, log, categorize}. If Wikipedia introduces new event types, the validation will reject them until the code is updated.

3. **C0 Control Characters**: The validity check explicitly rejects characters 0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F while allowing tab (0x09), newline (0x0A), and carriage return (0x0D). This assumes these are legitimate in `title` fields while other C0 controls are not.

4. **Partition Consistency**: The consistency check assumes the `dt` and `hour` partition columns in bronze were correctly derived from `timestamp`. If bronze has partition inconsistencies, they will propagate to the rejected table but be flagged.

5. **Unix Epoch Timestamps**: The `timestamp` field is assumed to be Unix epoch seconds (not milliseconds or other units). The code uses `from_unixtime()` which expects seconds.

6. **Idempotency**: Since both silver tables are streaming tables reading from CDF-enabled bronze, reruns are incremental. Full refresh is only needed if:
   - Schema changes require table recreation
   - Bronze undergoes a full refresh
   - State is corrupted

## Testing Recommendations

1. **Dry Run**: Run `startPipelineDryRun` to validate SQL/Python syntax and dependencies
2. **Unit Tests**: Test each DQ dimension independently with crafted test data
3. **Duplicate Testing**: Verify deduplication behavior with events arriving inside/outside watermark
4. **Late Arrival Testing**: Send events with large `_ingestion_latency_seconds` to verify flag
5. **Unicode Testing**: Send events with different Unicode normalizations of the same string
6. **CDF Propagation**: Update/delete rows in bronze and verify propagation to silver
7. **Threshold Configuration**: Test with different configuration values to ensure overrides work

## Monitoring

Key metrics to track:
- `silver_recentchange` row count (valid events)
- `silver_recentchange_rejected` row count by `_dq_failure_reasons` (dimension failures)
- `_is_late_arrival` percentage (timeliness tracking)
- Watermark lag (state management health)
- Processing latency (end-to-end pipeline performance)

Query rejected table to identify data quality issues:
```sql
SELECT _dq_failure_reasons, COUNT(*) as failure_count
FROM silver_recentchange_rejected
WHERE dt >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY _dq_failure_reasons
ORDER BY failure_count DESC
```

## Downstream Consumption

Gold layer tables should:
- Read from `silver_recentchange` (NOT the rejected table)
- Use batch reads (`spark.read.table`) for aggregations (materialized views)
- Use streaming reads with `skipChangeCommits=true` if CDF is not needed
- Join with dimension tables as needed
- Apply business-logic transformations

Example:
```python
@dp.materialized_view()
def gold_page_edit_summary():
    return (
        spark.read.table("silver_recentchange")
        .filter("type = 'edit'")
        .groupBy("wiki", "dt")
        .agg(
            F.count("*").alias("total_edits"),
            F.countDistinct("user").alias("unique_editors"),
            F.sum("length_new - length_old").alias("net_content_change")
        )
    )
```
