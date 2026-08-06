# Wikipedia CDC Streaming Pipeline

A Databricks Spark Declarative Pipeline (SDP) for ingesting and processing Wikipedia recent changes data.

## Pipeline Architecture

### Bronze Layer

#### bronze_recentchange
Raw Wikipedia recent changes streaming data ingested from Unity Catalog Volume using Auto Loader.
- **Source**: `/Volumes/wiki-cdc-streaming/raw/wiki-cdc-streaming/raw/`
- **Format**: Parquet files with Hive-style partitions (`dt=YYYY-MM-DD/hour=HH/`)
- **Target**: `wiki-cdc-streaming.bronze.bronze_recentchange`
- **Type**: Streaming Table
- **Features**:
  - Incremental ingestion with Auto Loader (cloudFiles)
  - Schema enforcement with explicit StructType
  - Automatic partition column inference (dt, hour)
  - Rescued data column for schema evolution
  - Load timestamp tracking (_bronze_loaded_at)
  - Directory listing mode (file notification mode available but requires AWS permissions)

#### bronze_wiki_reference
Reference data for Wikipedia projects (languages, types, status).
- **Source**: `/Volumes/wiki-cdc-streaming/raw/wiki-cdc-streaming/dim_wiki_reference/`
- **Format**: JSON snapshot files
- **Target**: `wiki-cdc-streaming.bronze.bronze_wiki_reference`
- **Type**: Materialized View (batch refresh)
- **Schema**:
  - `wiki_code`: Wikipedia project code (e.g., "enwiki")
  - `language_name`: Human-readable language name
  - `project_type`: Type of wiki project
  - `is_closed`: Boolean flag for closed wikis
  - `_snapshot_fetched_at`: Timestamp when snapshot was captured
  - `_bronze_loaded_at`: Timestamp when loaded into bronze

### Silver Layer

The silver layer implements comprehensive data quality validation with a staging view feeding two streaming tables (valid and rejected records).

#### _silver_recentchange_staging (Temporary View)
Staging view with 4 blocking data quality dimensions:
- **Completeness**: Critical fields must not be null (id, type, timestamp, wiki, _ingested_at)
- **Validity**: Type values in allowed set, no control characters in text fields
- **Accuracy**: Business rule validation, timestamp sanity checks (not in future, not before 2001)
- **Consistency**: Partition alignment with event timestamp

**Enrichment**:
- Unicode NFC normalization for `title` and `user` fields
- `_ingestion_latency_seconds`: Time between event occurrence and bronze ingestion
- `_is_late_arrival`: Flag for events exceeding configurable latency threshold
- `_silver_loaded_at`: Silver layer processing timestamp

**Data Transformations**:
- `timestamp`: Converted from Unix epoch (LongType) to TimestampType
- `_ingested_at`: Converted from StringType to TimestampType

#### silver_recentchange
Valid records that passed all data quality checks.
- **Target**: `` `wiki-cdc-streaming`.silver.silver_recentchange ``
- **Type**: Streaming Table
- **Features**:
  - Streaming-safe deduplication on `_event_id`
  - Configurable watermark for state management
  - Auto liquid clustering for optimal file organization
  - Change Data Feed enabled for downstream consumption

#### silver_recentchange_rejected
Records that failed one or more data quality checks.
- **Target**: `` `wiki-cdc-streaming`.silver.silver_recentchange_rejected ``
- **Type**: Streaming Table
- **Features**:
  - `_dq_failure_reasons`: Array of specific DQ dimension failures
  - Enables root cause analysis for data quality issues
  - Same schema as valid records for easy comparison
  - Auto liquid clustering

### Gold Layer
- Coming soon: Aggregated metrics and analytics-ready tables

## Project Structure

```
wiki-cdc-databricks/
├── transformations/
│   ├── bronze/
│   │   ├── bronze_recentchange.py
│   │   └── bronze_wiki_reference.py
│   ├── silver/
│   │   ├── _silver_recentchange_staging.py
│   │   ├── silver_recentchange.py
│   │   ├── silver_recentchange_rejected.py
│   │   └── README.md
│   └── gold/
├── README.md
├── CHANGELOG.md
├── .gitignore
└── LICENSE
```

## Pipeline Configuration

- **Catalog**: `wiki-cdc-streaming`
- **Schemas**: `bronze`, `silver`
- **Compute**: Serverless
- **Runtime**: Current channel with Photon
- **Pipeline Type**: Continuous (triggered)
- **Features**: Liquid clustering, Change Data Feed, Auto Loader

## Getting Started

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Access to the `wiki-cdc-streaming` catalog
- Permissions to read from the source Volume

### Setup

1. Clone this repository into your Databricks workspace
2. Create or update the pipeline to point to this repo:
   - Libraries: `/Workspace/Repos/<your-email>/wiki-cdc-databricks/transformations/**`
   - Catalog: `wiki-cdc-streaming`
   - Schemas: `bronze`, `silver`
3. Run the pipeline

### Initial Data Load

⚠️ **Important**: After deploying schema changes, run a full refresh on silver tables:
- `silver_recentchange`
- `silver_recentchange_rejected`

This is required due to type conversions (Unix epoch → TimestampType, String → TimestampType).

## Development Workflow

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes to transformation files
3. Test with dry-run: Pipeline validates code without processing data
4. Run pipeline update to test with real data
5. Commit and push: `git commit -am "Your message" && git push`
6. Create a pull request for review

## Data Quality Configuration

Silver layer data quality checks use configurable thresholds via `spark.conf`:

```python
# Accuracy thresholds
spark.conf.set("dq.timestamp.min_year", "2001")  # Wikipedia launch year
spark.conf.set("dq.timestamp.future_tolerance_seconds", "300")  # 5 minutes

# Timeliness thresholds (informational only)
spark.conf.set("dq.latency.threshold_seconds", "3600")  # 1 hour
```

## Data Schema

### Bronze Layer

**bronze_recentchange**:
- `id`: Wikipedia event ID (BIGINT)
- `type`: Change type (edit, new, categorize, etc.) (STRING)
- `title`: Page title (STRING)
- `user`: Username who made the change (STRING)
- `bot`: Boolean flag for bot edits (BOOLEAN)
- `wiki`: Wiki identifier (enwiki, commonswiki, etc.) (STRING)
- `timestamp`: Event timestamp (BIGINT - Unix epoch)
- `server_url`: Wiki server URL (STRING)
- `meta`: Metadata struct (request_id, domain, stream, etc.) (STRUCT)
- `length`: Old and new page length (STRUCT)
- `dt`: Partition date (DATE)
- `hour`: Partition hour (INT)
- `_ingested_at`: Bronze ingestion timestamp (STRING)
- `_bronze_loaded_at`: Bronze load timestamp (TIMESTAMP)

**bronze_wiki_reference**:
- `wiki_code`: Wikipedia project code (STRING)
- `language_name`: Language name (STRING)
- `project_type`: Wiki project type (STRING)
- `is_closed`: Closed wiki flag (BOOLEAN)
- `_snapshot_fetched_at`: Snapshot timestamp (TIMESTAMP)
- `_bronze_loaded_at`: Bronze load timestamp (TIMESTAMP)

### Silver Layer

**silver_recentchange** and **silver_recentchange_rejected**:
- All fields from bronze_recentchange, plus:
- `timestamp`: Event timestamp (TIMESTAMP - converted from Unix epoch)
- `_ingested_at`: Bronze ingestion timestamp (TIMESTAMP - converted from STRING)
- `_event_id`: Unique event identifier for deduplication (STRING)
- `_ingestion_latency_seconds`: Latency between event and ingestion (DOUBLE)
- `_is_late_arrival`: Late arrival flag (BOOLEAN)
- `_silver_loaded_at`: Silver load timestamp (TIMESTAMP)
- `_dq_failure_reasons`: Array of DQ failure reasons (ARRAY<STRING>) - rejected table only

## Architecture Decisions

### Liquid Clustering
- Used instead of traditional partitioning for streaming tables
- Automatically optimizes file organization based on access patterns
- Reduces small file problems in streaming workloads

### Type Conversions in Silver Layer
- `timestamp`: Unix epoch → TimestampType for consumer-friendly queries
- `_ingested_at`: String → TimestampType for proper time-based operations

### Data Quality Strategy
- **Blocking dimensions**: Completeness, Validity, Accuracy, Consistency
- **Non-blocking dimension**: Timeliness (tracked but doesn't reject records)
- **Rejected records table**: Enables root cause analysis without data loss

## Contributing

Contributions are welcome! Please follow the development workflow above.

See [CHANGELOG.md](CHANGELOG.md) for detailed change history.

## License

See LICENSE file for details.
