# Wikipedia CDC Streaming Pipeline

A Databricks Spark Declarative Pipeline (SDP) for ingesting and processing Wikipedia recent changes data.

## Pipeline Architecture

### Bronze Layer
- **bronze_recentchange**: Raw Wikipedia recent changes data ingested from Unity Catalog Volume using Auto Loader
  - Source: `/Volumes/wiki-cdc-streaming/raw/wiki-cdc-streaming/raw/`
  - Format: Parquet files with Hive-style partitions (`dt=YYYY-MM-DD/hour=HH/`)
  - Target: `wiki-cdc-streaming.bronze.bronze_recentchange`
  - Features:
    - Incremental ingestion with Auto Loader (cloudFiles)
    - Schema enforcement with explicit StructType
    - Automatic partition column inference (dt, hour)
    - Rescued data column for schema evolution
    - Load timestamp tracking (_bronze_loaded_at)

### Silver Layer
- Coming soon: Cleaned and validated data

### Gold Layer
- Coming soon: Aggregated metrics and analytics-ready tables

## Project Structure

```
wiki-cdc-databricks/
├── transformations/
│   ├── bronze/
│   │   └── bronze_recentchange.py
│   ├── silver/
│   └── gold/
├── README.md
├── .gitignore
└── LICENSE
```

## Pipeline Configuration

- **Catalog**: `wiki-cdc-streaming`
- **Schema**: `bronze`
- **Compute**: Serverless
- **Runtime**: Current channel with Photon
- **Pipeline Type**: Continuous (triggered)

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
   - Schema: `bronze`
3. Run the pipeline

## Development Workflow

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes to transformation files
3. Test in development mode
4. Commit and push: `git commit -am "Your message" && git push`
5. Create a pull request for review

## Data Schema

The bronze_recentchange table contains the following key fields:
- `id`: Wikipedia event ID
- `type`: Change type (edit, new, categorize, etc.)
- `title`: Page title
- `user`: Username who made the change
- `bot`: Boolean flag for bot edits
- `wiki`: Wiki identifier (enwiki, commonswiki, etc.)
- `timestamp`: Unix timestamp of the change
- `server_url`: Wiki server URL
- `meta`: Metadata struct (request_id, domain, stream, etc.)
- `length`: Old and new page length
- `dt`: Partition date (DATE)
- `hour`: Partition hour (INT)
- `_bronze_loaded_at`: Ingestion timestamp

## Contributing

Contributions are welcome! Please follow the development workflow above.

## License

See LICENSE file for details.
