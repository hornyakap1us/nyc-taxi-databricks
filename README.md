# NYC Taxi Databricks Pipeline

End-to-end data engineering pipeline built on Databricks using Delta Live Tables,
medallion architecture, and CI/CD via GitHub Actions.

## Architecture

Raw CSV Data
↓
Bronze Layer (Delta Lake) — Raw ingestion, no transformations
↓
Silver Layer (Delta Lake) — Cleaned, validated, typed
↓
Gold Layer (Delta Lake)   — Business level monthly aggregations

## Tech Stack

- **Databricks** — Lakehouse platform and pipeline execution
- **Delta Live Tables** — Declarative pipeline framework with built-in data quality
- **Delta Lake** — ACID-compliant storage layer across all medallion tiers
- **PySpark** — Distributed data transformation engine
- **Databricks Asset Bundles** — Infrastructure as code for dev/prod deployment
- **GitHub Actions** — CI/CD automation for testing and deployment
- **Python / pytest** — Data quality test suite

## Project Structure

nyc-taxi-databricks/
├── .github/workflows/deploy.yml    # GitHub Actions CI/CD pipeline
├── databricks.yml                  # Asset Bundle config (dev/prod targets)
├── notebooks/
│   ├── 01_bronze_ingest.py         # Raw ingestion exploration notebook
│   ├── 02_silver_transform.py      # Transformation validation notebook
│   └── 03_gold_aggregate.py        # Aggregation exploration notebook
├── pipelines/
│   └── taxi_dlt_pipeline.py        # Delta Live Tables pipeline definition
├── resources/
│   └── taxi_workflow.yml           # Lakeflow Job orchestration definition
└── tests/
└── test_data_quality.py        # Pytest data quality test suite

## CI/CD Pipeline

Every push to this repository triggers the GitHub Actions workflow:

1. **Pull request** → runs data quality tests → deploys to **dev** environment
2. **Merge to main** → runs data quality tests → deploys to **prod** environment

Deployments are blocked automatically if any data quality test fails.

## Data Quality Tests

Twelve pytest tests validate each layer of the pipeline:

- **Bronze** — table exists, row count exceeds 1M, expected columns present
- **Silver** — no negative fares, no invalid passenger counts, no null timestamps
- **Gold** — row count valid, no null trip counts, no negative revenue, trip counts match silver

## How to Run Locally

### Prerequisites
- Databricks CLI installed
- Databricks workspace URL and token

### Deploy to dev
```bash
databricks bundle deploy --target dev
```

### Deploy to prod
```bash
databricks bundle deploy --target prod
```

### Run tests
```bash
python -m pytest tests/ -v
```

## Related Projects

- [NYC Taxi Batch Pipeline](https://github.com/hornyakap1us/nyc-taxi-pipeline) — BigQuery, dbt Core, Prefect
- [NYC Taxi Streaming Pipeline](https://github.com/hornyakap1us/nyc-taxi-streaming-pipeline) — Confluent Kafka, BigQuery, dbt Core, Airflow