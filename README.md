# WS Analytics — dbt Project

## Project Overview (Shared)
- Business objective: deliver actionable soccer performance insights from historical and near‑real‑time data, inspired by Soccermatics methods (https://soccermatics.readthedocs.io/en/latest/index.html).
- Pipeline (end‑to‑end):
  - ws_scrapping: acquire raw match/event data from web sources/APIs
  - ws_preprocessing: validate, normalize, and load to staging (S3→Parquet→Redshift when applicable)
  - ws_dbt: transform into bronze/silver/gold models for analytics (team/player/match)
  - ws_orchestrator: schedule and monitor flows (S3 state when applicable)
  - ws_streamlit: visualize KPIs and match insights
  - ws_infrastructure: IaC for compute, storage, security, CI/CD
- Data stores: AWS S3 and Redshift are primary; not all steps use both.
- Future (planned next year): add an xG/xT training job; extend the pipeline/dbt (Python models or external tasks) to load trained parameters, infer and persist xG/xT per event, and compute aggregates, using dbt tags to separate standard vs inference runs.

## Producer–Consumer Pattern
- Consumer of shared infrastructure produced by `ws_infrastructure`.
- Consumes from SSM via profiles/connection: warehouse host/credentials, analytics bucket name.
- Produces: transformed models in Redshift (bronze, silver, gold), documentation and tests results.

## Orchestration & Pipeline Context
- Cloud execution: this step is triggered by the orchestrator after preprocessing.
- Previous step: `ws_preprocessing` loads bronze tables.
- Next step: `ws_streamlit` consumes gold models for dashboards.

## Project Layout
- `models/bronze_layer`: staged raw tables, thin transformations
- `models/silver_layer`: cleaned, conformed tables and intermediate facts
- `models/gold_layer`: aggregated marts for team/player/match insights
- `src/profiles.yml`: dbt profile (see Configuration)

## Configuration

### Environment & Profiles
- Configure `profiles.yml` (example in `src/profiles.yml`) to point to Redshift.
- Typical environment variables (or SSM-sourced values injected by orchestrator/Batch):
  - `REDSHIFT_HOST`, `REDSHIFT_USER`, `REDSHIFT_PASSWORD`, `REDSHIFT_DATABASE`, `REDSHIFT_PORT`
  - Optional IAM auth where supported by the dbt adapter

### Model Materializations
- Bronze: views or incremental tables (as appropriate)
- Silver: tables/incremental for performance
- Gold: tables for fast consumption by Streamlit

## Usage

### Local Development
1. Install dependencies (dbt adapter for Redshift):
```
pip install -r requirements.txt
```
2. Set environment or edit `profiles.yml`.
3. Run dbt:
```
dbt deps
dbt seed --select <seeds>   # if using seeds
dbt run --select tag:bronze
dbt run --select tag:silver
dbt run --select tag:gold
dbt test
```

### AWS Batch Deployment
This project can run dbt inside an AWS Batch job definition.
1. Build image:
```
docker build -t ws_dbt:latest .
```
2. Push to ECR:
```
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com
docker tag ws_dbt:latest <account-id>.dkr.ecr.us-east-1.amazonaws.com/ws_dbt:latest
docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/ws_dbt:latest
```
3. Submit job (example):
```
aws batch submit-job \
  --job-name ws-dbt-$(date +%s) \
  --job-definition ws-analytics-dbt \
  --job-queue ws-analytics-job-queue
```

## Performance
- Use incremental models and clustering/keys where supported.
- Prefer pre-aggregations in gold to reduce query latency in Streamlit.

## Notes
- Tagging: reserve tags to separate standard runs vs (future) inference runs.
