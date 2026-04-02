# Django Property Data Pipeline

This project implements the assignment with separate Django apps for extraction and output writers:

- `data_extractor`: reads JSON files into PySpark DataFrames
- `data_process`: joins, transforms, validates, and builds the standardized property dataset
- `write_iceberg`: writes the final DataFrame into an Iceberg table
- `write_postgres`: writes the final DataFrame into PostgreSQL via Spark JDBC

The current repository uses local JSON files under `data/raw_data/`. These are mapped in [config.py](/home/w3e39/Documents/data_processing/data_pipeline/config.py).

The pipeline uses one Spark session and, on every run, writes the processed `final_df` to both Iceberg and PostgreSQL.

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Start PostgreSQL with Docker:

```bash
docker compose up -d
```

The default Docker database settings are:

- host: `localhost`
- port: `5432`
- database: `property_pipeline`
- user: `pipeline_user`
- password: `pipeline_pass`

You can override the Postgres target used by the pipeline with these environment variables before running it:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_TABLE`
- `POSTGRES_WRITE_MODE`

## Run

This project no longer defines a custom `process_data` management command, so `python manage.py` now behaves like a normal Django entry point and shows the standard command help when no subcommand is provided.

Run the extractor app against the products API with:

```bash
python3 manage.py extract_data
```

This command fetches product data from `https://dummyjson.com/products` and logs the extracted PySpark DataFrame details.

Run the full pipeline with:

```bash
python3 run_pipeline.py
```

This pipeline reads the local property and reviews JSON files in `data/raw_data/`, builds `final_df`, and writes it to all configured outputs:

- JSON file output
- Iceberg table output
- PostgreSQL table output
- validation report output

There is no runtime switch for choosing sinks. The pipeline always writes to:

- `data/warehouse/property_data.json`
- `iceberg_catalog.db.property_table`
- `public.property_table`

## Output

- Final JSON output: `data/warehouse/property_data.json`
- Iceberg table: `iceberg_catalog.db.property_table`
- PostgreSQL table: `public.property_table`
- Validation report: `validation_report.txt`
- Structured logs: `logs/<YYMMDD>/<script_name>_<YYMMDD>_<HHMMSS>.json`

## PostgreSQL Verification

After the pipeline runs, you can inspect the loaded table with:

```bash
docker exec -it property-pipeline-postgres psql -U pipeline_user -d property_pipeline -c "SELECT COUNT(*) FROM public.property_table;"
```

## Standardized Output Schema

The processor produces exactly these 10 columns:

- `id`
- `feed_provider_id`
- `property_name`
- `property_slug`
- `country_code`
- `currency`
- `usd_price`
- `star_rating`
- `review_score`
- `published`

## Dependencies

- Django
- PySpark
- Iceberg support via Spark catalog settings
- PostgreSQL JDBC driver via Spark package resolution
- `pyiceberg` for local Iceberg-related dependency coverage
- Docker for the local PostgreSQL service

## Assumptions

- `source_id` is derived from each source file's top-level `id` field.
- `property_name` is built from `name.en-us` and trader address city when available; if one part is missing, the available part is used.
- `country_code` is taken from `location.country` first, then trader address country.
- `review_score` is the average score from the nested `reviews` array.
- `usd_price` defaults to `0.0` because the provided property feed does not expose a direct USD price field.
- PostgreSQL writes use Spark JDBC with `overwrite` mode by default.
- The existing Django app names are preserved and separate writer apps are used for external sinks.
