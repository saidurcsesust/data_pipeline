# Django Assignment: Standardizing Property Data

## Objective

Build a Django-based data processing system using separate apps for extraction, processing, and output writing.
## Description

You are given two JSON files containing accommodation data. Your task is to create a Django project that:

1. Uses the Extractor app to load and structure data from JSON files using PySpark DataFrames.
2. Uses the Processor app to join, transform, and clean the data.
3. Produces a final standardized dataset with exactly 10 columns.
4. Writes the final dataset using two writer apps: `write_iceberg` and `write_postgres`.

## Project Structure

```text
project_root/
├── data/
│   ├── raw_data/
|   |_  |__source_a.json
|   |   |__source_a.json
|   |__ warehouse/
├── logs/
├── extractor_app/
│   ├── models.py
│   ├── services/
│   │   ├── extractor_service.py
│   │   └── utils.py
│   └── management/
│       └── commands/
│           └── extract_data.py
├── processor_app/
│   ├── models.py
│   ├── services/
│   │   └── processor_service.py
│   ├── management/
│   └── validators.py
├── config.py
├── requirements.txt
├── README.md
├── validation_report.txt
└── manage.py
```

## Functional Requirements

### Input Files

- `data/source_a.json`
- `data/source_b.json`

### Processing Logic

#### 1. Extractor App

- Reads JSON files
- Converts raw data into PySpark DataFrames
- Logs extraction steps

#### 2. Processor App

- Joins datasets from the Extractor app using the common key from `config.py`
- Uses left join so that records in `source_a` appear even if missing in `source_b`
- Applies transformations and cleaning rules
- Optionally writes the final dataset into an Iceberg table
- Logs processing steps

#### 3. Transformations

Create a final dataset with these columns:

```text
{id, feed_provider_id, property_name, property_slug,
country_code, currency, usd_price, star_rating,
review_score, published}
```

#### 4. Transformation Rules

| Field | Rule |
| --- | --- |
| `id` | `"GEN-" + source_id` |
| `feed_provider_id` | source property id |
| `property_name` | `name + city` (handle missing values) |
| `property_slug` | lowercase + dash-separated |
| `country_code` | uppercase |
| `currency` | default `"USD"` if missing |
| `usd_price` | default `0.0` if missing |
| `star_rating` | default `0.0` if missing |
| `review_score` | default `0.0` if missing |
| `published` | always `true` |

#### 5. Data Cleaning Rules

- Remove rows with missing `source_id`

## Output

### 1. Final Dataset

- Save output as JSON in:

```text
data/warehouse/property_data.json
```


### 2. Output Writers

- `write_iceberg` writes the final dataset into an Iceberg table.
- `write_postgres` writes the final dataset into PostgreSQL via Docker.

### 3. Validation Report

Create `validation_report.txt` containing:

- Row count of `source_a`
- Row count of `source_b`
- Row count after join
- Final output row count
- Dropped rows (missing `source_id`)
- Final schema
- Confirmation: exactly 10 columns



