import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
OUTPUT_DIR = DATA_DIR / "warehouse"
ICEBERG_WAREHOUSE_DIR = OUTPUT_DIR
VALIDATION_REPORT_FILE = BASE_DIR / "validation_report.txt"

INPUT_SOURCE_A_FILE = DATA_DIR / "raw_data" / "property.json"
INPUT_SOURCE_B_FILE = DATA_DIR / "raw_data" / "reviews.json"
PRODUCTS_API_URL = "https://dummyjson.com/products"
EXTRACTED_PRODUCTS_FILE = DATA_DIR / "raw_data" / "products_api.json"
JOIN_KEY = "source_id"
DEFAULT_CURRENCY = "USD"
OUTPUT_FILE = OUTPUT_DIR / "property_data.json"
ICEBERG_TABLE_PATH = "iceberg_catalog.db.property_table"
ICEBERG_SPARK_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "property_pipeline")
POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline_pass")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "public.property_table")
POSTGRES_WRITE_MODE = os.getenv("POSTGRES_WRITE_MODE", "overwrite")
POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
POSTGRES_SPARK_PACKAGE = "org.postgresql:postgresql:42.7.3"
SPARK_APP_NAME = "django_property_data_pipeline"


LOG_DATE_FORMAT = "%y%m%d"
LOG_TIME_FORMAT = "%H%M%S"
