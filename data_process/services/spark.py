import os

from pyspark.sql import SparkSession

import config


def create_spark_session() -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    builder = (
        SparkSession.builder.appName(config.SPARK_APP_NAME)
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.showConsoleProgress", "false")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    config.ICEBERG_SPARK_PACKAGE,
                    config.POSTGRES_SPARK_PACKAGE,
                ]
            ),
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
        .config(
            "spark.sql.catalog.iceberg_catalog.warehouse",
            str(config.ICEBERG_WAREHOUSE_DIR),
        )
    )

    return builder.getOrCreate()
