from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import config


def write_iceberg_parquet_by_country(final_df: DataFrame):
    spark = final_df.sparkSession
    namespace = ".".join(config.ICEBERG_TABLE_PATH.split(".")[:-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    (
        final_df.writeTo(config.ICEBERG_TABLE_PATH)
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .partitionedBy(F.col("country_code"))
        .createOrReplace()
    )
