from pyspark.sql import DataFrame

import config
from data_process.services.spark import create_spark_session
from data_process.services.utils import (
    build_final_dataframe,
    write_output,
    write_validation_report,
)
from write_iceberg.services.write import write_iceberg_parquet_by_country
from write_postgres.services.write import write_postgres_table


class ProcessorService:
    def __init__(self):
        self.spark = create_spark_session()

    def read_property_data(self) -> DataFrame:
        return (
            self.spark.read.option("multiline", "true")
            .json(str(config.INPUT_SOURCE_A_FILE))
            .withColumnRenamed("id", config.JOIN_KEY)
        )

    def read_review_data(self) -> DataFrame:
        return (
            self.spark.read.option("multiline", "true")
            .json(str(config.INPUT_SOURCE_B_FILE))
            .withColumnRenamed("id", config.JOIN_KEY)
        )

    def process(self):
        property_df = self.read_property_data()
        review_df = self.read_review_data()
        extraction_summary = {
            "source_a_path": str(config.INPUT_SOURCE_A_FILE),
            "source_b_path": str(config.INPUT_SOURCE_B_FILE),
            "source_a_rows": property_df.count(),
            "source_b_rows": review_df.count(),
            "source_a_columns": property_df.columns,
            "source_b_columns": review_df.columns,
        }
        joined_df, final_df, processing_summary = build_final_dataframe(property_df, review_df)

        write_output(final_df)
        write_validation_report(processing_summary)
        write_iceberg_parquet_by_country(final_df)
        write_postgres_table(final_df)

        result = {**extraction_summary, **processing_summary}
        return joined_df, final_df, result

    def stop(self):
        self.spark.stop()
