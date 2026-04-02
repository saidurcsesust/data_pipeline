from pyspark.sql import DataFrame

import config


def write_postgres_table(final_df: DataFrame):
    (
        final_df.write.format("jdbc")
        .option("url", config.POSTGRES_JDBC_URL)
        .option("dbtable", config.POSTGRES_TABLE)
        .option("user", config.POSTGRES_USER)
        .option("password", config.POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode(config.POSTGRES_WRITE_MODE)
        .save()
    )
