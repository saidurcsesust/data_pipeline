from pathlib import Path

import shutil

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import config
from data_process.validators import build_validation_report


def normalized_name_column():
    return F.coalesce(
        F.col("name.`en-us`"),
        F.col("name.de"),
        F.col("name.fr"),
        F.col("name.es"),
    )


def normalized_city_column():
    return F.col("description.trader.address.city")


def clean_property_name():
    name_col = F.trim(normalized_name_column())
    city_col = F.trim(normalized_city_column())
    return (
        F.when(name_col.isNull() & city_col.isNull(), F.lit(None))
        .when(name_col.isNull(), city_col)
        .when(city_col.isNull(), name_col)
        .otherwise(F.concat_ws(" ", name_col, city_col))
    )


def aggregate_reviews(review_df: DataFrame) -> DataFrame:
    return (
        review_df.withColumn("review_item", F.explode_outer("reviews"))
        .groupBy(config.JOIN_KEY)
        .agg(F.avg(F.col("review_item.score")).alias("review_score"))
    )


def build_final_dataframe(property_df: DataFrame, review_df: DataFrame):
    source_a_rows = property_df.count()
    source_b_rows = review_df.count()

    cleaned_property_df = property_df.filter(F.col(config.JOIN_KEY).isNotNull())
    dropped_rows = source_a_rows - cleaned_property_df.count()
    review_explode_df = aggregate_reviews(review_df)

    joined_df = cleaned_property_df.join(review_explode_df, on=config.JOIN_KEY, how="left")
    post_join_rows = joined_df.count()

    final_df = (
        joined_df.withColumn(
            "id",
            F.concat(F.lit("GEN-"), F.col(config.JOIN_KEY).cast("string")),
        )
        .withColumn("feed_provider_id", F.col(config.JOIN_KEY).cast("string"))
        .withColumn("property_name", clean_property_name())
        .withColumn(
            "property_slug",
            F.when(
                F.col("property_name").isNull(),
                F.lit(None),
            ).otherwise(
                F.regexp_replace(
                    F.regexp_replace(F.lower(F.col("property_name")), r"[^a-z0-9]+", "-"),
                    r"(^-+|-+$)",
                    "",
                )
            ),
        )
        .withColumn(
            "country_code",
            F.upper(
                F.coalesce(
                    F.col("location.country"),
                    F.col("description.trader.address.country"),
                )
            ),
        )
        .withColumn("currency", F.coalesce(F.col("currency"), F.lit(config.DEFAULT_CURRENCY)))
        .withColumn("usd_price", F.lit(0.0))
        .withColumn("star_rating", F.coalesce(F.col("rating.stars"), F.lit(0.0)))
        .withColumn("review_score", F.col("review_score"))
        .withColumn("published", F.lit(True))
        .select(
            "id",
            "feed_provider_id",
            "property_name",
            "property_slug",
            "country_code",
            "currency",
            "usd_price",
            "star_rating",
            "review_score",
            "published",
        )
        .orderBy("id")
    )

    summary = {
        "source_a_rows": source_a_rows,
        "source_b_rows": source_b_rows,
        "post_join_rows": post_join_rows,
        "final_output_rows": final_df.count(),
        "dropped_rows_missing_source_id": dropped_rows,
        "final_schema": final_df.schema.simpleString(),
        "exactly_ten_columns": len(final_df.columns) == 10,
    }

    return joined_df, final_df, summary


def write_validation_report(summary):
    config.VALIDATION_REPORT_FILE.write_text(
        build_validation_report(summary),
        encoding="utf-8",
    )

def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path

def resolve_part_file(output_dir: Path) -> Path:
    for candidate in output_dir.iterdir():
        if candidate.name.startswith("part-") and candidate.suffix == ".json":
            return candidate
    raise FileNotFoundError(f"No Spark part file found in {output_dir}")



def write_dataframe(dataframe, output_file: Path):
    ensure_directory(output_file.parent)
    temp_dir = output_file.parent / f".{output_file.stem}_tmp"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if output_file.exists():
        output_file.unlink()

    dataframe.coalesce(1).write.mode("overwrite").json(str(temp_dir))
    part_file = resolve_part_file(temp_dir)
    shutil.move(str(part_file), str(output_file))
    shutil.rmtree(temp_dir)


def write_output(final_df: DataFrame):
    ensure_directory(config.OUTPUT_DIR)
    write_dataframe(final_df, Path(config.OUTPUT_FILE))
