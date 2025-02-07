from pathlib import Path
from typing import Collection

import polars as pl  # type: ignore
import s3fs  # type: ignore
from io import StringIO

# Define types.
# NOTE: used for convenience, as these structs are present at
# multiple levels in the input data schema. Not necessary per se,
# but I did not want to repeat myself when defining the schema.
StructLocation = pl.Struct({"latitude": pl.Float64, "longitude": pl.Float64})
StructHighLow = pl.Struct({"high": pl.Int64(), "low": pl.Int64()})


# Define utility functions.


def build_expected_input_schema() -> pl.Schema:
    """
    Returns the schema that the input animal data is expected to have.

    NOTE: added here since it's likely more robust to pre-set the schema
    rather than letting Polars infer it on read.

    NOTE: this also allows us to exclude fields from the input JSON if we
    do not need them. Simply skip fields that are not required from the
    schema definition, and Polars will not add them to the dataframe.
    """
    return pl.Schema(
        {
            "expedition_id": pl.String(),
            "start_date": pl.String(),
            "end_date": pl.String(),
            "expedition_location": StructLocation,
            "reserve": pl.Struct(
                {
                    "name": pl.String(),
                    "location": StructLocation,
                    "species": pl.List(
                        pl.Struct(
                            {
                                "name": pl.String(),
                                "population": pl.UInt64(),
                                "tracking": pl.Struct(
                                    {
                                        "tagged": pl.UInt64(),
                                        "sightings": pl.List(
                                            pl.Struct(
                                                {
                                                    "date": pl.String(),
                                                    "location": StructLocation,
                                                    "activity": pl.String(),
                                                }
                                            )
                                        ),
                                    }
                                ),
                            }
                        )
                    ),
                    "environmental_conditions": pl.Struct(
                        {
                            "rainfall_mm": StructHighLow,
                            "temperature_c": StructHighLow,
                        }
                    ),
                }
            ),
        }
    )


def read_local_data(path: Path) -> pl.LazyFrame:
    """
    Helper to read in local data using the known schema.
    """

    return pl.scan_ndjson(
        path,
        schema=build_expected_input_schema(),
    )


def read_cloud_data(bucket: str) -> pl.LazyFrame:
    """
    Helper function to read data from a public S3 bucket with
    JSONL files using the known schema.

    NOTE: ideally, we would not need to use s3fs directly,
    but rather let Polars figure it out using 'scan_ndjson'.
    However, this does not work in the case of our example
    bucket, even though it's public. I've tried to add multiple
    different 'storage_options' configs, yet generic cloud errors
    were being thrown.

    For what it's worth, you can try running the below code if you want.
    Maybe you won't get the same errors that I am encountering :)

    >>> s3_path = "s3://sumeo-jungle-data-lake/jungle/*.jsonl"
    >>> return pl.scan_ndjson(s3_path, schema=build_expected_input_schema())
    """

    print("Reading cloud data...")

    # We set 'anon' to true since we don't need auth to read from a public bucket.
    s3 = s3fs.S3FileSystem(anon=True)
    contents = (s3.read_text(file) for file in s3.ls(bucket) if file.endswith(".jsonl"))
    docs = (doc for content in contents for doc in content.splitlines())

    return pl.concat(
        # Scanning does not add much benefit here, since we already read
        # all the files in memory when downloading them using 's3fs'.
        # It's only done because it should work without the prior download,
        # and becuase a pl.LazyFrame is returned, which is expected by the
        # subsequent functions in the script. Granted, we could also just
        # call 'read_ndjson().lazy()' to get a pl.LazyFrame back...
        pl.scan_ndjson(
            StringIO(doc),
            schema=build_expected_input_schema(),
        )
        for doc in docs
    )


# Define analysis functions.
# NOTE: collection of functions showcasing various analytical
# operations that can be applied on the example dataset.


def compute_unique_expedition_count(data: pl.LazyFrame) -> int:
    """
    Determine the distinct count of expedition IDs.

    NOTE: start off simple, with no nested struct querying.
    """
    return data.select(pl.col("expedition_id").n_unique()).collect().item()


def compute_unique_species_count_per_expedition(data: pl.LazyFrame) -> pl.LazyFrame:
    """
    Computes the unique count of species per expedition.

    NOTE: requires fetching specific fields from nested structs.
    """
    return data.select(
        pl.col("expedition_id"),
        pl.col("reserve")
        .struct.field("species")
        .list.eval(pl.element().struct.field("name"))
        .list.n_unique()
        .alias("count_unique_species"),
    )


def compute_species_population(data: pl.LazyFrame) -> pl.LazyFrame:
    """
    Computes the known population of all species across all expeditions.

    NOTE: uses 'explode' to unnest a struct field and then groups by
    some of its keys.
    """
    return (
        data.select(
            pl.col("reserve")
            .struct.field("species")
            .list.explode()
            .struct.field("name", "population")
        )
        .group_by("name")
        .agg(pl.sum("population"))
        .sort("population", descending=True)
    )


def determine_tracking_issues_by_species(data: pl.LazyFrame) -> pl.LazyFrame:
    """
    Determines the species for which tracking issues may exist/
    Uses the heuristic that the 'tagged' individuals in a sighting
    cannot be greater than the known population of that species.

    NOTE: combines filtering and unnesting.
    """
    return (
        # Get the species info in an unnsted view.
        data.select(
            pl.col("reserve").struct.field("species").list.explode().struct.unnest()
        )
        # Get the tagged field.
        .with_columns(
            pl.col("tracking").struct.field("tagged").alias("tagged"),
        )
        # Filter for records that have tracking > population
        .filter(pl.col("population") < pl.col("tagged"))
        # Compute the ration of tagged animals of the known population.
        .with_columns(
            ratio_tagged=(pl.col("tagged") / pl.col("population")).round(2),
            excess_count=pl.col("tagged") - pl.col("population"),
        )
        # Perform final pre-display stuff.
        .sort("ratio_tagged", descending=True)
        .select("name", "population", "tagged", "ratio_tagged", "excess_count")
    )


def flatten(data: pl.LazyFrame) -> pl.LazyFrame:
    """
    Returns a fully unnester dataframe.

    NOTE: need to finish this implementation if we want to demo how
    to get to a totally unnested struct. This will not be too useful
    since the cardinality will go up dramatically, given how nested
    the types are.
    """

    return data.select(
        "expedition_id",
        "start_date",
        "end_date",
        pl.col("expedition_location")
        .struct.rename_fields(["expedition_latitude", "expedition_longitude"])
        .struct.unnest(),
        pl.col("reserve").struct.unnest(),
        # TODO MG: continue unnesting of remaining fields.
    )


def count_activity_matches_per_expedition(
    data: pl.LazyFrame, *, target_activity: str, min_activity_count: int
) -> pl.LazyFrame:
    """
    Fetches the expediton ids in which the target activity was sighted
    for any tracked species at least 'min_activity_count' times.

    NOTE: uses list flattening and counting operations on list types.
    """
    return (
        data.select(
            "expedition_id",
            sighted_activities=pl.col("reserve")
            .struct.field("species")
            .list.eval(
                pl.element()
                .struct.field("tracking")
                .struct.field("sightings")
                .list.eval(pl.element().struct.field("activity"))
                .flatten()
            ),
        )
        .with_columns(
            target_activity_counts=pl.col("sighted_activities").list.count_matches(
                target_activity
            ),
        )
        .filter(pl.col("target_activity_counts") > pl.lit(min_activity_count))
    )


def filter_for_species_by_name(
    data: pl.LazyFrame, target_species: Collection[str]
) -> pl.LazyFrame:
    """
    Filters for all species info given their name across all expeditions.

    NOTE: showcases filtering by element values in a list of structs.

    FIXME: this returns duplicates, likely due to the randomness of input data.
    See below example when running on S3 dataset.
    We might need to fix this somehow...
    ┌─────────────┬────────────┬────────┬─────────────────────────────────┐
    │ name        ┆ population ┆ tagged ┆ sightings                       │
    │ ---         ┆ ---        ┆ ---    ┆ ---                             │
    │ str         ┆ i64        ┆ i64    ┆ list[struct[3]]                 │
    ╞═════════════╪════════════╪════════╪═════════════════════════════════╡
    │ polyphemus  ┆ 54         ┆ 22     ┆ [{"2025-01-21",{-22.328316,25.…│
    │ dromedarius ┆ 131        ┆ 44     ┆ [{"2025-01-21",{-22.33408,25.8…│
    │ dromedarius ┆ 168        ┆ 20     ┆ [{"2025-01-21",{-22.280339,25.…│
    └─────────────┴────────────┴────────┴─────────────────────────────────┘
    """

    return (
        data.select(
            species=pl.col("reserve")
            .struct.field("species")
            .list.eval(
                pl.element().filter(
                    pl.element()
                    .struct.field("name")
                    .str.to_lowercase()
                    .is_in(target_species)
                )
            )
        )
        .filter(pl.col("species").list.len() > 0)
        .select(pl.col("species").flatten().struct.unnest())
        .with_columns(pl.col("tracking").struct.unnest())
        .select("name", "population", "tagged", "sightings")
    )


def compute_most_common_activity_per_species(data: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute the most common activity per species across all expeditions.
    This showcases complex querying patterns that can be used in Polars,
    such as list / struct unnesting or exploding, list mapping using
    list.eval(pl.element()...), filtering over partitions, similar to
    window functions, and value counts.
    """
    return (
        (
            data.select(
                "expedition_id",
                pl.col("end_date").alias("expedition_end_date"),
                pl.col("reserve").struct.field("species").alias("species"),
            )
            .explode("species")
            .unnest("species")
        )
        .select(
            "expedition_id",
            "expedition_end_date",
            pl.col("name").alias("species_name"),
            pl.col("tracking")
            .struct.field("sightings")
            .list.eval(pl.element().struct.field("activity"))
            .alias("activities"),
        )
        .sort("species_name", "expedition_end_date", "expedition_id")
        .group_by("species_name", maintain_order=True)
        .agg(
            pl.concat_list("activities")
            .flatten()
            .explode()
            .value_counts()
            .alias("activity_counts")
        )
        .explode("activity_counts")
        .unnest("activity_counts")
        .filter(pl.col("count") == pl.max("count").over("species_name"))
        .select("species_name", pl.col("activities").alias("activity"), "count")
    )


def summarize(data: pl.LazyFrame) -> None:
    """
    Print data summaries by applying multiple analytical functions.
    """
    with pl.Config() as cfg:
        cfg.set_tbl_rows(100)
        cfg.set_tbl_width_chars(1000)

        print(f"Unique expedition count: {compute_unique_expedition_count(data)}")

        print(
            f"Unique species count: {compute_unique_species_count_per_expedition(data).collect()}"
        )

        print(compute_species_population(data).collect())

        print(determine_tracking_issues_by_species(data).collect())

        print(
            count_activity_matches_per_expedition(
                data, target_activity="hunting", min_activity_count=2
            ).collect()
        )

        print(filter_for_species_by_name(data, ["polyphemus", "dromedarius"]).collect())

        print(
            "Most common activity per species:",
            compute_most_common_activity_per_species(data).collect(),
        )


def main():
    """
    Run main script.
    """
    print("Hello from surviving-json-jungle!")

    # ldf = read_local_data(
    #     Path("data/sample.jsonl")
    # )  # uncomment to run with local data.
    ldf = read_cloud_data("s3://sumeo-jungle-data-lake/jungle/")
    print(ldf.sort("expedition_id").limit(10).collect())

    summarize(ldf)


if __name__ == "__main__":
    main()
