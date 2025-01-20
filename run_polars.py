from pathlib import Path
from typing import Collection

import polars as pl

# Define types.
# NOTE: used for convenience, as these structs are present at
# multiple levels in the input data schema. Not necessary per se,
# but I did not want to repeat myself when defining the schema.
StructLocation = pl.Struct({"latitude": pl.Float64, "longitude": pl.Float64})
StructHighLow = pl.Struct({"high": pl.Int32(), "low": pl.Int32()})


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
            "start_date": pl.Date(),
            "end_date": pl.Date(),
            "expedition_location": StructLocation,
            "reserve": pl.Struct(
                {
                    "name": pl.String(),
                    "location": StructLocation,
                    "species": pl.List(
                        pl.Struct(
                            {
                                "name": pl.String(),
                                "population": pl.Int32(),
                                "tracking": pl.Struct(
                                    {
                                        "tagged": pl.Int32(),
                                        "sightings": pl.List(
                                            pl.Struct(
                                                {
                                                    # NOTE: parsing nested dates does not work (i.e., returns 'null').
                                                    # Ideally, we'd want the 'date' field to be of type 'pl.Date'.
                                                    # However, this will result in all 'null' values, even though
                                                    # all dates in the sample input data are valid :(
                                                    # Therefore, we keep it as a string here and we can attempt to
                                                    # parse it as a date later. This seems rather tricky though...
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
                            "temperature_c": pl.List(StructHighLow),
                        }
                    ),
                }
            ),
        }
    )


def read_data(path: Path) -> pl.LazyFrame:
    """
    Helper to read in data using the known schema.
    """
    return pl.scan_ndjson(path, schema=build_expected_input_schema())


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


# def fix_nested_date_parsing(data: pl.LazyFrame):
#     # TODO MG: figure out why the nested date is not parsed...
#     print("Showcases how the inner, nested date fields are not parsed correctly.")
#     print("I am not sure why this is the case. Still investigating...")
#     print(
#         "The query is expected to return valid dates of sightings, which are all non-null in the sample dataset."
#     )
#     res = data.select(
#         pl.col("reserve")
#         .struct.field("species")
#         .list.first()
#         .struct.field("tracking")
#         .struct.field("sightings")
#         .first()
#         .flatten()
#         .struct.unnest()
#     )
#     print(res.collect())


def main():
    """
    Run main script.
    """
    print("Hello from surviving-json-jungle!")

    ldf = read_data(Path("data/sample.jsonl"))
    print(ldf.limit(10).collect())

    summarize(ldf)

    # fix_nested_date_parsing(ldf)


if __name__ == "__main__":
    main()
