import duckdb


def compute_unique_expedition_count(con: duckdb.DuckDBPyConnection) -> int:
    """
    Determine the distinct count of expedition IDs.
    """
    result = con.sql("SELECT COUNT(DISTINCT expedition_id) FROM expeditions").fetchone()
    return 0 if result is None else result[0]


def compute_unique_species_count_per_expedition(
    con: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    """
    Computes the unique count of species per expedition.
    """
    return con.sql("""
        SELECT
            expedition_id,
            COUNT(DISTINCT species_name) AS count_unique_species
        FROM (
            SELECT
                expedition_id,
                unnest(reserve.species).name AS species_name
            FROM expeditions
        )
        GROUP BY expedition_id
    """)


def determine_tracking_issues_by_species(
    con: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    """
    Determines the species for which tracking issues may exist/
    Uses the heuristic that the 'tagged' individuals in a sighting
    cannot be greater than the known population of that species.
    """
    return con.sql("""
        WITH species_data AS (
            SELECT
                unnest(reserve.species).name AS name,
                unnest(reserve.species).population AS population,
                unnest(reserve.species).tracking.tagged AS tagged
            FROM expeditions
        )
        SELECT
            name,
            population,
            tagged,
            ROUND(CAST(tagged AS FLOAT) / population, 2) AS ratio_tagged,
            tagged - population AS excess_count
        FROM species_data
        WHERE population < tagged
        ORDER BY ratio_tagged DESC
    """)


def count_activity_matches_per_expedition(
    con: duckdb.DuckDBPyConnection, target_activity: str, min_activity_count: int
) -> duckdb.DuckDBPyRelation:
    """
    Fetches the expedition ids in which the target activity was sighted
    for any tracked species at least 'min_activity_count' times.
    """
    return con.sql(
        """
        WITH species_sightings AS (
            SELECT unnest(reserve.species).tracking.sightings AS sightings,
                   expedition_id
            FROM expeditions
        ),
        activities AS (
            SELECT unnest(sightings).activity AS activity,
                   expedition_id
            FROM species_sightings
        ),
        activity_counts AS (
            SELECT expedition_id,
                   COUNT(*) AS target_activity_count
            FROM activities
            WHERE activity = $target_activity
            GROUP BY expedition_id
        )
        SELECT expedition_id,
               target_activity_count
        FROM activity_counts
        WHERE target_activity_count > $min_activity_count
        ORDER BY target_activity_count DESC
        """,
        params={
            "target_activity": target_activity,
            "min_activity_count": min_activity_count,
        },
    )


def compute_species_population(
    con: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    """
    Computes the known population of all species across all expeditions.
    """
    return con.sql("""
        SELECT
            name,
            SUM(population) AS population
        FROM (
            SELECT
                unnest(reserve.species).name as name,
                unnest(reserve.species).population as population
            FROM expeditions
        )
        GROUP BY name
        ORDER BY population DESC
    """)


def main():
    """
    Run main script.
    """
    print("Hello from surviving-json-jungle!")
    con = duckdb.connect("jungle.db")

    con.sql("""
        CREATE OR REPLACE TABLE expeditions AS
        SELECT *
        FROM read_ndjson(
            's3://sumeo-jungle-data-lake/jungle/*.jsonl',
            columns = {
                expedition_id: 'VARCHAR',
                start_date: 'VARCHAR',
                end_date: 'VARCHAR',
                expedition_location: 'STRUCT(latitude DOUBLE,longitude DOUBLE)',
                reserve: 'STRUCT(
                    "name" VARCHAR,
                    "location" STRUCT(
                        latitude DOUBLE,
                        longitude DOUBLE
                    ),
                    species STRUCT(
                        "name" VARCHAR,
                        population UBIGINT,
                        tracking STRUCT(
                            tagged UBIGINT,
                            sightings STRUCT(
                                date VARCHAR,
                                "location" STRUCT(
                                    latitude DOUBLE,
                                    longitude DOUBLE
                                ),
                                activity VARCHAR
                            )[]
                        )
                    )[],
                    environmental_conditions STRUCT(
                        rainfall_mm STRUCT(
                            high UBIGINT,
                            low UBIGINT
                        ),
                        temperature_c STRUCT(
                            high UBIGINT,
                            low UBIGINT
                        )
                    )
                )'
            }
        )
    """)
    print(con.sql("SELECT * FROM expeditions LIMIT 10"))

    # Print analysis results
    print(f"Unique expedition count: {compute_unique_expedition_count(con)}")

    unique_species_count_per_expedition = compute_unique_species_count_per_expedition(
        con
    )
    print("Species count per expedition:")
    print(unique_species_count_per_expedition)

    species_population = compute_species_population(con)
    print("Species population:")
    print(species_population)

    tracking_issues = determine_tracking_issues_by_species(con)
    print("Tracking issues by species:")
    print(tracking_issues)

    activity_matches = count_activity_matches_per_expedition(con, "hunting", 2)
    print("Activity matches per expedition:")
    print(activity_matches)


if __name__ == "__main__":
    main()
