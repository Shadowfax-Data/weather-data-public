#!/usr/bin/env python3
"""
CLI tool to compute the N closest weather stations to each ZIP code based on latitude and longitude.
Takes data from existing stations and zipcode tables in a DuckDB database and creates a new relationship table.
"""

import argparse
import math

import duckdb


def compute_closest_stations(
    db_path, stations_table, zipcodes_table, output_table, num_stations, batch_size=100
):
    """
    Compute the N closest weather stations to each ZIP code and store the results in a new table.
    Processes zipcodes in batches for better progress tracking and memory management.

    Args:
        db_path (str): Path to the DuckDB database
        stations_table (str): Name of the stations table
        zipcodes_table (str): Name of the ZIP codes table
        output_table (str): Name of the output table to create
        num_stations (int): Number of closest stations to find for each ZIP code
        batch_size (int): Number of ZIP codes to process in each batch
    """
    print(f"Connecting to database: {db_path}")
    con = duckdb.connect(db_path)

    # Set memory limit to 512MB
    print("Setting DuckDB memory limit to 512MB")
    con.execute("PRAGMA memory_limit='512MB'")

    try:
        # Check if tables exist
        stations_exist = (
            con.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{stations_table}'"
            ).fetchone()
            is not None
        )

        zipcodes_exist = (
            con.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{zipcodes_table}'"
            ).fetchone()
            is not None
        )

        if not stations_exist:
            raise ValueError(f"Table '{stations_table}' does not exist in the database")

        if not zipcodes_exist:
            raise ValueError(f"Table '{zipcodes_table}' does not exist in the database")

        # Check if output table exists and drop if it does
        output_exists = (
            con.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{output_table}'"
            ).fetchone()
            is not None
        )

        if output_exists:
            print(f"Table '{output_table}' exists, dropping it for a clean reload")
            con.execute(f"DROP TABLE {output_table}")

        # Create the output table
        print(f"Creating table '{output_table}'")
        con.execute(f"""
            CREATE TABLE {output_table} (
                zipcode VARCHAR,
                station_id VARCHAR,
                station_name VARCHAR,
                station_latitude DOUBLE,
                station_longitude DOUBLE,
                zipcode_latitude DOUBLE,
                zipcode_longitude DOUBLE,
                distance_miles DOUBLE,
                rank INT
            )
        """)

        # Get all ZIP codes
        print("Fetching all ZIP codes")
        zipcodes = con.execute(f"""
            SELECT GEOID as zipcode, INTPTLAT as latitude, INTPTLONG as longitude
            FROM {zipcodes_table}
        """).fetchall()
        total_zipcodes = len(zipcodes)
        print(f"Found {total_zipcodes} ZIP codes")

        # Get all stations
        print("Fetching all stations")
        stations = con.execute(f"""
            SELECT id, name, latitude, longitude
            FROM {stations_table}
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """).fetchall()
        total_stations = len(stations)
        print(f"Found {total_stations} stations with valid coordinates")

        # Process ZIP codes in batches
        print(
            f"Computing {num_stations} closest stations for each ZIP code in batches of {batch_size}..."
        )

        # Create a temporary table to store distances
        print("Creating temporary table for distances")
        con.execute("""
            CREATE TEMPORARY TABLE temp_distances (
                zipcode VARCHAR,
                station_id VARCHAR,
                station_name VARCHAR,
                station_latitude DOUBLE,
                station_longitude DOUBLE,
                zipcode_latitude DOUBLE,
                zipcode_longitude DOUBLE,
                distance_miles DOUBLE,
                rank INT
            )
        """)

        # Process in batches
        total_batches = math.ceil(total_zipcodes / batch_size)
        processed_zipcodes = 0

        for batch_num in range(total_batches):
            batch_start = batch_num * batch_size
            batch_end = min((batch_num + 1) * batch_size, total_zipcodes)
            batch_zipcodes = zipcodes[batch_start:batch_end]

            # Create a temporary table for this batch of zipcodes
            print(
                f"Processing batch {batch_num + 1}/{total_batches} (ZIP codes {batch_start + 1}-{batch_end} of {total_zipcodes})..."
            )

            # Create a temporary table for this batch of zipcodes
            zipcode_list = "','".join([z[0] for z in batch_zipcodes])

            # Process this batch
            con.execute(f"""
                INSERT INTO temp_distances
                WITH zipcode_data AS (
                    SELECT
                        GEOID as zipcode,
                        INTPTLAT as zipcode_latitude,
                        INTPTLONG as zipcode_longitude
                    FROM {zipcodes_table}
                    WHERE GEOID IN ('{zipcode_list}')
                ),
                station_data AS (
                    SELECT
                        id as station_id,
                        name as station_name,
                        latitude as station_latitude,
                        longitude as station_longitude
                    FROM {stations_table}
                    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                ),
                cross_join AS (
                    SELECT
                        z.zipcode,
                        s.station_id,
                        s.station_name,
                        s.station_latitude,
                        s.station_longitude,
                        z.zipcode_latitude,
                        z.zipcode_longitude,
                        -- Haversine formula in SQL
                        2 * 3956 * asin(
                            sqrt(
                                pow(sin(radians(s.station_latitude - z.zipcode_latitude) / 2), 2) +
                                cos(radians(z.zipcode_latitude)) * cos(radians(s.station_latitude)) *
                                pow(sin(radians(s.station_longitude - z.zipcode_longitude) / 2), 2)
                            )
                        ) as distance_miles
                    FROM zipcode_data z
                    CROSS JOIN station_data s
                    WHERE (
                        -- Filter out stations that are obviously too far based on latitude
                        abs(z.zipcode_latitude - s.station_latitude) <= 5 AND
                        -- Filter out stations that are obviously too far based on longitude
                        abs(z.zipcode_longitude - s.station_longitude) <= 5
                    )
                ),
                ranked AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY zipcode ORDER BY distance_miles) as rank
                    FROM cross_join
                    WHERE distance_miles <= 200
                )
                SELECT *
                FROM ranked
                WHERE rank <= {num_stations}
            """)

            processed_zipcodes += len(batch_zipcodes)
            current_count = con.execute(
                "SELECT COUNT(*) FROM temp_distances"
            ).fetchone()[0]
            print(
                f"Processed {processed_zipcodes}/{total_zipcodes} ZIP codes. Current temp table has {current_count} entries."
            )

        # Insert all results from temp table into the final table
        print("Transferring results from temporary table to final output table...")
        con.execute(f"""
            INSERT INTO {output_table}
            SELECT * FROM temp_distances
        """)

        # Drop the temporary table
        con.execute("DROP TABLE temp_distances")

        # Get row count of the output table
        row_count = con.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
        print(f"Successfully created {output_table} with {row_count} entries")

        # Show a sample of the results
        print("\nSample of the results:")
        sample = con.execute(f"""
            SELECT * FROM {output_table}
            ORDER BY zipcode, rank
            LIMIT 5
        """).fetchall()

        for row in sample:
            print(
                f"ZIP code: {row[0]}, Station: {row[1]} ({row[2]}), Distance: {row[7]:.2f} miles, Rank: {row[8]}"
            )

    except Exception as e:
        print(f"Error computing closest stations: {str(e)}")
    finally:
        # Close DuckDB connection
        con.close()
        print("DuckDB connection closed")


def main():
    """Parse command-line arguments and run the processing."""
    parser = argparse.ArgumentParser(
        description="Compute the N closest weather stations to each ZIP code."
    )
    parser.add_argument(
        "--db-path",
        default="weather.duckdb",
        help="Path to DuckDB database (default: weather.duckdb)",
    )
    parser.add_argument(
        "--stations-table",
        default="ghcnd_stations",
        help="Name of the table with station data (default: ghcnd_stations)",
    )
    parser.add_argument(
        "--zipcodes-table",
        default="zipcodes",
        help="Name of the table with ZIP code data (default: zipcodes)",
    )
    parser.add_argument(
        "--output-table",
        default="zipcode_stations",
        help="Name of the output table to create (default: zipcode_stations)",
    )
    parser.add_argument(
        "--num-stations",
        type=int,
        default=20,
        help="Number of closest stations to find for each ZIP code (default: 20)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of ZIP codes to process in each batch (default: 100)",
    )

    args = parser.parse_args()

    compute_closest_stations(
        args.db_path,
        args.stations_table,
        args.zipcodes_table,
        args.output_table,
        args.num_stations,
        args.batch_size,
    )


if __name__ == "__main__":
    main()
