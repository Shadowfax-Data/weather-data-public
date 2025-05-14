#!/usr/bin/env python3
"""
CLI tool to compute daily weather metrics for each zipcode.
Calculates PRCP, SNOW, SNWD, TMAX, TMIN metrics for all zipcodes using
weather station data within a specified distance range.
"""

import argparse
import datetime
import sys

import duckdb


def compute_daily_metrics(
    db_path, output_table, max_distance_miles, min_date, max_date
):
    """
    Compute daily weather metrics for each zipcode using data from nearby weather stations.

    Args:
        db_path (str): Path to the DuckDB database
        output_table (str): Name of the output table to create
        max_distance_miles (float): Maximum distance in miles for station data to be considered
        min_date (datetime.date): Minimum date to compute metrics for
        max_date (datetime.date): Maximum date to compute metrics for
    """
    print(f"Connecting to database: {db_path}")
    con = duckdb.connect(db_path)

    # Set memory limit
    print("Setting DuckDB memory limit to 512MB")
    con.execute("PRAGMA memory_limit='512MB'")

    try:
        # Check if required tables exist
        ghcn_exist = (
            con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='ghcn_daily_raw'"
            ).fetchone()
            is not None
        )

        stations_exist = (
            con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='zipcode_stations'"
            ).fetchone()
            is not None
        )

        if not ghcn_exist:
            raise ValueError("Table 'ghcn_daily_raw' does not exist in the database")

        if not stations_exist:
            raise ValueError("Table 'zipcode_stations' does not exist in the database")

        # Format dates for SQL queries
        min_date_str = min_date.strftime("%Y-%m-%d")
        max_date_str = max_date.strftime("%Y-%m-%d")

        # Check if output table exists and create if it doesn't
        output_exists = (
            con.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{output_table}'"
            ).fetchone()
            is not None
        )

        if output_exists:
            # Delete existing data for the specified date range instead of dropping the entire table
            print(
                f"Table '{output_table}' exists, deleting data in range {min_date_str} to {max_date_str}"
            )
            # First count rows that will be deleted
            count_result = con.execute(f"""
                SELECT COUNT(*) FROM {output_table}
                WHERE date BETWEEN '{min_date_str}' AND '{max_date_str}'
            """).fetchone()
            deleted_count = count_result[0] if count_result else 0

            # Then delete the rows
            con.execute(f"""
                DELETE FROM {output_table}
                WHERE date BETWEEN '{min_date_str}' AND '{max_date_str}'
            """)
            print(
                f"Deleted {deleted_count:,} rows from existing table for the specified date range"
            )
        else:
            # Create the output table with appropriate columns for each metric
            print(f"Creating new table '{output_table}'")
            con.execute(f"""
                CREATE TABLE {output_table} (
                zipcode VARCHAR,
                date DATE,
                year INTEGER,  -- Year extracted from date
                month INTEGER,  -- Month extracted from date
                day INTEGER,  -- Day extracted from date
                prcp DOUBLE,  -- Precipitation (tenths of mm)
                prcp_count INTEGER,
                prcp_stddev DOUBLE,
                snow DOUBLE,  -- Snowfall (mm)
                snow_count INTEGER,
                snow_stddev DOUBLE,
                snwd DOUBLE,  -- Snow depth (mm)
                snwd_count INTEGER,
                snwd_stddev DOUBLE,
                tmax DOUBLE,  -- Maximum temperature (tenths of degrees C)
                tmax_count INTEGER,
                tmax_stddev DOUBLE,
                tmin DOUBLE,  -- Minimum temperature (tenths of degrees C)
                tmin_count INTEGER,
                tmin_stddev DOUBLE
            )
        """)

        # Create a temporary view with filtered stations by distance
        print(f"Creating temporary view of stations within {max_distance_miles} miles")
        con.execute(f"""
            CREATE TEMPORARY VIEW filtered_stations AS
            SELECT *
            FROM zipcode_stations
            WHERE distance_miles <= {max_distance_miles}
        """)

        # Get distinct zipcodes
        print("Fetching distinct zipcodes from zipcode_stations table")
        zipcodes = con.execute("""
            SELECT DISTINCT zipcode
            FROM filtered_stations
            ORDER BY zipcode
        """).fetchall()
        total_zipcodes = len(zipcodes)
        print(
            f"Found {total_zipcodes} zipcodes with stations within {max_distance_miles} miles"
        )

        # Get date range to process
        print(f"Computing metrics for date range: {min_date_str} to {max_date_str}")
        date_range = con.execute(f"""
            SELECT DISTINCT date
            FROM ghcn_daily_raw
            WHERE date BETWEEN '{min_date_str}' AND '{max_date_str}'
            ORDER BY date
        """).fetchall()
        total_dates = len(date_range)
        print(f"Found {total_dates} distinct dates to process")

        # Process one day at a time
        processed_dates = 0

        for current_date_row in date_range:
            current_date = current_date_row[0]
            current_date_str = f"'{current_date}'"

            print(
                f"Processing date {processed_dates + 1}/{total_dates}: {current_date}"
            )

            # Process metrics for this single date
            query = f"""
                INSERT INTO {output_table}
                WITH date_values (date) AS (
                    VALUES ({current_date_str})
                ),
                date_range AS (
                    SELECT date FROM date_values
                ),
                zipcode_dates AS (
                    SELECT z.zipcode, d.date
                    FROM (SELECT DISTINCT zipcode FROM filtered_stations) z
                    CROSS JOIN date_range d
                ),
                station_data AS (
                    SELECT
                        s.zipcode,
                        g.date,
                        g.element,
                        g.data_value,
                        s.distance_miles
                    FROM ghcn_daily_raw g
                    JOIN filtered_stations s ON g.id = s.station_id
                    WHERE g.date = {current_date_str}
                    AND g.element IN ('PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN')
                    AND g.q_flag IS NULL  -- exclude quality-flagged data
                ),
                metrics_by_element AS (
                    SELECT
                        zipcode,
                        date,
                        element,
                        AVG(data_value) AS avg_value,
                        COUNT(*) AS data_count,
                        STDDEV(data_value) AS stddev_value
                    FROM station_data
                    GROUP BY zipcode, date, element
                ),
                pivoted AS (
                    SELECT
                        zipcode,
                        date,
                        MAX(CASE WHEN element = 'PRCP' THEN avg_value ELSE NULL END) AS prcp,
                        MAX(CASE WHEN element = 'PRCP' THEN data_count ELSE NULL END) AS prcp_count,
                        MAX(CASE WHEN element = 'PRCP' THEN stddev_value ELSE NULL END) AS prcp_stddev,
                        MAX(CASE WHEN element = 'SNOW' THEN avg_value ELSE NULL END) AS snow,
                        MAX(CASE WHEN element = 'SNOW' THEN data_count ELSE NULL END) AS snow_count,
                        MAX(CASE WHEN element = 'SNOW' THEN stddev_value ELSE NULL END) AS snow_stddev,
                        MAX(CASE WHEN element = 'SNWD' THEN avg_value ELSE NULL END) AS snwd,
                        MAX(CASE WHEN element = 'SNWD' THEN data_count ELSE NULL END) AS snwd_count,
                        MAX(CASE WHEN element = 'SNWD' THEN stddev_value ELSE NULL END) AS snwd_stddev,
                        MAX(CASE WHEN element = 'TMAX' THEN avg_value ELSE NULL END) AS tmax,
                        MAX(CASE WHEN element = 'TMAX' THEN data_count ELSE NULL END) AS tmax_count,
                        MAX(CASE WHEN element = 'TMAX' THEN stddev_value ELSE NULL END) AS tmax_stddev,
                        MAX(CASE WHEN element = 'TMIN' THEN avg_value ELSE NULL END) AS tmin,
                        MAX(CASE WHEN element = 'TMIN' THEN data_count ELSE NULL END) AS tmin_count,
                        MAX(CASE WHEN element = 'TMIN' THEN stddev_value ELSE NULL END) AS tmin_stddev
                    FROM metrics_by_element
                    GROUP BY zipcode, date
                )
                SELECT
                    zd.zipcode,
                    zd.date,
                    year(cast(zd.date as date)) AS year,
                    month(cast(zd.date as date)) AS month,
                    day(cast(zd.date as date)) AS day,
                    p.prcp,
                    p.prcp_count,
                    p.prcp_stddev,
                    p.snow,
                    p.snow_count,
                    p.snow_stddev,
                    p.snwd,
                    p.snwd_count,
                    p.snwd_stddev,
                    p.tmax,
                    p.tmax_count,
                    p.tmax_stddev,
                    p.tmin,
                    p.tmin_count,
                    p.tmin_stddev
                FROM zipcode_dates zd
                LEFT JOIN pivoted p ON zd.zipcode = p.zipcode AND zd.date = p.date
            """

            con.execute(query)
            processed_dates += 1

            print(
                f"Processed {processed_dates}/{total_dates} dates ({processed_dates / total_dates:.1%} complete)"
            )

        # Print summary statistics
        row_count = con.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
        print(
            f"Successfully computed metrics for {row_count:,} zipcode-date combinations"
        )

        # Print summary of coverage
        metrics = ["prcp", "snow", "snwd", "tmax", "tmin"]
        for metric in metrics:
            coverage = con.execute(f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN {metric} IS NOT NULL THEN 1 ELSE 0 END) as with_data,
                    SUM(CASE WHEN {metric} IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as percentage
                FROM {output_table}
            """).fetchone()
            print(
                f"{metric} coverage: {coverage[1]:,}/{coverage[0]:,} ({coverage[2]:.1f}%)"
            )

    except Exception as e:
        print(f"Error computing daily metrics: {str(e)}")
        raise
    finally:
        con.close()


def main():
    # Get current date for default max_date
    today = datetime.date.today()

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Compute daily weather metrics for each zipcode based on nearby stations"
    )
    parser.add_argument(
        "--db-path",
        default="weather.duckdb",
        help="Path to the DuckDB database (default: weather.duckdb)",
    )
    parser.add_argument(
        "--table-name",
        default="zipcode_daily_metrics",
        help="Name of the output table (default: zipcode_daily_metrics)",
    )
    parser.add_argument(
        "--max-distance-miles",
        type=float,
        default=20.0,
        help="Maximum distance in miles for station data to be considered (default: 20)",
    )
    parser.add_argument(
        "--min-date",
        default=None,
        help="Minimum date to compute metrics for (YYYY-MM-DD, default: auto-determined from existing data)",
    )
    parser.add_argument(
        "--max-date",
        default=today.strftime("%Y-%m-%d"),
        help=f"Maximum date to compute metrics for (YYYY-MM-DD, default: today ({today.strftime('%Y-%m-%d')}))",
    )

    args = parser.parse_args()

    # Set max_date from argument
    try:
        max_date = datetime.datetime.strptime(args.max_date, "%Y-%m-%d").date()
    except ValueError as e:
        print(f"Error parsing max date: {str(e)}")
        sys.exit(1)

    # Determine min_date based on existing data if not provided
    if args.min_date is None:
        print("No min-date specified, attempting to determine from existing data...")
        try:
            # Connect to database
            con = duckdb.connect(args.db_path)

            # Check if output table exists
            table_exists = (
                con.execute(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{args.table_name}'"
                ).fetchone()
                is not None
            )

            if table_exists:
                # Get the maximum date from the existing table
                max_existing_date_result = con.execute(f"""
                    SELECT MAX(date) FROM {args.table_name}
                """).fetchone()

                if max_existing_date_result and max_existing_date_result[0] is not None:
                    # Set min_date to the maximum date from the existing table
                    # No need to adjust by batch size since we're processing one day at a time
                    max_existing_date = max_existing_date_result[0]
                    min_date = max_existing_date
                    print(f"Found existing data with max date: {max_existing_date}")
                    print(
                        f"Setting min-date to {min_date} (starting from last processed date)"
                    )
                else:
                    # No data in table, use a default starting date
                    min_date = datetime.datetime.strptime(
                        "1900-01-01", "%Y-%m-%d"
                    ).date()
                    print(
                        f"Table {args.table_name} exists but contains no date data, using default min-date: {min_date}"
                    )
            else:
                # Table doesn't exist, use a default starting date
                min_date = datetime.datetime.strptime("2000-01-01", "%Y-%m-%d").date()
                print(
                    f"Table {args.table_name} does not exist, using default min-date: {min_date}"
                )

            con.close()
        except Exception as e:
            print(f"Error determining min-date from existing data: {str(e)}")
            min_date = datetime.datetime.strptime("2000-01-01", "%Y-%m-%d").date()
            print(f"Using default min-date: {min_date}")
    else:
        # Parse min_date from argument
        try:
            min_date = datetime.datetime.strptime(args.min_date, "%Y-%m-%d").date()
        except ValueError as e:
            print(f"Error parsing min date: {str(e)}")
            sys.exit(1)

    # Compute metrics
    try:
        compute_daily_metrics(
            db_path=args.db_path,
            output_table=args.table_name,
            max_distance_miles=args.max_distance_miles,
            min_date=min_date,
            max_date=max_date,
        )
        print("Metric computation completed successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
