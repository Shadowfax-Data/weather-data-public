#!/usr/bin/env python3
import argparse
import os
import sys
import time

import duckdb


def main():
    parser = argparse.ArgumentParser(description="Export DuckDB table to S3 as parquet")

    # S3 connection parameters
    parser.add_argument("--access-key", required=True, help="S3 access key")
    parser.add_argument("--secret-key", required=True, help="S3 secret key")
    parser.add_argument("--endpoint-url", required=True, help="S3 endpoint URL")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--prefix", required=True, help="Path prefix in the bucket")

    # DuckDB parameters
    parser.add_argument(
        "--db-path",
        default="weather.duckdb",
        help="Path to DuckDB database (default: weather.duckdb)",
    )
    parser.add_argument(
        "--table-name",
        default="zipcode_daily_metrics",
        help="Table name to export (default: zipcode_daily_metrics)",
    )
    parser.add_argument(
        "--partition-col",
        default="year, month",
        help='Partition column expression for parquet (default: "year, month")',
    )
    parser.add_argument(
        "--where",
        default=None,
        help="SQL WHERE clause to filter the table data (default: None)",
    )
    # Note: Overwrite option removed as it's not implemented for remote file systems
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    args = parser.parse_args()

    # Verify that database file exists
    if not os.path.exists(args.db_path):
        print(f"Error: Database file '{args.db_path}' does not exist", file=sys.stderr)
        sys.exit(1)

    try:
        # Connect to DuckDB
        if args.verbose:
            print(f"Connecting to DuckDB database at '{args.db_path}'")
        conn = duckdb.connect(args.db_path)

        # Verify that the table exists
        table_exists = conn.execute(
            f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{args.table_name}'"
        ).fetchone()[0]
        if table_exists == 0:
            print(
                f"Error: Table '{args.table_name}' does not exist in the database",
                file=sys.stderr,
            )
            sys.exit(1)

        # Configure httpfs for S3 access
        if args.verbose:
            print("Installing and loading httpfs extension for S3 access")
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")

        # Set S3 credentials
        if args.verbose:
            print("Configuring S3 credentials")
        conn.execute(f"SET s3_access_key_id='{args.access_key}';")
        conn.execute(f"SET s3_secret_access_key='{args.secret_key}';")
        conn.execute(f"SET s3_endpoint='{args.endpoint_url}';")
        conn.execute("SET s3_use_ssl=true;")
        conn.execute("SET max_memory='512MB';")

        # Build the S3 path
        s3_path = os.path.join(f"s3://{args.bucket}", args.prefix, args.table_name)

        # Create a temporary view to simplify the COPY command
        view_name = f"temp_export_view_{int(time.time())}"

        # Prepare the where clause if provided
        where_clause = f"WHERE {args.where}" if args.where else ""

        # Get the distinct partition values if partition column is provided
        start_time = time.time()

        if args.partition_col:
            # Parse the partition columns
            partition_cols = [col.strip() for col in args.partition_col.split(",")]

            # Query to get all unique partition combinations
            partition_query = f"""
            SELECT DISTINCT {args.partition_col}
            FROM {args.table_name}
            {where_clause}
            ORDER BY {args.partition_col}
            """

            if args.verbose:
                print(f"Querying distinct partition values:\n{partition_query}")

            # Get all unique partition combinations
            partitions = conn.execute(partition_query).fetchall()

            if args.verbose:
                print(f"Found {len(partitions)} partitions to process")

            # Process each partition separately
            for idx, partition_values in enumerate(partitions):
                # Create partition filter conditions
                partition_filters = []
                for i, col in enumerate(partition_cols):
                    partition_filters.append(f"{col} = {partition_values[i]}")

                partition_filter_clause = " AND ".join(partition_filters)

                # Create full where clause combining user filter and partition filter
                full_where_clause = where_clause
                if where_clause and partition_filter_clause:
                    full_where_clause = f"{where_clause} AND {partition_filter_clause}"
                elif partition_filter_clause:
                    full_where_clause = f"WHERE {partition_filter_clause}"

                # Create a unique view name for this partition
                partition_view_name = f"{view_name}_part_{idx}"

                # Create temporary view with filtered data for this partition
                create_view_sql = f"""
                CREATE OR REPLACE TEMPORARY VIEW {partition_view_name} AS
                SELECT * FROM {args.table_name}
                {full_where_clause};
                """

                if args.verbose:
                    partition_desc = ", ".join(
                        [
                            f"{col}={val}"
                            for col, val in zip(partition_cols, partition_values)
                        ]
                    )
                    print(
                        f"\nProcessing partition {idx + 1}/{len(partitions)}: {partition_desc}"
                    )
                    print(f"Creating temporary view:\n{create_view_sql}")

                conn.execute(create_view_sql)

                # Prepare the partition clause for the COPY command
                partition_clause = f", PARTITION_BY ({args.partition_col})"

                # Build the S3 path for this specific partition
                # This still puts everything under the same parent folder for compatibility
                # The file structure will be correctly partitioned due to PARTITION_BY

                # Build the COPY command for this partition
                copy_command = f"""
                COPY {partition_view_name} TO '{s3_path}' (
                    FORMAT PARQUET,
                    OVERWRITE_OR_IGNORE,
                    ROW_GROUP_SIZE 200000
                    {partition_clause}
                );
                """

                # Display the command to be executed
                if args.verbose:
                    print(f"\nExecuting SQL for partition {idx + 1}/{len(partitions)}:")
                    print(copy_command)

                # Execute the COPY command for this partition
                conn.execute(copy_command)

                if args.verbose:
                    print(f"Partition {idx + 1}/{len(partitions)} completed")
        else:
            # If no partition column specified, process everything in one go (original behavior)
            # Create temporary view with filtered data
            create_view_sql = f"""
            CREATE OR REPLACE TEMPORARY VIEW {view_name} AS
            SELECT * FROM {args.table_name}
            {where_clause};
            """

            if args.verbose:
                print("Creating temporary view:")
                print(create_view_sql)

            conn.execute(create_view_sql)

            # Build the simplified COPY command that references the view
            copy_command = f"""
            COPY {view_name} TO '{s3_path}' (
                FORMAT PARQUET,
                OVERWRITE_OR_IGNORE,
                ROW_GROUP_SIZE 200000
            );
            """

            # Display the command to be executed
            if args.verbose:
                print("\nExecuting SQL:")
                print(copy_command)
            else:
                print(
                    f"Exporting filtered data from table '{args.table_name}' to '{s3_path}'"
                )
                if args.where:
                    print(f"Using WHERE clause: {args.where}")

        # Execute the COPY command (only if not partitioned - for partitioned data this is done in the loop above)
        if not args.partition_col:
            conn.execute(copy_command)
        end_time = time.time()

        duration = end_time - start_time
        print(
            f"Successfully exported table '{args.table_name}' to '{s3_path}' as parquet"
        )
        print(f"Export completed in {duration:.2f} seconds")

    except duckdb.Error as e:
        print(f"DuckDB Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        # Close the connection if it exists
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
