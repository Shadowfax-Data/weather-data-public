#!/usr/bin/env python3
"""
CLI tool to download the ZIP code data file from the Census Bureau and load it into a DuckDB database.
"""

import argparse
import os
import tempfile
import zipfile
from urllib.request import urlretrieve

import duckdb


def download_and_process_zipcode(url, db_path, table_name):
    """
    Download the ZIP code file from the given URL, extract it, and load it into a DuckDB table.

    Args:
        url (str): URL to the ZIP code data file
        db_path (str): Path to DuckDB database
        table_name (str): Name of the table to store ZIP code data
    """
    print(f"Downloading from URL: {url}")

    # Create a temporary directory for download and extraction
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download the zip file
        zip_file_path = os.path.join(temp_dir, "zipcode_data.zip")
        print(f"Downloading to: {zip_file_path}")
        urlretrieve(url, zip_file_path)

        # Extract the zip file
        print("Extracting zip file")
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
            # Get the extracted file name (assuming there's only one file or we want the first one)
            extracted_files = zip_ref.namelist()
            if not extracted_files:
                raise ValueError("No files found in the zip archive")

            extracted_file = os.path.join(temp_dir, extracted_files[0])
            print(f"Extracted file: {extracted_file}")

        # Connect to DuckDB
        con = duckdb.connect(db_path)

        try:
            # Check if table exists
            table_exists = (
                con.execute(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                ).fetchone()
                is not None
            )

            # Drop the table if it exists to ensure a clean reload
            if table_exists:
                print(f"Table {table_name} exists, dropping it for a clean reload")
                con.execute(f"DROP TABLE {table_name}")

            # Create the table with the specified schema
            print(f"Creating table {table_name}")
            con.execute(f"""
                CREATE TABLE {table_name} (
                    GEOID VARCHAR,
                    ALAND BIGINT,
                    AWATER BIGINT,
                    ALAND_SQMI DOUBLE,
                    AWATER_SQMI DOUBLE,
                    INTPTLAT DOUBLE,
                    INTPTLONG DOUBLE
                )
            """)

            # Load data from the tab-delimited file
            print(f"Loading data from {extracted_file} into {table_name}")
            con.execute(f"""
                INSERT INTO {table_name}
                SELECT *
                FROM read_csv('{extracted_file}', header=true, delim='\t')
            """)

            # Get row count and column information
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            columns = con.execute(f"PRAGMA table_info({table_name})").fetchall()

            print(
                f"Successfully loaded {row_count:,} ZIP code records into {table_name}"
            )
            print("Table schema:")
            for col in columns:
                print(f"  {col[1]} ({col[2]})")

        except Exception as e:
            print(f"Error processing ZIP code data: {str(e)}")
        finally:
            # Close DuckDB connection
            con.close()
            print("DuckDB connection closed")


def main():
    """Parse command-line arguments and run the processing."""
    parser = argparse.ArgumentParser(
        description="Download ZIP code data from Census Bureau and load it into a DuckDB database."
    )
    parser.add_argument(
        "--url",
        default="https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_zcta_national.zip",
        help="URL to the ZIP code data file (default: https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_zcta_national.zip)",
    )
    parser.add_argument(
        "--db-path",
        default="weather.duckdb",
        help="Path to DuckDB database (default: weather.duckdb)",
    )
    parser.add_argument(
        "--table-name",
        default="zipcodes",
        help="Name of the table to store ZIP code data (default: zipcodes)",
    )

    args = parser.parse_args()

    download_and_process_zipcode(args.url, args.db_path, args.table_name)


if __name__ == "__main__":
    main()
