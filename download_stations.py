#!/usr/bin/env python3
"""
CLI tool to download the GHCN station data file from NOAA's FTP server and load it into a DuckDB database.
"""

import argparse
import ftplib
import os
import tempfile

import duckdb


def download_and_process_stations(ftp_server, file_path, db_path, table_name):
    """
    Connect to FTP server, download the stations file, and load it into a DuckDB table.

    Args:
        ftp_server (str): FTP server URL
        file_path (str): Path to the stations file on the FTP server
        db_path (str): Path to DuckDB database
        table_name (str): Name of the table to store station data
    """
    print(f"Connecting to FTP server: {ftp_server}")
    ftp = ftplib.FTP(ftp_server)
    ftp.login("anonymous", "anonymous")

    # Extract directory and filename from the file_path
    directory = os.path.dirname(file_path)
    filename = os.path.basename(file_path)

    # Remove leading 'ftp://{server}' if present in the directory path
    if directory.startswith(f"ftp://{ftp_server}"):
        directory = directory[len(f"ftp://{ftp_server}") :]

    print(f"Changing to directory: {directory}")
    ftp.cwd(directory)

    # Create a temporary directory for download
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = os.path.join(temp_dir, filename)

        # Download the file
        print(f"Downloading: {filename}")
        with open(temp_file_path, "wb") as local_file:
            ftp.retrbinary(f"RETR {filename}", local_file.write, blocksize=2 * 8192)

        # Close FTP connection
        ftp.quit()
        print("FTP connection closed")

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
                    id VARCHAR,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    elevation DOUBLE,
                    state VARCHAR,
                    name VARCHAR,
                    gsn_flag VARCHAR,
                    hcn_crn_flag VARCHAR,
                    wmo_id VARCHAR
                )
            """)

            # Load the fixed-width data into the table
            print(f"Loading data from {temp_file_path} into {table_name}")

            # Use a SQL query to parse the fixed-width file
            con.execute(f"""
                INSERT INTO {table_name}
                SELECT
                    SUBSTRING(line, 1, 11) AS id,
                    CAST(SUBSTRING(line, 13, 8) AS DOUBLE) AS latitude,
                    CAST(SUBSTRING(line, 22, 9) AS DOUBLE) AS longitude,
                    CAST(SUBSTRING(line, 32, 6) AS DOUBLE) AS elevation,
                    SUBSTRING(line, 39, 2) AS state,
                    TRIM(SUBSTRING(line, 42, 30)) AS name,
                    SUBSTRING(line, 73, 3) AS gsn_flag,
                    SUBSTRING(line, 77, 3) AS hcn_crn_flag,
                    SUBSTRING(line, 81, 5) AS wmo_id
                FROM read_csv('{temp_file_path}', header=False, columns={{'line': 'VARCHAR'}})
            """)

            # Get row count
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"Successfully loaded {row_count:,} stations into {table_name}")

        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")
        finally:
            # Clean up the temporary file
            os.remove(temp_file_path)
            print(f"Deleted temporary file: {temp_file_path}")

            # Print table schema
            print("\nTable schema:")
            for row in con.execute(f"DESCRIBE {table_name}").fetchall():
                print(f"{row[0]:<20}{row[1]:<20}{row[2]}")

            # Close DuckDB connection
            con.close()
            print("DuckDB connection closed")


def main():
    """Parse command-line arguments and run the processing."""
    parser = argparse.ArgumentParser(
        description="Download GHCN station data from NOAA's FTP server and load it into a DuckDB database."
    )
    parser.add_argument(
        "--ftp-server",
        default="ftp.ncei.noaa.gov",
        help="FTP server URL (default: ftp.ncei.noaa.gov)",
    )
    parser.add_argument(
        "--file-path",
        default="/pub/data/ghcn/daily/ghcnd-stations.txt",
        help="Path to the stations file on the FTP server (default: /pub/data/ghcn/daily/ghcnd-stations.txt)",
    )
    parser.add_argument(
        "--db-path",
        default="weather.duckdb",
        help="Path to DuckDB database (default: weather.duckdb)",
    )
    parser.add_argument(
        "--table-name",
        default="ghcnd_stations",
        help="Name of the table to store station data (default: ghcnd_stations)",
    )

    args = parser.parse_args()

    download_and_process_stations(
        args.ftp_server, args.file_path, args.db_path, args.table_name
    )


if __name__ == "__main__":
    main()
