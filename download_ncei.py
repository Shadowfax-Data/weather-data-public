#!/usr/bin/env python3
"""
CLI tool to download CSV.GZ files from an FTP server and load them into a DuckDB database.
"""

import argparse
import ftplib
import os
import queue
import signal
import sys
import tempfile
import threading

import duckdb


def download_and_process_files(
    ftp_server, folder_path, db_path, table_name, since_year=None
):
    """
    Connect to FTP server, download all CSV.GZ files, and append to a DuckDB table.

    Args:
        ftp_server (str): FTP server URL
        folder_path (str): Path to folder containing CSV.GZ files
        db_path (str): Path to DuckDB database
        table_name (str): Name of the table to append data to
    """
    # First, query the database to find the max date of existing data
    max_date = None
    try:
        con = duckdb.connect(db_path)
        # Check if table exists
        table_exists = (
            con.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            ).fetchone()
            is not None
        )

        if table_exists:
            # Try to get max date, handling empty table case
            result = con.execute(f"SELECT MAX(date) FROM {table_name}").fetchone()
            if result and result[0] is not None:
                max_date = result[0]
                print(f"Found existing data with max date: {max_date}")
            else:
                print(f"Table {table_name} exists but contains no data")
        else:
            print(f"Table {table_name} does not exist yet")
    except Exception as e:
        print(f"Error querying database: {str(e)}")
    finally:
        con.close()

    print(f"Connecting to FTP server: {ftp_server}")
    ftp = ftplib.FTP(ftp_server)
    ftp.login("anonymous", "anonymous")

    # Remove leading 'ftp://{server}' if present in the folder path
    if folder_path.startswith(f"ftp://{ftp_server}"):
        folder_path = folder_path[len(f"ftp://{ftp_server}") :]

    print(f"Changing to directory: {folder_path}")
    ftp.cwd(folder_path)

    # Get list of CSV.GZ files and sort them alphabetically
    files = []
    ftp.retrlines("NLST *.csv.gz", files.append)
    files.sort()  # Sort files alphabetically

    # Filter files based on since_year if specified
    if since_year:
        files = [f for f in files if int(f[:4]) >= since_year]
        print(f"Filtered files to include only those from year {since_year} or later")

    # Filter files based on max_date if available
    if max_date:
        # Files are named YYYY.csv.gz, so we need to filter based on year
        max_year = max_date.year

        # Truncate data for the max year before proceeding
        try:
            print(f"Truncating data for year {max_year} to ensure clean reload")
            con = duckdb.connect(db_path)
            # Check if table exists before attempting to truncate
            table_exists = (
                con.execute(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                ).fetchone()
                is not None
            )

            if table_exists:
                # Get count of rows to be deleted for logging
                count_result = con.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE EXTRACT(YEAR FROM date) = {max_year}"
                ).fetchone()
                if count_result and count_result[0] > 0:
                    print(f"Deleting {count_result[0]:,} rows from year {max_year}")
                    # Delete data for the max year
                    con.execute(
                        f"DELETE FROM {table_name} WHERE EXTRACT(YEAR FROM date) = {max_year}"
                    )
                    print(f"Successfully truncated data for year {max_year}")
                else:
                    print(f"No data found for year {max_year} to truncate")
        except Exception as e:
            print(f"Error truncating data for year {max_year}: {str(e)}")
        finally:
            con.close()

        # Filter files to include only the max year and newer
        filtered_files = [f for f in files if int(f.split(".")[0]) >= max_year]
        skipped_count = len(files) - len(filtered_files)
        if skipped_count > 0:
            print(f"Skipping {skipped_count} files with year < {max_year}")
        files = filtered_files

    print(f"Found {len(files)} CSV.GZ files to process")

    # Create a temporary directory for downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create queue for file processing
        file_queue = queue.Queue(maxsize=2)

        # Create an event to signal when to stop processing
        stop_event = threading.Event()

        # Set up signal handler for graceful termination
        def signal_handler(sig, frame):
            print("\nInterrupt received, shutting down gracefully...")
            stop_event.set()
            # Don't exit here, let the main thread handle cleanup

        # Register signal handlers
        original_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal_handler)

        # Function to download files
        def download_files():
            try:
                for filename in files:
                    # Check if stop was requested
                    if stop_event.is_set():
                        print("Download thread stopping due to interrupt")
                        break

                    temp_file_path = os.path.join(temp_dir, filename)
                    print(f"Downloading: {filename}")
                    with open(temp_file_path, "wb") as local_file:
                        ftp.retrbinary(
                            f"RETR {filename}", local_file.write, blocksize=2 * 8192
                        )
                    # Check again if stop was requested during download
                    if stop_event.is_set():
                        # Clean up the file we just downloaded
                        os.remove(temp_file_path)
                        print("Download thread stopping due to interrupt")
                        break
                    file_queue.put((filename, temp_file_path))
            finally:
                # Always signal end of files, even if interrupted
                file_queue.put(None)  # Signal end of files

        # Connect to DuckDB with memory limit
        con = duckdb.connect(db_path, config={"memory_limit": "256MB"})

        # Create table if it doesn't exist (we'll assume the schema based on the first file)

        # Start download thread (not as daemon so we can join it properly)
        download_thread = threading.Thread(target=download_files, daemon=True)
        download_thread.start()

        # Process files as they become available
        while True:
            try:
                # Use a timeout to periodically check for stop_event
                item = file_queue.get(timeout=1.0)
                if item is None:
                    break
            except queue.Empty:
                # Check if we should stop
                if stop_event.is_set():
                    print("Main thread stopping due to interrupt")
                    break
                continue

            filename, temp_file_path = item
            try:
                # First time we need to check if table exists
                if not con.execute(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                ).fetchone():
                    # Create table with explicit schema
                    con.execute(f"""
                        CREATE TABLE {table_name} (
                            id VARCHAR,
                            date DATE,
                            element VARCHAR,
                            data_value INTEGER,
                            m_flag VARCHAR,
                            q_flag VARCHAR,
                            s_flag VARCHAR,
                            obs_time VARCHAR
                        )
                    """)
                    rows_before = 0
                else:
                    # Get current row count
                    rows_before = con.execute(
                        f"SELECT COUNT(*) FROM {table_name}"
                    ).fetchone()[0]

                # Now insert the data using DuckDB's default column names
                con.execute(f"""
                    INSERT INTO {table_name}
                    SELECT
                        CAST(column0 AS VARCHAR),
                        CAST(column1 AS DATE),
                        CAST(column2 AS VARCHAR),
                        CAST(column3 AS INTEGER),
                        CAST(column4 AS VARCHAR),
                        CAST(column5 AS VARCHAR),
                        CAST(column6 AS VARCHAR),
                        CAST(column7 AS VARCHAR)
                    FROM read_csv_auto('{temp_file_path}',
                        header=False,
                        dateformat='%Y%m%d',
                        types={{
                            'column0': 'VARCHAR',
                            'column1': 'DATE',
                            'column2': 'VARCHAR',
                            'column3': 'INTEGER',
                            'column4': 'VARCHAR',
                            'column5': 'VARCHAR',
                            'column6': 'VARCHAR',
                            'column7': 'VARCHAR'
                        }})
                """)

                # Get new row count and calculate the difference
                rows_after = con.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                rows_added = rows_after - rows_before

                print(
                    f"Successfully loaded {filename} into {table_name} - {rows_added:,} rows appended"
                )
                # Get total row count
                total_rows = con.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                print(f"Total rows in {table_name}: {total_rows:,}")
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")
            finally:
                # Clean up the temporary file
                os.remove(temp_file_path)
                print(f"Deleted temporary file: {temp_file_path}")
                file_queue.task_done()

    # Close FTP connection
    try:
        ftp.quit()
        print("FTP connection closed")
    except Exception as e:
        print(f"Error closing FTP connection: {str(e)}")

    # Wait for download thread to finish with a timeout
    if download_thread.is_alive():
        print("Waiting for download thread to finish (max 5 seconds)...")
        download_thread.join(timeout=5.0)
        if download_thread.is_alive():
            print("Download thread did not finish in time")

    # Close DuckDB connection
    try:
        con.close()
        print("DuckDB connection closed")
    except Exception as e:
        print(f"Error closing DuckDB connection: {str(e)}")

    # Get final row count if not interrupted
    if not stop_event.is_set():
        try:
            con = duckdb.connect(db_path)
            total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            con.close()
            print(
                f"All files processed and loaded into {db_path}:{table_name} - Total rows: {total_rows:,}"
            )
        except Exception as e:
            print(f"Error getting final row count: {str(e)}")

    # Restore original signal handler
    signal.signal(signal.SIGINT, original_sigint_handler)


def main():
    """Parse command-line arguments and run the processing."""
    parser = argparse.ArgumentParser(
        description="Download CSV.GZ files from an FTP server and load them into a DuckDB database."
    )
    parser.add_argument(
        "--ftp-server",
        default="ftp.ncei.noaa.gov",
        help="FTP server URL (default: ftp.ncei.noaa.gov)",
    )
    parser.add_argument(
        "--folder-path",
        default="/pub/data/ghcn/daily/by_year",
        help="Path to folder containing CSV.GZ files (default: /pub/data/ghcn/daily/by_year)",
    )
    parser.add_argument(
        "--db-path",
        default="weather.duckdb",
        help="Path to DuckDB database (default: weather.duckdb)",
    )
    parser.add_argument(
        "--table-name",
        default="ghcn_daily_raw",
        help="Name of the table to append data to (default: ghcn_daily_raw)",
    )
    parser.add_argument(
        "--since-year",
        type=int,
        help="Only process files from this year or later. If not specified, process all files.",
    )

    args = parser.parse_args()

    try:
        download_and_process_files(
            args.ftp_server,
            args.folder_path,
            args.db_path,
            args.table_name,
            args.since_year,
        )
    except KeyboardInterrupt:
        # This will catch any KeyboardInterrupt that wasn't handled by our signal handler
        print("\nProcess interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()
