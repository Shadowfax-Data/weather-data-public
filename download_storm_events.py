#!/usr/bin/env python3
"""
CLI tool to download Storm Events CSV.GZ files from NCEI FTP server and load them into a DuckDB database.
"""

import argparse
import ftplib
import os
import queue
import re
import signal
import sys
import tempfile
import threading

import duckdb


def extract_year_from_filename(filename):
    """
    Extract year from storm events filename.
    Example: StormEvents_details-ftp_v1.0_d2025_c20250520.csv.gz -> 2025
    """
    match = re.search(r'_d(\d{4})_', filename)
    if match:
        return int(match.group(1))
    return None


def create_storm_events_table(con, table_name):
    """
    Create the storm events table with explicit schema.
    """
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            BEGIN_YEARMONTH BIGINT,
            BEGIN_DAY BIGINT,
            BEGIN_TIME BIGINT,
            END_YEARMONTH BIGINT,
            END_DAY BIGINT,
            END_TIME BIGINT,
            EPISODE_ID VARCHAR,
            EVENT_ID BIGINT,
            STATE VARCHAR,
            STATE_FIPS BIGINT,
            YEAR BIGINT,
            MONTH_NAME VARCHAR,
            EVENT_TYPE VARCHAR,
            CZ_TYPE VARCHAR,
            CZ_FIPS BIGINT,
            CZ_NAME VARCHAR,
            WFO VARCHAR,
            BEGIN_DATE_TIME TIMESTAMP,
            CZ_TIMEZONE VARCHAR,
            END_DATE_TIME TIMESTAMP,
            INJURIES_DIRECT BIGINT,
            INJURIES_INDIRECT BIGINT,
            DEATHS_DIRECT BIGINT,
            DEATHS_INDIRECT BIGINT,
            DAMAGE_PROPERTY DOUBLE,
            DAMAGE_CROPS DOUBLE,
            SOURCE VARCHAR,
            MAGNITUDE BIGINT,
            MAGNITUDE_TYPE VARCHAR,
            FLOOD_CAUSE VARCHAR,
            CATEGORY VARCHAR,
            TOR_F_SCALE VARCHAR,
            TOR_LENGTH DOUBLE,
            TOR_WIDTH BIGINT,
            TOR_OTHER_WFO VARCHAR,
            TOR_OTHER_CZ_STATE VARCHAR,
            TOR_OTHER_CZ_FIPS VARCHAR,
            TOR_OTHER_CZ_NAME VARCHAR,
            BEGIN_RANGE BIGINT,
            BEGIN_AZIMUTH VARCHAR,
            BEGIN_LOCATION VARCHAR,
            END_RANGE BIGINT,
            END_AZIMUTH VARCHAR,
            END_LOCATION VARCHAR,
            BEGIN_LAT DOUBLE,
            BEGIN_LON DOUBLE,
            END_LAT DOUBLE,
            END_LON DOUBLE,
            EPISODE_NARRATIVE VARCHAR,
            EVENT_NARRATIVE VARCHAR,
            DATA_SOURCE VARCHAR
        )
    """)


def download_and_process_files(
    ftp_server, folder_path, db_path, table_name, since_year=None
):
    """
    Connect to FTP server, download all Storm Events CSV.GZ files, and append to a DuckDB table.

    Args:
        ftp_server (str): FTP server URL
        folder_path (str): Path to folder containing CSV.GZ files
        db_path (str): Path to DuckDB database
        table_name (str): Name of the table to append data to
        since_year (int): Only process files from this year or later
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
            result = con.execute(f"SELECT MAX(BEGIN_YEARMONTH) FROM {table_name}").fetchone()
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

    # Get list of Storm Events CSV.GZ files and sort them alphabetically
    files = []
    ftp.retrlines("NLST StormEvents_details*.csv.gz", files.append)
    files.sort()  # Sort files alphabetically

    # Filter files based on since_year if specified
    if since_year:
        filtered_files = []
        for f in files:
            file_year = extract_year_from_filename(f)
            if file_year and file_year >= since_year:
                filtered_files.append(f)
        files = filtered_files
        print(f"Filtered files to include only those from year {since_year} or later")

    # Filter files based on max_date if available
    if max_date:
        # Extract year from BEGIN_YEARMONTH (integer like 202412)
        try:
            max_year = max_date // 100  # Extract year from YYYYMM format

            if max_year:
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
                            f"SELECT COUNT(*) FROM {table_name} WHERE YEAR = {max_year}"
                        ).fetchone()
                        if count_result and count_result[0] > 0:
                            print(f"Deleting {count_result[0]:,} rows from year {max_year}")
                            # Delete data for the max year
                            con.execute(
                                f"DELETE FROM {table_name} WHERE YEAR = {max_year}"
                            )
                            print(f"Successfully truncated data for year {max_year}")
                        else:
                            print(f"No data found for year {max_year} to truncate")
                except Exception as e:
                    print(f"Error truncating data for year {max_year}: {str(e)}")
                finally:
                    con.close()

                # Filter files to include only the max year and newer
                filtered_files = []
                for f in files:
                    file_year = extract_year_from_filename(f)
                    if file_year and file_year >= max_year:
                        filtered_files.append(f)
                skipped_count = len(files) - len(filtered_files)
                if skipped_count > 0:
                    print(f"Skipping {skipped_count} files with year < {max_year}")
                files = filtered_files
        except Exception as e:
            print(f"Error parsing max_date {max_date}: {str(e)}")

    print(f"Found {len(files)} Storm Events CSV.GZ files to process")

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

        # Create table if it doesn't exist
        create_storm_events_table(con, table_name)

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
                # Get current row count before insertion
                rows_before = con.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]

                # Create temporary table name
                temp_table_name = f"temp_{table_name}_{threading.current_thread().ident}"

                                # First, read CSV into a temporary table with all VARCHAR columns
                con.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                con.execute(f"""
                    CREATE TABLE {temp_table_name} AS
                    SELECT * FROM read_csv_auto('{temp_file_path}',
                        header=True,
                        ignore_errors=True
                    )
                """)

                # Now insert into final table with proper transformations
                con.execute(f"""
                    INSERT INTO {table_name}
                    SELECT
                        BEGIN_YEARMONTH,
                        BEGIN_DAY,
                        BEGIN_TIME,
                        END_YEARMONTH,
                        END_DAY,
                        END_TIME,
                        EPISODE_ID,
                        EVENT_ID,
                        STATE,
                        STATE_FIPS,
                        YEAR,
                        MONTH_NAME,
                        EVENT_TYPE,
                        CZ_TYPE,
                        CZ_FIPS,
                        CZ_NAME,
                        WFO,
                        -- Compute BEGIN_DATE_TIME from BEGIN_YEARMONTH, BEGIN_DAY, BEGIN_TIME
                        CASE
                            WHEN BEGIN_YEARMONTH IS NULL OR BEGIN_DAY IS NULL OR BEGIN_TIME IS NULL THEN NULL
                            ELSE TRY_CAST(
                                strptime(
                                    CAST(BEGIN_YEARMONTH AS VARCHAR) || '-' ||
                                    LPAD(CAST(BEGIN_DAY AS VARCHAR), 2, '0') || ' ' ||
                                    LPAD(CAST(BEGIN_TIME AS VARCHAR), 4, '0'),
                                    '%Y%m-%d %H%M'
                                ) AS TIMESTAMP
                            )
                        END AS BEGIN_DATE_TIME,
                        CZ_TIMEZONE,
                        -- Compute END_DATE_TIME from END_YEARMONTH, END_DAY, END_TIME
                        CASE
                            WHEN END_YEARMONTH IS NULL OR END_DAY IS NULL OR END_TIME IS NULL THEN NULL
                            ELSE TRY_CAST(
                                strptime(
                                    CAST(END_YEARMONTH AS VARCHAR) || '-' ||
                                    LPAD(CAST(END_DAY AS VARCHAR), 2, '0') || ' ' ||
                                    LPAD(CAST(END_TIME AS VARCHAR), 4, '0'),
                                    '%Y%m-%d %H%M'
                                ) AS TIMESTAMP
                            )
                        END AS END_DATE_TIME,
                        INJURIES_DIRECT,
                        INJURIES_INDIRECT,
                        DEATHS_DIRECT,
                        DEATHS_INDIRECT,
                        -- Transform DAMAGE_PROPERTY: convert "5k" to 5000, "0.00k" to 0, etc.
                        CASE
                            WHEN DAMAGE_PROPERTY IS NULL OR CAST(DAMAGE_PROPERTY AS VARCHAR) = '' THEN 0.0
                            WHEN UPPER(CAST(DAMAGE_PROPERTY AS VARCHAR)) LIKE '%K' THEN
                                CASE
                                    WHEN REPLACE(UPPER(CAST(DAMAGE_PROPERTY AS VARCHAR)), 'K', '') = '' THEN 0.0
                                    ELSE CAST(REPLACE(UPPER(CAST(DAMAGE_PROPERTY AS VARCHAR)), 'K', '') AS DOUBLE) * 1000
                                END
                            WHEN UPPER(CAST(DAMAGE_PROPERTY AS VARCHAR)) LIKE '%M' THEN
                                CASE
                                    WHEN REPLACE(UPPER(CAST(DAMAGE_PROPERTY AS VARCHAR)), 'M', '') = '' THEN 0.0
                                    ELSE CAST(REPLACE(UPPER(CAST(DAMAGE_PROPERTY AS VARCHAR)), 'M', '') AS DOUBLE) * 1000000
                                END
                            WHEN UPPER(CAST(DAMAGE_PROPERTY AS VARCHAR)) LIKE '%B' THEN
                                CASE
                                    WHEN REPLACE(UPPER(CAST(DAMAGE_PROPERTY AS VARCHAR)), 'B', '') = '' THEN 0.0
                                    ELSE CAST(REPLACE(UPPER(CAST(DAMAGE_PROPERTY AS VARCHAR)), 'B', '') AS DOUBLE) * 1000000000
                                END
                            ELSE
                                COALESCE(TRY_CAST(DAMAGE_PROPERTY AS DOUBLE), 0.0)
                        END AS DAMAGE_PROPERTY,
                        -- Transform DAMAGE_CROPS: convert "5k" to 5000, "0.00k" to 0, etc.
                        CASE
                            WHEN DAMAGE_CROPS IS NULL OR CAST(DAMAGE_CROPS AS VARCHAR) = '' THEN 0.0
                            WHEN UPPER(CAST(DAMAGE_CROPS AS VARCHAR)) LIKE '%K' THEN
                                CASE
                                    WHEN REPLACE(UPPER(CAST(DAMAGE_CROPS AS VARCHAR)), 'K', '') = '' THEN 0.0
                                    ELSE CAST(REPLACE(UPPER(CAST(DAMAGE_CROPS AS VARCHAR)), 'K', '') AS DOUBLE) * 1000
                                END
                            WHEN UPPER(CAST(DAMAGE_CROPS AS VARCHAR)) LIKE '%M' THEN
                                CASE
                                    WHEN REPLACE(UPPER(CAST(DAMAGE_CROPS AS VARCHAR)), 'M', '') = '' THEN 0.0
                                    ELSE CAST(REPLACE(UPPER(CAST(DAMAGE_CROPS AS VARCHAR)), 'M', '') AS DOUBLE) * 1000000
                                END
                            WHEN UPPER(CAST(DAMAGE_CROPS AS VARCHAR)) LIKE '%B' THEN
                                CASE
                                    WHEN REPLACE(UPPER(CAST(DAMAGE_CROPS AS VARCHAR)), 'B', '') = '' THEN 0.0
                                    ELSE CAST(REPLACE(UPPER(CAST(DAMAGE_CROPS AS VARCHAR)), 'B', '') AS DOUBLE) * 1000000000
                                END
                            ELSE
                                COALESCE(TRY_CAST(DAMAGE_CROPS AS DOUBLE), 0.0)
                        END AS DAMAGE_CROPS,
                        SOURCE,
                        MAGNITUDE,
                        MAGNITUDE_TYPE,
                        FLOOD_CAUSE,
                        CATEGORY,
                        TOR_F_SCALE,
                        TOR_LENGTH,
                        TOR_WIDTH,
                        TOR_OTHER_WFO,
                        TOR_OTHER_CZ_STATE,
                        TOR_OTHER_CZ_FIPS,
                        TOR_OTHER_CZ_NAME,
                        BEGIN_RANGE,
                        BEGIN_AZIMUTH,
                        BEGIN_LOCATION,
                        END_RANGE,
                        END_AZIMUTH,
                        END_LOCATION,
                        BEGIN_LAT,
                        BEGIN_LON,
                        END_LAT,
                        END_LON,
                        EPISODE_NARRATIVE,
                        EVENT_NARRATIVE,
                        DATA_SOURCE
                    FROM {temp_table_name}
                """)

                # Drop the temporary table
                con.execute(f"DROP TABLE {temp_table_name}")

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
                # Try to clean up temp table if it exists
                try:
                    con.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                except:
                    pass
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
        description="Download Storm Events CSV.GZ files from NCEI FTP server and load them into a DuckDB database."
    )
    parser.add_argument(
        "--ftp-server",
        default="ftp.ncei.noaa.gov",
        help="FTP server URL (default: ftp.ncei.noaa.gov)",
    )
    parser.add_argument(
        "--folder-path",
        default="/pub/data/swdi/stormevents/csvfiles",
        help="Path to folder containing CSV.GZ files (default: /pub/data/swdi/stormevents/csvfiles)",
    )
    parser.add_argument(
        "--db-path",
        default="weather.duckdb",
        help="Path to DuckDB database (default: weather.duckdb)",
    )
    parser.add_argument(
        "--table-name",
        default="storm_events",
        help="Name of the table to append data to (default: storm_events)",
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
