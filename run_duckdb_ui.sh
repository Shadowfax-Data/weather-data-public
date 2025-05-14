#!/bin/bash

# Check that duckdb is available
if ! [ -x "$(command -v duckdb)" ]; then
  echo 'Error: duckdb is not installed.' >&2
  echo 'Please install duckdb first (https://duckdb.org/docs/installation) and try again.' >&2
  exit 1
fi

# Set up environment variables for R2 access
# These are shared READ ONLY credentials
export DATA_LAKE_R2_READ_ACCESS_KEY_ID="c39af19dae56c5b548524563de196edc"
export DATA_LAKE_R2_READ_SECRET_ACCESS_KEY="d2ed752a3ca1b99d6e19b1b9b4103498382fa591718d2cd63a205ea4a9b19867"
export ENDPOINT_URL="44007a5f27aad408df6c3ef106f012f1.r2.cloudflarestorage.com"
export BUCKET_NAME="shadowfax-data-lake"

# Create directory for data if it doesn't exist
mkdir -p data

# Download the DuckDB file if it doesn't exist
if [ ! -f "data/weather_base.duckdb" ]; then
    echo "Downloading weather_base.duckdb..."
    curl -o data/weather.duckdb https://datalake.shadowfaxdata.com/weather/weather_base.duckdb
    if [ $? -ne 0 ]; then
        echo "Error downloading weather_base.duckdb"
        exit 1
    fi
    echo "Download complete!"
else
    echo "Using existing weather.duckdb file"
fi

# Create a temporary SQL file for initialization
cat > data/init.sql << EOF
-- Install and load the cache_httpfs extension
INSTALL cache_httpfs FROM community;
LOAD cache_httpfs;

-- Configure S3/R2 settings using CREATE SECRET
CREATE SECRET IF NOT EXISTS (
    TYPE s3,
    KEY_ID '${DATA_LAKE_R2_READ_ACCESS_KEY_ID}',
    SECRET '${DATA_LAKE_R2_READ_SECRET_ACCESS_KEY}',
    REGION 'auto',
    ENDPOINT '${ENDPOINT_URL}',
    URL_STYLE 'path'
);

-- Create the view for zipcode daily metrics
CREATE OR REPLACE VIEW zipcode_daily_metrics_view AS
SELECT * FROM read_parquet('s3://${BUCKET_NAME}/weather/zipcode_daily_metrics/**/*.parquet');

-- Show the created view
DESCRIBE zipcode_daily_metrics_view;
-- Select 5 rows from the view
SELECT * FROM zipcode_daily_metrics_view LIMIT 5;

EOF

# Start DuckDB with the initialization SQL
echo "Starting DuckDB..."
duckdb -ui data/weather.duckdb -init data/init.sql
