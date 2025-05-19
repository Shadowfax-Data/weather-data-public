# Weather Database Documentation

![Workflow Status](https://github.com/Shadowfax-Data/weather-data-public/actions/workflows/download_ncei.yml/badge.svg)

[Pipeline is running twice daily to ingest the latest weather data](https://github.com/Shadowfax-Data/weather-data-public/actions/workflows/download_ncei.yml)
[Available for instant access on Snowflake Marketplace](https://app.snowflake.com/marketplace/listing/GZTYZI0X30/shadowfax-ai-us-historical-weather-data-by-zip-code-built-by-ai)

This repository contains tools and data for processing weather information from NOAA's Global Historical Climatology Network (GHCN).

## Data Sources

- Weather data is sourced from the National Centers for Environmental Information (NCEI):
  - ncei.noaa.gov/pub/data/ghcn/daily
  - ftp.ncei.noaa.gov
- ZIP code geographic data is sourced from the U.S. Census Bureau (census.gov)

## Public R2 Data Lake
We provide public read only access into our data lake with these credentials:
```
ACCESS_KEY_ID="c39af19dae56c5b548524563de196edc"
SECRET_ACCESS_KEY="d2ed752a3ca1b99d6e19b1b9b4103498382fa591718d2cd63a205ea4a9b19867"
ENDPOINT_URL="44007a5f27aad408df6c3ef106f012f1.r2.cloudflarestorage.com"
BUCKET_NAME="shadowfax-data-lake"
```

## Local DuckDB with remote access to data lake

To run DuckDB with remote access to the data lake:
```bash
# make sure duckdb is installed first
./run_duckdb_ui.sh
```

## Methodology

The system combines three primary data sources to produce zipcode-level daily weather metrics:

1. **NOAA Station Readings** (`ghcn_daily_raw`): Raw weather observations from NOAA's Global Historical Climatology Network, including precipitation, temperature, snow measurements and other metrics.

2. **NOAA Station Location Data** (`ghcnd_stations`): Geographic coordinates and metadata for each NOAA weather station.

3. **ZIP Code Gazette** (`zipcodes`): Geographic information about ZIP codes, including centroid coordinates.

### Data Processing Pipeline

The processing occurs in two main stages:

#### Stage 1: Linking ZIP Codes to Weather Stations

The `compute_zipcode_stations.py` script creates a relationship table (`zipcode_stations`) that maps each ZIP code to its nearest weather stations:

1. For each ZIP code, the script calculates the distance to all weather stations using the Haversine formula (which accounts for Earth's curvature).
2. Performance optimizations include:
   - Pre-filtering stations based on latitude/longitude differences (≤ 5 degrees)
   - Setting a maximum distance threshold (200 miles)
   - Processing ZIP codes in batches to manage memory usage
3. The script ranks stations by proximity to each ZIP code and stores the N closest stations.
4. The resulting table includes distance information and geographic coordinates of both the ZIP code and the station.

#### Stage 2: Computing Daily Weather Metrics by ZIP Code

The `compute_daily_metric.py` script generates daily weather metrics for each ZIP code (`zipcode_daily_metrics`):

1. The script filters the `zipcode_stations` table to include only stations within a specified distance (default: 20 miles).
2. For each date in the specified range:
   - Weather observations from `ghcn_daily_raw` are joined with filtered stations.
   - Quality-flagged data is excluded.
   - For each weather element (PRCP, SNOW, SNWD, TMAX, TMIN), the script calculates:
     - Average value across all stations for each ZIP code
     - Count of observations
     - Standard deviation of observations
3. The aggregated metrics are pivoted into a wide format with columns for each weather element.
4. Results are stored in the `zipcode_daily_metrics` table with year, month, and day components extracted for easier querying.

This methodology enables the creation of a comprehensive dataset that provides daily weather metrics for any ZIP code with nearby weather stations, effectively interpolating point-based weather observations to area-based geographic regions.

## Database Schema: `weather.duckdb`

The database contains the following tables:

### 1. ghcnd_stations
Weather station metadata.
```
id           VARCHAR     # Station identifier
latitude     DOUBLE      # Station latitude
longitude    DOUBLE      # Station longitude
elevation    DOUBLE      # Station elevation
state        VARCHAR     # State code
name         VARCHAR     # Station name
gsn_flag     VARCHAR     # GSN flag
hcn_crn_flag VARCHAR     # HCN/CRN flag
wmo_id       VARCHAR     # WMO identifier
```

### 3. zipcode_daily_metrics
Aggregated weather metrics by zipcode and date.
```
zipcode     VARCHAR     # ZIP code
date        DATE        # Date of observation
year        INTEGER     # Year component of date
month       INTEGER     # Month component of date
day         INTEGER     # Day component of date
prcp        DOUBLE      # Precipitation (mm)
prcp_count  INTEGER     # Count of precipitation observations
prcp_stddev DOUBLE      # Standard deviation of precipitation
snow        DOUBLE      # Snowfall (mm)
snow_count  INTEGER     # Count of snowfall observations
snow_stddev DOUBLE      # Standard deviation of snowfall
snwd        DOUBLE      # Snow depth (mm)
snwd_count  INTEGER     # Count of snow depth observations
snwd_stddev DOUBLE      # Standard deviation of snow depth
tmax        DOUBLE      # Maximum temperature (tenths of degrees C)
tmax_count  INTEGER     # Count of maximum temperature observations
tmax_stddev DOUBLE      # Standard deviation of maximum temperature
tmin        DOUBLE      # Minimum temperature (tenths of degrees C)
tmin_count  INTEGER     # Count of minimum temperature observations
tmin_stddev DOUBLE      # Standard deviation of minimum temperature
```

### 4. zipcode_stations
Mapping between ZIP codes and weather stations.
```
zipcode           VARCHAR     # ZIP code
station_id        VARCHAR     # Station identifier
station_name      VARCHAR     # Station name
station_latitude  DOUBLE      # Station latitude
station_longitude DOUBLE      # Station longitude
zipcode_latitude  DOUBLE      # ZIP code centroid latitude
zipcode_longitude DOUBLE      # ZIP code centroid longitude
distance_miles    DOUBLE      # Distance between station and ZIP code centroid
rank              INTEGER     # Rank of station proximity to ZIP code
```

### 5. zipcodes
Geographic information about ZIP codes.
```
GEOID       VARCHAR     # ZIP code identifier
ALAND       BIGINT      # Land area (square meters)
AWATER      BIGINT      # Water area (square meters)
ALAND_SQMI  DOUBLE      # Land area (square miles)
AWATER_SQMI DOUBLE      # Water area (square miles)
INTPTLAT    DOUBLE      # Internal point latitude
INTPTLONG   DOUBLE      # Internal point longitude
```

### 6. ghcn_daily_raw
Raw weather data from NOAA's GHCN.
```
id          VARCHAR     # Station identifier
date        DATE        # Observation date
element     VARCHAR     # Weather element type (PRCP, TMAX, TMIN, etc.)
data_value  INTEGER     # Value of the weather element
m_flag      VARCHAR     # Measurement flag
q_flag      VARCHAR     # Quality flag
s_flag      VARCHAR     # Source flag
obs_time    VARCHAR     # Observation time
```
