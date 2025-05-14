# Weather Database Documentation

![Workflow Status](https://github.com/Shadowfax-Data/weather-data-public/actions/workflows/download_ncei.yml/badge.svg)

[Pipeline is running twice daily to ingest the latest weather data](https://github.com/Shadowfax-Data/weather-data-public/actions/workflows/download_ncei.yml)

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
