# Weather Database Documentation

![Workflow Status](https://github.com/Shadowfax-Data/weather-data-public/actions/workflows/download_daily_metrics.yml/badge.svg)

[Pipeline is running twice daily to ingest the latest weather data](https://github.com/Shadowfax-Data/weather-data-public/actions/workflows/download_daily_metrics.yml)

[Storm events pipeline is running weekly to ingest the latest storm data](https://github.com/Shadowfax-Data/weather-data-public/actions/workflows/download_storm_events.yml)

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

### 7. storm_events
Storm events data from NOAA's Storm Data publication, containing details about significant weather events that caused fatalities, injuries, property damage, and/or disruption to commerce.
```
BEGIN_YEARMONTH      BIGINT      # Year and month the event began (YYYYMM format)
BEGIN_DAY            BIGINT      # Day of the month the event began
BEGIN_TIME           BIGINT      # Time of day the event began (HHMM format)
END_YEARMONTH        BIGINT      # Year and month the event ended (YYYYMM format)
END_DAY              BIGINT      # Day of the month the event ended
END_TIME             BIGINT      # Time of day the event ended (HHMM format)
EPISODE_ID           VARCHAR     # NWS-assigned ID for storm episode (may contain multiple events)
EVENT_ID             BIGINT      # NWS-assigned unique ID for individual storm event (primary key)
STATE                VARCHAR     # State name where event occurred (all caps)
STATE_FIPS           BIGINT      # State Federal Information Processing Standard number
YEAR                 BIGINT      # Four digit year of the event
MONTH_NAME           VARCHAR     # Full month name (January, February, etc.)
EVENT_TYPE           VARCHAR     # Type of weather event (Hail, Tornado, Thunderstorm Wind, etc.)
CZ_TYPE              VARCHAR     # County (C), Zone (Z), or Marine (M) designation
CZ_FIPS              BIGINT      # County FIPS number or NWS Forecast Zone number
CZ_NAME              VARCHAR     # County/Parish, Zone or Marine name
WFO                  VARCHAR     # National Weather Service Forecast Office identifier
BEGIN_DATE_TIME      TIMESTAMP   # Event start date and time (MM/DD/YYYY HH:MM:SS)
CZ_TIMEZONE          VARCHAR     # Time zone (EST-5, CST-6, MST-7, etc.)
END_DATE_TIME        TIMESTAMP   # Event end date and time (MM/DD/YYYY HH:MM:SS)
INJURIES_DIRECT      BIGINT      # Number of direct injuries caused by the event
INJURIES_INDIRECT    BIGINT      # Number of indirect injuries caused by the event
DEATHS_DIRECT        BIGINT      # Number of direct deaths caused by the event
DEATHS_INDIRECT      BIGINT      # Number of indirect deaths caused by the event
DAMAGE_PROPERTY      DOUBLE      # Estimated property damage in dollars
DAMAGE_CROPS         DOUBLE      # Estimated crop damage in dollars
SOURCE               VARCHAR     # Source reporting the event (Public, Law Enforcement, etc.)
MAGNITUDE            BIGINT      # Measured extent for wind speeds (knots) or hail size (inches)
MAGNITUDE_TYPE       VARCHAR     # Type of magnitude measurement (EG, ES, MS, MG)
FLOOD_CAUSE          VARCHAR     # Reported or estimated cause of flood events
CATEGORY             VARCHAR     # Event category (rarely populated)
TOR_F_SCALE          VARCHAR     # Enhanced Fujita Scale for tornadoes (EF0-EF5)
TOR_LENGTH           DOUBLE      # Length of tornado path on ground (miles)
TOR_WIDTH            BIGINT      # Width of tornado path on ground (yards)
TOR_OTHER_WFO        VARCHAR     # Continuation WFO if tornado crossed forecast areas
TOR_OTHER_CZ_STATE   VARCHAR     # State of continuing tornado segment
TOR_OTHER_CZ_FIPS    VARCHAR     # FIPS of county entered by continuing tornado
TOR_OTHER_CZ_NAME    VARCHAR     # Name of county entered by continuing tornado
BEGIN_RANGE          BIGINT      # Distance to begin location (miles)
BEGIN_AZIMUTH        VARCHAR     # 16-point compass direction to begin location
BEGIN_LOCATION       VARCHAR     # Reference location name for begin point
END_RANGE            BIGINT      # Distance to end location (miles)
END_AZIMUTH          VARCHAR     # 16-point compass direction to end location
END_LOCATION         VARCHAR     # Reference location name for end point
BEGIN_LAT            DOUBLE      # Latitude of event begin point (decimal degrees)
BEGIN_LON            DOUBLE      # Longitude of event begin point (decimal degrees)
END_LAT              DOUBLE      # Latitude of event end point (decimal degrees)
END_LON              DOUBLE      # Longitude of event end point (decimal degrees)
EPISODE_NARRATIVE    VARCHAR     # General description of the storm episode
EVENT_NARRATIVE      VARCHAR     # Detailed description of the individual event
DATA_SOURCE          VARCHAR     # Source of the storm data
```
