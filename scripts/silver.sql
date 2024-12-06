
USE dev_db;
USE SCHEMA dev_db.silver_sch;

####################################################################################
-- DE-DUPLICATION AND FLATTENING OF THE RCORDS
####################################################################################

-- Create or replace a dynamic table to store cleaned and flattened air quality data
CREATE OR REPLACE DYNAMIC TABLE clean_aqi_dt
    TARGET_LAG = 'downstream'  -- Specify the lag time for downstream processes
    WAREHOUSE = transform_wh   -- Set the warehouse for data transformation
AS
  -- Step 1: Rank air quality records based on file load timestamp
  WITH aqi_with_rank AS (
    SELECT 
        index_record_ts,  -- Timestamp for the air quality record
        json_data,         -- Raw JSON data of air quality records
        record_count,      -- Number of records in the data
        json_version,      -- Version of the JSON structure
        _stg_file_name,    -- Name of the staging file
        _stg_file_load_ts, -- Timestamp when the file was loaded
        _stg_file_md5,     -- MD5 checksum of the staging file
        _copy_data_ts,     -- Timestamp of when the data was copied
        ROW_NUMBER() OVER (PARTITION BY index_record_ts ORDER BY _stg_file_load_ts DESC) AS latest_file_rank -- Rank records based on the latest file load timestamp
    FROM dev_db.stage_sch.raw_aqi  -- Source table for raw air quality data
    WHERE index_record_ts IS NOT NULL  -- Filter out records with null timestamps
),
-- Step 2: Retrieve the most recent record for each unique air quality timestamp
unique_aqi_data AS (
    SELECT * 
    FROM 
        aqi_with_rank 
    WHERE latest_file_rank = 1  -- Keep only the most recent record for each timestamp
)

-- Step 3: Flatten the JSON data into structured columns
SELECT 
    index_record_ts,                                               
    hourly_rec.value:country::TEXT AS country,                     
    hourly_rec.value:state::TEXT AS state,                         
    hourly_rec.value:city::TEXT AS city,                           
    hourly_rec.value:station::TEXT AS station ,                     
    hourly_rec.value:latitude::NUMBER(12,7) AS latitude,           
    hourly_rec.value:longitude::NUMBER(12,7) AS longitude,         
    hourly_rec.value:pollutant_id::TEXT AS pollutant_id,           
    hourly_rec.value:pollutant_max::TEXT AS pollutant_max,         
    hourly_rec.value:pollutant_min::TEXT AS pollutant_min,         
    hourly_rec.value:pollutant_avg::TEXT AS pollutant_avg,  
    
    -- Metadata
    _stg_file_name,                                                
    _stg_file_load_ts,                                             
    _stg_file_md5,                                                 
    _copy_data_ts                                                  
FROM unique_aqi_data,                                      
     LATERAL FLATTEN (INPUT => json_data:records) hourly_rec;      


-- Verify the data in the dynamic table
SELECT * 
FROM dev_db.silver_sch.clean_aqi_dt;


####################################################################################
-- Transposed avg pollutants -> reduces the records significntly
####################################################################################

CREATE OR REPLACE DYNAMIC TABLE dev_db.silver_sch.clean_flattened_aqi_dt
    TARGET_LAG = '120 minutes'
    WAREHOUSE = transform_wh
AS
WITH avg_pollutants_transposed AS (
    SELECT 
        INDEX_RECORD_TS,
        COUNTRY,
        STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        MAX(CASE WHEN POLLUTANT_ID = 'PM10' THEN POLLUTANT_AVG END) AS PM10_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'PM2.5' THEN POLLUTANT_AVG END) AS PM25_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'SO2' THEN POLLUTANT_AVG END) AS SO2_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'NO2' THEN POLLUTANT_AVG END) AS NO2_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'NH3' THEN POLLUTANT_AVG END) AS NH3_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'CO' THEN POLLUTANT_AVG END) AS CO_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'OZONE' THEN POLLUTANT_AVG END) AS O3_AVG
    FROM 
        clean_aqi_dt
    GROUP BY 
        INDEX_RECORD_TS, COUNTRY, STATE, CITY, STATION, LATITUDE, LONGITUDE
    ORDER BY 
        COUNTRY, STATE, CITY, STATION
),
cleaned_by_replacing_na AS (
    SELECT 
        INDEX_RECORD_TS,
        COUNTRY,
        REPLACE(STATE, '_', ' ') AS STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        ROUND(COALESCE(NULLIF(PM25_AVG, 'NA'), 0)) AS PM25_AVG,
        ROUND(COALESCE(NULLIF(PM10_AVG, 'NA'), 0)) AS PM10_AVG,
        ROUND(COALESCE(NULLIF(SO2_AVG, 'NA'), 0)) AS SO2_AVG,
        ROUND(COALESCE(NULLIF(NO2_AVG, 'NA'), 0)) AS NO2_AVG,
        ROUND(COALESCE(NULLIF(NH3_AVG, 'NA'), 0)) AS NH3_AVG,
        ROUND(COALESCE(NULLIF(CO_AVG, 'NA'), 0)) AS CO_AVG,
        ROUND(COALESCE(NULLIF(O3_AVG, 'NA'), 0)) AS O3_AVG   
    FROM  avg_pollutants_transposed
)
SELECT 
    *
FROM cleaned_by_replacing_na;

-- Verify the data in the dynamic table
SELECT *
FROM dev_db.silver_sch.clean_flattened_aqi_dt;