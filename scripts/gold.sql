

-- Create a dynamic table dim_date 
CREATE OR REPLACE DYNAMIC TABLE date_dim
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = transform_wh
AS
WITH hourly_data AS (
    SELECT 
        index_record_ts AS measurement_time, 
        YEAR(index_record_ts) AS aqi_year, 
        MONTH(index_record_ts) AS aqi_month, 
        QUARTER(index_record_ts) AS aqi_quarter, 
        DAY(index_record_ts) AS aqi_day, 
        HOUR(index_record_ts) + 1 AS aqi_hour
    FROM 
        dev_db.silver_sch.clean_flattened_aqi_dt
    GROUP BY index_record_ts
)
SELECT 
    HASH(measurement_time) AS date_id, 
    *
FROM hourly_data
ORDER BY aqi_year, aqi_month, aqi_day, aqi_hour;


-- Create a dynamic table location_dim 
CREATE OR REPLACE DYNAMIC TABLE location_dim
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = transform_wh
AS
WITH location_data AS (
    SELECT 
        LATITUDE, 
        LONGITUDE, 
        COUNTRY, 
        STATE, 
        CITY, 
        STATION
    FROM dev_db.silver_sch.clean_flattened_aqi_dt
    GROUP BY LATITUDE, LONGITUDE, COUNTRY, STATE, CITY, STATION
)
SELECT 
    HASH(LATITUDE, LONGITUDE) AS location_id, 
    *
FROM location_data
ORDER BY COUNTRY, STATE, CITY, STATION;


-- Create a dynamic air_quality_fact table
CREATE OR REPLACE DYNAMIC TABLE air_quality_fact
    TARGET_LAG = '120 min'
    WAREHOUSE = compute_wh
AS
SELECT 
    HASH(index_record_ts, latitude, longitude) AS aqi_id,
    HASH(index_record_ts) AS date_id,
    HASH(latitude, longitude) AS location_id,
    pm10_avg,
    pm25_avg,
    so2_avg,
    no2_avg,
    nh3_avg,
    co_avg,
    o3_avg,
    prominent_index(PM25_AVG, PM10_AVG, SO2_AVG, NO2_AVG, NH3_AVG, CO_AVG, O3_AVG) AS prominent_pollutant,
    CASE
        WHEN three_sub_index_criteria(PM25_AVG, PM10_AVG, SO2_AVG, NO2_AVG, NH3_AVG, CO_AVG, O3_AVG) > 2 
        THEN GREATEST(PM25_AVG, PM10_AVG, SO2_AVG, NO2_AVG, NH3_AVG, CO_AVG, O3_AVG)
        ELSE 0
    END AS aqi
FROM 
    dev_db.silver_sch.clean_flattened_aqi_dt;