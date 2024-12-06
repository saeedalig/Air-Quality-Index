-- Function 1:
-- This function is useful when you need to determine which pollutant is most prominent in a given set of air quality measurements 
-- for a particular location and time, ensuring that missing or invalid data doesn't disrupt the analysis.

CREATE OR REPLACE FUNCTION prominent_index(
    pm25 NUMBER,
    pm10 NUMBER,
    so2 NUMBER,
    no2 NUMBER,
    nh3 NUMBER,
    co NUMBER,
    o3 NUMBER
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'prominent_index'
AS $$
def prominent_index(pm25, pm10, so2, no2, nh3, co, o3):
    # Handle None values by replacing them with 0
    pm25 = pm25 if pm25 is not None else 0
    pm10 = pm10 if pm10 is not None else 0
    so2 = so2 if so2 is not None else 0
    no2 = no2 if no2 is not None else 0
    nh3 = nh3 if nh3 is not None else 0
    co = co if co is not None else 0
    o3 = o3 if o3 is not None else 0

    # Create a dictionary to map pollutant names to their values
    variables = {'PM25': pm25, 'PM10': pm10, 'SO2': so2, 'NO2': no2, 'NH3': nh3, 'CO': co, 'O3': o3}
    
    # Find the pollutant with the highest average value
    max_variable = max(variables, key=variables.get)
    
    return max_variable
$$;

-- take the pollutants for testing
select * from dev_db.clean_sch.clean_flatten_aqi_dt;
-- testing
-- 	56,70 , 12	4	17	47	3
-- 	89,70 , 12	4	17	47	3
select prominent_index(41,57,5,21,4,39,11) ;

-- Function 2: To check pm and non-pm pollutants. 
-- To quantify the AQI, we need to have atleast one pm and 2 non-pm pollutants. That's what this function checks
CREATE OR REPLACE FUNCTION three_sub_index_criteria(
    pm25 NUMBER,
    pm10 NUMBER,
    so2 NUMBER,
    no2 NUMBER,
    nh3 NUMBER,
    co NUMBER,
    o3 NUMBER
)
RETURNS NUMBER(38,0)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'three_sub_index_criteria'
AS $$
def three_sub_index_criteria(pm25, pm10, so2, no2, nh3, co, o3):
    # Initialize counters for PM and non-PM pollutants
    pm_count = 0
    non_pm_count = 0

    # Check if PM2.5 or PM10 is greater than 0
    if pm25 is not None and pm25 > 0:
        pm_count = 1
    elif pm10 is not None and pm10 > 0:
        pm_count = 1

    # Check for non-PM pollutants (SO2, NO2, NH3, CO, O3) that are not None or 0
    non_pm_count = min(2, sum(p is not None and p != 0 for p in [so2, no2, nh3, co, o3]))

    # Return the sum of PM count and non-PM count
    return pm_count + non_pm_count
$$;


-- Function 3:
-- There is no need to run this function and even it is excluded from the dynamic table creation.

CREATE OR REPLACE FUNCTION get_int(input_value VARCHAR)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
'
    SELECT 
        CASE 
            WHEN input_value IS NULL THEN 0
            WHEN input_value = ''NA'' THEN 0
            ELSE TO_NUMBER(input_value) 
        END
';

-- Final Wide Table 
CREATE OR REPLACE DYNAMIC TABLE aqi_final_wide_dt
    TARGET_LAG = '120 MINUTES'
    WAREHOUSE = transform_wh
AS
SELECT 
    index_record_ts, 
    YEAR(index_record_ts) AS aqi_year, 
    MONTH(index_record_ts) AS aqi_month, 
    QUARTER(index_record_ts) AS aqi_quarter, 
    DAY(index_record_ts) AS aqi_day, 
    HOUR(index_record_ts) AS aqi_hour, 
    country, 
    state, 
    city, 
    station, 
    latitude, 
    longitude, 
    pm10_avg,
    pm25_avg, 
    so2_avg, 
    no2_avg, 
    nh3_avg, 
    co_avg, 
    o3_avg, 
    
    -- Function1 being called: determines which pollutant is most prominent in a given set of air quality measurements
    prominent_index(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) AS prominent_pollutant, 
    
    CASE
        -- Function2 being called: To check pm and non-pm pollutants.   
        WHEN three_sub_index_criteria(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) > 2 
            THEN GREATEST(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg)
        ELSE 0 
    END AS aqi
FROM 
    dev_db.clean_sch.clean_flatten_aqi_dt;
