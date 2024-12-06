-- Create an internal stage and enable directory service
CREATE STAGE IF NOT EXISTS raw_stg
    DIRECTORY = ( ENABLE = TRUE )
    COMMENT = 'all the air quality raw data will store in this internal stage location';

-- Validate the stage
SHOW STAGES;

-- List files available in stage
LIST @raw_stg;

-- Create file format to process the JSON data
CREATE FILE FORMAT IF NOT EXISTS json_file_format
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    COMMENT = 'this is json file format object';


##############################################################################################
-- You may validate the data present in stage location before coping into the target table
##############################################################################################

-- Have a look at data stored in internal stage
SELECT * 
FROM @dev_db.bronze_sch.raw_stg
    (FILE_FORMAT => json_file_format) t;


-- level-2: Looking at the nested json data
SELECT 
    TRY_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts,
    t.$1,
    t.$1:total::int as record_count,
    t.$1:version::text as json_version  
FROM @dev_db.bronze_sch.raw_stg
(FILE_FORMAT => json_file_format) t;

-- level3: Looking nested data along the metadata 
SELECT 
    TRY_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts,
    t.$1,
    t.$1:total::int as record_count,
    t.$1:version::text as json_version,
    
    -- meta data information
    metadata$filename as _stg_file_name,
    metadata$FILE_LAST_MODIFIED as _stg_file_load_ts,
    metadata$FILE_CONTENT_KEY as _stg_file_md5,
    current_timestamp() as _copy_data_ts
    
FROM @dev_db.bronze_sch.raw_stg
(FILE_FORMAT => json_file_format) t;



##############################################################################################
-- Creating a transient table to store raw air quality data
##############################################################################################

CREATE OR REPLACE TRANSIENT TABLE dev_db.stage_sch.raw_aqi (
    id INT PRIMARY KEY AUTOINCREMENT,           -- Unique identifier
    index_record_ts TIMESTAMP NOT NULL,         -- Timestamp of the record
    json_data VARIANT NOT NULL,                 -- JSON data of the air quality record
    record_count NUMBER NOT NULL DEFAULT 0,     -- Count of records in the JSON
    json_version TEXT NOT NULL,                 -- Version of the JSON schema
    
    -- Audit columns for debugging purposes
    _stg_file_name TEXT,                        -- Name of the staging file
    _stg_file_load_ts TIMESTAMP,                -- Timestamp when the staging file was loaded
    _stg_file_md5 TEXT,                         -- MD5 checksum of the staging file
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP() -- Timestamp when the data was copied
);


##############################################################################################
-- Create Task to schedule the execution of copy into commandt every hour  from stage to raw_aqi table.
##############################################################################################

CREATE OR REPLACE TASK copy_air_quality_data
    WAREHOUSE = compute_wh
    SCHEDULE = '60 MINUTE'                            --'USING CRON 0 * * * * Asia/Kolkata'
AS
COPY INTO raw_aqi (
    index_record_ts,
    json_data,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
) 
FROM (
    SELECT 
        TRY_TO_TIMESTAMP(t.$1:records[0].last_update::TEXT, 'dd-mm-yyyy hh24:mi:ss') AS index_record_ts,
        t.$1 AS json_data,
        t.$1:total::INT AS record_count,
        t.$1:version::TEXT AS json_version,
        metadata$filename AS _stg_file_name,
        metadata$FILE_LAST_MODIFIED AS _stg_file_load_ts,
        metadata$FILE_CONTENT_KEY AS _stg_file_md5,
        CURRENT_TIMESTAMP() AS _copy_data_ts
    FROM @dev_db.bronze_sch.raw_stg AS t
)
FILE_FORMAT = (FORMAT_NAME = 'dev_db.bronze_sch.json_file_format')
ON_ERROR = ABORT_STATEMENT;

-- Verify the data
SELECT *
FROM dev_db.bronze_sch.raw_aqi;