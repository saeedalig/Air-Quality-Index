-- Create dev_db 
CREATE DATABASE IF NOT EXISTS dev_db;

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze_sch;
CREATE SCHEMA IF NOT EXISTS silver_sch;
CREATE SCHEMA IF NOT EXISTS gold_sch;

SHOW DATABASES;
SHOW SCHEMAS LIKE '%SCH%';

-- Create your warehouse
CREATE OR ALTER WAREHOUSE project_wh
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_RESUME = TRUE
    AUTO_SUSPEND = 60
    WAREHOUSE_TYPE = 'STANDARD'
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'this is ETL warehosue for all loading activity';
