-- ============================================================================
-- DK Aviation Flight Insights - Streamlit in Snowflake Deployment
-- ============================================================================
-- Run this script in Snowsight or SnowSQL to deploy the Streamlit app
-- ============================================================================

-- Set context
USE ROLE ACCOUNTADMIN;
USE DATABASE CAPSTONE;
USE SCHEMA GOLD;
USE WAREHOUSE X_SMALL_CLUSTER;

-- Create a stage for Streamlit files (if not exists)
CREATE STAGE IF NOT EXISTS CAPSTONE.GOLD.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Streamlit application files';

-- Grant permissions (adjust role as needed)
GRANT READ, WRITE ON STAGE CAPSTONE.GOLD.STREAMLIT_STAGE TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- OPTION 1: Create Streamlit App via UI (RECOMMENDED)
-- ============================================================================
-- 1. Go to Snowsight → Streamlit → + Streamlit App
-- 2. Name: DK_AVIATION_FLIGHT_INSIGHTS
-- 3. Warehouse: X_SMALL_CLUSTER (or COMPUTE_WH)
-- 4. Database: CAPSTONE
-- 5. Schema: GOLD
-- 6. Copy the contents of streamlit_app.py into the editor
-- 7. Add packages: plotly, pandas
-- 8. Click Run

-- ============================================================================
-- OPTION 2: Create Streamlit App via SQL (after uploading file to stage)
-- ============================================================================
-- First, upload streamlit_app.py to the stage using Snowsight or SnowSQL:
-- PUT file:///path/to/streamlit_app.py @CAPSTONE.GOLD.STREAMLIT_STAGE AUTO_COMPRESS=FALSE;

-- Then create the Streamlit app:
/*
CREATE OR REPLACE STREAMLIT CAPSTONE.GOLD.DK_AVIATION_FLIGHT_INSIGHTS
    ROOT_LOCATION = '@CAPSTONE.GOLD.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'X_SMALL_CLUSTER'
    COMMENT = 'DK Aviation Flight Insights POC - Customer-facing data application'
    TITLE = 'DK Aviation Flight Insights';
*/

-- ============================================================================
-- Verify the view exists and has data
-- ============================================================================
SELECT COUNT(*) as total_records FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW;

-- Quick test of key queries used by the app
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT TAIL_NUMBER) as unique_aircraft,
    COUNT(DISTINCT AIRCRAFT_MANUFACTURER) as unique_manufacturers,
    MIN(RECORD_TS) as earliest_record,
    MAX(RECORD_TS) as latest_record
FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW;

-- ============================================================================
-- Grant access to the Streamlit app (after creation)
-- ============================================================================
-- GRANT USAGE ON STREAMLIT CAPSTONE.GOLD.DK_AVIATION_FLIGHT_INSIGHTS TO ROLE <your_role>;

-- ============================================================================
-- List existing Streamlit apps
-- ============================================================================
SHOW STREAMLITS IN SCHEMA CAPSTONE.GOLD;

