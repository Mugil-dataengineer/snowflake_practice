-- =============================================================================
-- ShopSphere Data Warehouse – Setup & Data Load
-- File   : 01_setup_and_load.sql
-- Purpose: Create database, schemas, file format, internal stage, RAW tables,
--          upload dataset files, and COPY INTO each RAW table.
--
-- Run order: Execute each section top-to-bottom in a Snowflake worksheet.
-- =============================================================================


-- =============================================================================
-- SECTION 1 : DATABASE & SCHEMA SETUP
-- =============================================================================

USE ROLE SYSADMIN;

-- Database
CREATE DATABASE IF NOT EXISTS SHOPSPHERE_DW
    COMMENT = 'ShopSphere e-commerce data warehouse';

USE DATABASE SHOPSPHERE_DW;

-- Schemas
CREATE SCHEMA IF NOT EXISTS RAW     COMMENT = 'As-is source data landing zone';
CREATE SCHEMA IF NOT EXISTS STAGING COMMENT = 'Cleaned and transformed data';
CREATE SCHEMA IF NOT EXISTS MARTS   COMMENT = 'Star-schema analytics layer';
CREATE SCHEMA IF NOT EXISTS COMMON  COMMENT = 'Shared lookup and reference tables';
CREATE SCHEMA IF NOT EXISTS AUDIT   COMMENT = 'Pipeline and data-quality monitoring';


-- =============================================================================
-- SECTION 2 : VIRTUAL WAREHOUSE
-- =============================================================================

CREATE WAREHOUSE IF NOT EXISTS WH_XS_ADHOC
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Ad-hoc analyst queries';

CREATE WAREHOUSE IF NOT EXISTS WH_M_ETL
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND   = 120
    AUTO_RESUME    = TRUE
    COMMENT        = 'ETL pipeline tasks';

USE WAREHOUSE WH_M_ETL;


-- =============================================================================
-- SECTION 3 : FILE FORMAT
-- =============================================================================

USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT FF_CSV_HEADER
    TYPE                = 'CSV'
    FIELD_DELIMITER     = ','
    RECORD_DELIMITER    = '\n'
    SKIP_HEADER         = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF             = ('NULL', 'null', '', 'NA', 'N/A')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE          = TRUE
    DATE_FORMAT         = 'AUTO'
    TIMESTAMP_FORMAT    = 'AUTO'
    COMMENT             = 'Standard CSV with header row and optional double-quote enclosure';


-- =============================================================================
-- SECTION 4 : INTERNAL STAGE
-- =============================================================================

CREATE OR REPLACE STAGE STG_OLIST_STAGE
    FILE_FORMAT = FF_CSV_HEADER
    COMMENT     = 'Internal stage for Olist CSV dataset files';


-- =============================================================================
-- SECTION 5 : UPLOAD FILES TO STAGE
--
-- Run the PUT commands from SnowSQL CLI (not the Snowflake web UI worksheet).
-- Replace the local path with the actual path on your machine.
--
-- Syntax:
--   PUT file://<local_path>/<filename>  @STG_OLIST_STAGE  AUTO_COMPRESS=TRUE;
--
-- Example (Windows path — use forward slashes in SnowSQL):
-- =============================================================================

PUT file://C:/Users/MugilvarnanM/Downloads/snowflake_dataset/olist_customers_dataset.csv
    @SHOPSPHERE_DW.RAW.STG_OLIST_STAGE
    AUTO_COMPRESS = TRUE
    OVERWRITE     = TRUE;

PUT file://C:/Users/MugilvarnanM/Downloads/snowflake_dataset/olist_geolocation_dataset.csv
    @SHOPSPHERE_DW.RAW.STG_OLIST_STAGE
    AUTO_COMPRESS = TRUE
    OVERWRITE     = TRUE;

PUT file://C:/Users/MugilvarnanM/Downloads/snowflake_dataset/olist_orders_dataset.csv
    @SHOPSPHERE_DW.RAW.STG_OLIST_STAGE
    AUTO_COMPRESS = TRUE
    OVERWRITE     = TRUE;

PUT file://C:/Users/MugilvarnanM/Downloads/snowflake_dataset/olist_order_items_dataset.csv
    @SHOPSPHERE_DW.RAW.STG_OLIST_STAGE
    AUTO_COMPRESS = TRUE
    OVERWRITE     = TRUE;

PUT file://C:/Users/MugilvarnanM/Downloads/snowflake_dataset/olist_order_payments_dataset.csv
    @SHOPSPHERE_DW.RAW.STG_OLIST_STAGE
    AUTO_COMPRESS = TRUE
    OVERWRITE     = TRUE;

PUT file://C:/Users/MugilvarnanM/Downloads/snowflake_dataset/olist_order_reviews_dataset.csv
    @SHOPSPHERE_DW.RAW.STG_OLIST_STAGE
    AUTO_COMPRESS = TRUE
    OVERWRITE     = TRUE;

PUT file://C:/Users/MugilvarnanM/Downloads/snowflake_dataset/olist_products_dataset.csv
    @SHOPSPHERE_DW.RAW.STG_OLIST_STAGE
    AUTO_COMPRESS = TRUE
    OVERWRITE     = TRUE;

PUT file://C:/Users/MugilvarnanM/Downloads/snowflake_dataset/olist_sellers_dataset.csv
    @SHOPSPHERE_DW.RAW.STG_OLIST_STAGE
    AUTO_COMPRESS = TRUE
    OVERWRITE     = TRUE;

PUT file://C:/Users/MugilvarnanM/Downloads/snowflake_dataset/product_category_name_translation.csv
    @SHOPSPHERE_DW.RAW.STG_OLIST_STAGE
    AUTO_COMPRESS = TRUE
    OVERWRITE     = TRUE;

-- Verify all files are staged:
LIST @STG_OLIST_STAGE;


-- =============================================================================
-- SECTION 6 : RAW TABLE DEFINITIONS
-- =============================================================================

-- ── RAW_CUSTOMERS ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE RAW.RAW_CUSTOMERS (
    CUSTOMER_ID         VARCHAR(50),
    CUSTOMER_UNIQUE_ID  VARCHAR(50),
    ZIP_CODE_PREFIX     VARCHAR(10),
    CITY                VARCHAR(100),
    STATE               VARCHAR(10),
    -- Audit columns
    _LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE        VARCHAR(500),
    _ROW_HASH           VARCHAR(64)
)
COMMENT = 'Landing: one row per customer_id (order-scoped identity)';

-- ── RAW_GEOLOCATION ───────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE RAW.RAW_GEOLOCATION (
    ZIP_CODE_PREFIX     VARCHAR(10),
    LAT                 FLOAT,
    LNG                 FLOAT,
    CITY                VARCHAR(100),
    STATE               VARCHAR(10),
    _LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE        VARCHAR(500),
    _ROW_HASH           VARCHAR(64)
)
COMMENT = 'Landing: one row per GPS coordinate sample for a ZIP prefix';

-- ── RAW_ORDERS ────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE RAW.RAW_ORDERS (
    ORDER_ID                VARCHAR(50),
    CUSTOMER_ID             VARCHAR(50),
    ORDER_STATUS            VARCHAR(30),
    PURCHASE_TS             TIMESTAMP_NTZ,
    APPROVED_AT             TIMESTAMP_NTZ,
    DELIVERED_CARRIER_AT    TIMESTAMP_NTZ,
    DELIVERED_CUSTOMER_AT   TIMESTAMP_NTZ,
    ESTIMATED_DELIVERY_DT   DATE,
    _LOADED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE            VARCHAR(500),
    _ROW_HASH               VARCHAR(64)
)
COMMENT = 'Landing: one row per order';

-- ── RAW_ORDER_ITEMS ───────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE RAW.RAW_ORDER_ITEMS (
    ORDER_ID            VARCHAR(50),
    ORDER_ITEM_ID       NUMBER(5,0),
    PRODUCT_ID          VARCHAR(50),
    SELLER_ID           VARCHAR(50),
    SHIPPING_LIMIT_AT   TIMESTAMP_NTZ,
    PRICE_AMT           NUMBER(12,2),
    FREIGHT_AMT         NUMBER(12,2),
    _LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE        VARCHAR(500),
    _ROW_HASH           VARCHAR(64)
)
COMMENT = 'Landing: one row per line item within an order';

-- ── RAW_ORDER_PAYMENTS ────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE RAW.RAW_ORDER_PAYMENTS (
    ORDER_ID                VARCHAR(50),
    PAYMENT_SEQUENTIAL      NUMBER(5,0),
    PAYMENT_TYPE            VARCHAR(30),
    PAYMENT_INSTALLMENTS    NUMBER(5,0),
    PAYMENT_AMT             NUMBER(12,2),
    _LOADED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE            VARCHAR(500),
    _ROW_HASH               VARCHAR(64)
)
COMMENT = 'Landing: one row per payment entry per order';

-- ── RAW_ORDER_REVIEWS ─────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE RAW.RAW_ORDER_REVIEWS (
    REVIEW_ID           VARCHAR(50),
    ORDER_ID            VARCHAR(50),
    REVIEW_SCORE        NUMBER(1,0),
    REVIEW_TITLE        VARCHAR(500),
    REVIEW_MESSAGE      VARCHAR(5000),
    REVIEW_CREATED_DT   DATE,
    REVIEW_ANSWERED_AT  TIMESTAMP_NTZ,
    _LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE        VARCHAR(500),
    _ROW_HASH           VARCHAR(64)
)
COMMENT = 'Landing: one row per review submission';

-- ── RAW_PRODUCTS ──────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE RAW.RAW_PRODUCTS (
    PRODUCT_ID          VARCHAR(50),
    CATEGORY_NAME_PT    VARCHAR(100),
    PRODUCT_NAME_LEN    NUMBER(5,0),
    PRODUCT_DESC_LEN    NUMBER(7,0),
    PHOTO_CNT           NUMBER(5,0),
    WEIGHT_G            NUMBER(10,2),
    LENGTH_CM           NUMBER(8,2),
    HEIGHT_CM           NUMBER(8,2),
    WIDTH_CM            NUMBER(8,2),
    _LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE        VARCHAR(500),
    _ROW_HASH           VARCHAR(64)
)
COMMENT = 'Landing: one row per product SKU';

-- ── RAW_SELLERS ───────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE RAW.RAW_SELLERS (
    SELLER_ID           VARCHAR(50),
    ZIP_CODE_PREFIX     VARCHAR(10),
    CITY                VARCHAR(100),
    STATE               VARCHAR(10),
    _LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE        VARCHAR(500),
    _ROW_HASH           VARCHAR(64)
)
COMMENT = 'Landing: one row per seller';

-- ── COMMON.LKP_CATEGORY_NAMES ─────────────────────────────────────────────────
CREATE OR REPLACE TABLE COMMON.LKP_CATEGORY_NAMES (
    CATEGORY_NAME_PT    VARCHAR(100)  NOT NULL,
    CATEGORY_NAME_EN    VARCHAR(100)  NOT NULL,
    _LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Lookup: Portuguese to English product category name translation';


-- =============================================================================
-- SECTION 7 : COPY INTO RAW TABLES
-- =============================================================================

-- ── RAW_CUSTOMERS ─────────────────────────────────────────────────────────────
COPY INTO RAW.RAW_CUSTOMERS (
    CUSTOMER_ID, CUSTOMER_UNIQUE_ID, ZIP_CODE_PREFIX, CITY, STATE,
    _SOURCE_FILE, _ROW_HASH
)
FROM (
    SELECT
        $1, $2, $3, $4, $5,
        METADATA$FILENAME,
        SHA2(CONCAT_WS('|', $1, $2, $3, $4, $5))
    FROM @RAW.STG_OLIST_STAGE/olist_customers_dataset.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
ON_ERROR    = 'CONTINUE';

-- ── RAW_GEOLOCATION ───────────────────────────────────────────────────────────
COPY INTO RAW.RAW_GEOLOCATION (
    ZIP_CODE_PREFIX, LAT, LNG, CITY, STATE,
    _SOURCE_FILE, _ROW_HASH
)
FROM (
    SELECT
        $1, $2, $3, $4, $5,
        METADATA$FILENAME,
        SHA2(CONCAT_WS('|', $1, $2, $3, $4, $5))
    FROM @RAW.STG_OLIST_STAGE/olist_geolocation_dataset.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
ON_ERROR    = 'CONTINUE';

-- ── RAW_ORDERS ────────────────────────────────────────────────────────────────
COPY INTO RAW.RAW_ORDERS (
    ORDER_ID, CUSTOMER_ID, ORDER_STATUS,
    PURCHASE_TS, APPROVED_AT, DELIVERED_CARRIER_AT,
    DELIVERED_CUSTOMER_AT, ESTIMATED_DELIVERY_DT,
    _SOURCE_FILE, _ROW_HASH
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8,
        METADATA$FILENAME,
        SHA2(CONCAT_WS('|', $1, $2, $3, $4, $5, $6, $7, $8))
    FROM @RAW.STG_OLIST_STAGE/olist_orders_dataset.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
ON_ERROR    = 'CONTINUE';

-- ── RAW_ORDER_ITEMS ───────────────────────────────────────────────────────────
COPY INTO RAW.RAW_ORDER_ITEMS (
    ORDER_ID, ORDER_ITEM_ID, PRODUCT_ID, SELLER_ID,
    SHIPPING_LIMIT_AT, PRICE_AMT, FREIGHT_AMT,
    _SOURCE_FILE, _ROW_HASH
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7,
        METADATA$FILENAME,
        SHA2(CONCAT_WS('|', $1, $2, $3, $4, $5, $6, $7))
    FROM @RAW.STG_OLIST_STAGE/olist_order_items_dataset.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
ON_ERROR    = 'CONTINUE';

-- ── RAW_ORDER_PAYMENTS ────────────────────────────────────────────────────────
COPY INTO RAW.RAW_ORDER_PAYMENTS (
    ORDER_ID, PAYMENT_SEQUENTIAL, PAYMENT_TYPE,
    PAYMENT_INSTALLMENTS, PAYMENT_AMT,
    _SOURCE_FILE, _ROW_HASH
)
FROM (
    SELECT
        $1, $2, $3, $4, $5,
        METADATA$FILENAME,
        SHA2(CONCAT_WS('|', $1, $2, $3, $4, $5))
    FROM @RAW.STG_OLIST_STAGE/olist_order_payments_dataset.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
ON_ERROR    = 'CONTINUE';

-- ── RAW_ORDER_REVIEWS ─────────────────────────────────────────────────────────
COPY INTO RAW.RAW_ORDER_REVIEWS (
    REVIEW_ID, ORDER_ID, REVIEW_SCORE,
    REVIEW_TITLE, REVIEW_MESSAGE,
    REVIEW_CREATED_DT, REVIEW_ANSWERED_AT,
    _SOURCE_FILE, _ROW_HASH
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7,
        METADATA$FILENAME,
        SHA2(CONCAT_WS('|', $1, $2, $3, $4, $5, $6, $7))
    FROM @RAW.STG_OLIST_STAGE/olist_order_reviews_dataset.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
ON_ERROR    = 'CONTINUE';

-- ── RAW_PRODUCTS ──────────────────────────────────────────────────────────────
COPY INTO RAW.RAW_PRODUCTS (
    PRODUCT_ID, CATEGORY_NAME_PT,
    PRODUCT_NAME_LEN, PRODUCT_DESC_LEN, PHOTO_CNT,
    WEIGHT_G, LENGTH_CM, HEIGHT_CM, WIDTH_CM,
    _SOURCE_FILE, _ROW_HASH
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        METADATA$FILENAME,
        SHA2(CONCAT_WS('|', $1, $2, $3, $4, $5, $6, $7, $8, $9))
    FROM @RAW.STG_OLIST_STAGE/olist_products_dataset.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
ON_ERROR    = 'CONTINUE';

-- ── RAW_SELLERS ───────────────────────────────────────────────────────────────
COPY INTO RAW.RAW_SELLERS (
    SELLER_ID, ZIP_CODE_PREFIX, CITY, STATE,
    _SOURCE_FILE, _ROW_HASH
)
FROM (
    SELECT
        $1, $2, $3, $4,
        METADATA$FILENAME,
        SHA2(CONCAT_WS('|', $1, $2, $3, $4))
    FROM @RAW.STG_OLIST_STAGE/olist_sellers_dataset.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
ON_ERROR    = 'CONTINUE';

-- ── COMMON.LKP_CATEGORY_NAMES ─────────────────────────────────────────────────
COPY INTO COMMON.LKP_CATEGORY_NAMES (
    CATEGORY_NAME_PT, CATEGORY_NAME_EN
)
FROM (
    SELECT $1, $2
    FROM @RAW.STG_OLIST_STAGE/product_category_name_translation.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
ON_ERROR    = 'CONTINUE';


-- =============================================================================
-- SECTION 8 : VERIFY LOADS
-- =============================================================================

SELECT 'RAW_CUSTOMERS'        AS table_name, COUNT(*) AS row_count FROM RAW.RAW_CUSTOMERS        UNION ALL
SELECT 'RAW_GEOLOCATION'      AS table_name, COUNT(*) AS row_count FROM RAW.RAW_GEOLOCATION      UNION ALL
SELECT 'RAW_ORDERS'           AS table_name, COUNT(*) AS row_count FROM RAW.RAW_ORDERS           UNION ALL
SELECT 'RAW_ORDER_ITEMS'      AS table_name, COUNT(*) AS row_count FROM RAW.RAW_ORDER_ITEMS      UNION ALL
SELECT 'RAW_ORDER_PAYMENTS'   AS table_name, COUNT(*) AS row_count FROM RAW.RAW_ORDER_PAYMENTS   UNION ALL
SELECT 'RAW_ORDER_REVIEWS'    AS table_name, COUNT(*) AS row_count FROM RAW.RAW_ORDER_REVIEWS    UNION ALL
SELECT 'RAW_PRODUCTS'         AS table_name, COUNT(*) AS row_count FROM RAW.RAW_PRODUCTS         UNION ALL
SELECT 'RAW_SELLERS'          AS table_name, COUNT(*) AS row_count FROM RAW.RAW_SELLERS          UNION ALL
SELECT 'LKP_CATEGORY_NAMES'   AS table_name, COUNT(*) AS row_count FROM COMMON.LKP_CATEGORY_NAMES
ORDER BY table_name;

-- Expected row counts:
-- RAW_CUSTOMERS        99,441
-- RAW_GEOLOCATION   1,000,163
-- RAW_ORDERS           99,441
-- RAW_ORDER_ITEMS     112,650
-- RAW_ORDER_PAYMENTS  103,886
-- RAW_ORDER_REVIEWS   104,164
-- RAW_PRODUCTS         32,951
-- RAW_SELLERS           3,095
-- LKP_CATEGORY_NAMES       71

-- Check for any load errors:
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME    => 'RAW_ORDERS',
    START_TIME    => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
