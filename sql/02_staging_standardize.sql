-- =============================================================================
-- ShopSphere Data Warehouse – Staging Standardization
-- File   : 02_staging_standardize.sql
-- Purpose: Transform RAW tables into STAGING layer by applying:
--            1. Timestamp normalization  → TIMESTAMP_NTZ, strip midnight-only dates
--            2. Status standardization   → UPPER CASE, known-value mapping
--            3. Text cleaning            → TRIM, INITCAP for cities/names, UPPER for codes
--            4. Monetary rounding        → ROUND(..., 2), NULL guard to 0.00
--            5. Derived boolean flags    → IS_DELIVERED, IS_CANCELED, IS_POSITIVE, etc.
--            6. Deduplication            → ROW_NUMBER() on natural key, keep latest
--            7. Geolocation aggregation  → one centroid row per ZIP prefix
--
-- Prerequisites: sql/01_setup_and_load.sql must have been run successfully.
-- Run in      : Snowflake worksheet using WH_M_ETL
-- =============================================================================

USE DATABASE SHOPSPHERE_DW;
USE WAREHOUSE WH_M_ETL;


-- =============================================================================
-- SECTION 1 : STAGING TABLE DEFINITIONS
-- (CREATE OR REPLACE — safe to re-run)
-- =============================================================================

-- ── STG_CUSTOMERS ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.STG_CUSTOMERS (
    CUSTOMER_ID         VARCHAR(50)   NOT NULL,
    CUSTOMER_UNIQUE_ID  VARCHAR(50)   NOT NULL,
    ZIP_CODE_PREFIX     VARCHAR(10),
    CITY                VARCHAR(100),           -- INITCAP cleaned
    STATE               VARCHAR(10),            -- UPPER
    _IS_DUPLICATE       BOOLEAN       NOT NULL DEFAULT FALSE,
    _LOADED_AT          TIMESTAMP_NTZ NOT NULL,
    _UPDATED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging: cleaned customers, deduped on CUSTOMER_ID';

-- ── STG_GEOLOCATION ───────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.STG_GEOLOCATION (
    ZIP_CODE_PREFIX     VARCHAR(10)   NOT NULL,  -- one row per ZIP (aggregated)
    CITY                VARCHAR(100),
    STATE               VARCHAR(10),
    AVG_LAT             FLOAT,
    AVG_LNG             FLOAT,
    _LOADED_AT          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging: one centroid row per ZIP prefix, aggregated from raw GPS samples';

-- ── STG_ORDERS ────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.STG_ORDERS (
    ORDER_ID                VARCHAR(50)   NOT NULL,
    CUSTOMER_ID             VARCHAR(50)   NOT NULL,
    ORDER_STATUS            VARCHAR(30)   NOT NULL,  -- UPPER
    PURCHASE_TS             TIMESTAMP_NTZ NOT NULL,
    APPROVED_AT             TIMESTAMP_NTZ,           -- nullable (unpaid)
    DELIVERED_CARRIER_AT    TIMESTAMP_NTZ,
    DELIVERED_CUSTOMER_AT   TIMESTAMP_NTZ,
    ESTIMATED_DELIVERY_DT   DATE,
    IS_DELIVERED            BOOLEAN       NOT NULL,
    IS_CANCELED             BOOLEAN       NOT NULL,
    IS_LATE                 BOOLEAN,                 -- NULL when delivery ts missing
    DELIVERY_DELAY_DAYS     NUMBER(6,0),             -- positive=late, negative=early
    _LOADED_AT              TIMESTAMP_NTZ NOT NULL,
    _UPDATED_AT             TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging: cleaned orders with derived status flags and delivery delay';

-- ── STG_ORDER_ITEMS ───────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.STG_ORDER_ITEMS (
    ORDER_ID            VARCHAR(50)   NOT NULL,
    ORDER_ITEM_ID       NUMBER(5,0)   NOT NULL,
    PRODUCT_ID          VARCHAR(50)   NOT NULL,
    SELLER_ID           VARCHAR(50)   NOT NULL,
    SHIPPING_LIMIT_AT   TIMESTAMP_NTZ,
    PRICE_AMT           NUMBER(12,2)  NOT NULL,
    FREIGHT_AMT         NUMBER(12,2)  NOT NULL,
    TOTAL_ITEM_AMT      NUMBER(12,2)  NOT NULL,  -- PRICE_AMT + FREIGHT_AMT
    _LOADED_AT          TIMESTAMP_NTZ NOT NULL,
    _UPDATED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging: cleaned order line items with total amount derived';

-- ── STG_ORDER_PAYMENTS ────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.STG_ORDER_PAYMENTS (
    ORDER_ID                VARCHAR(50)   NOT NULL,
    PAYMENT_SEQUENTIAL      NUMBER(5,0)   NOT NULL,
    PAYMENT_TYPE            VARCHAR(30)   NOT NULL,  -- UPPER
    PAYMENT_INSTALLMENTS    NUMBER(5,0)   NOT NULL,
    PAYMENT_AMT             NUMBER(12,2)  NOT NULL,
    IS_VOUCHER              BOOLEAN       NOT NULL,
    IS_BOLETO               BOOLEAN       NOT NULL,
    IS_CREDIT_CARD          BOOLEAN       NOT NULL,
    IS_DEBIT_CARD           BOOLEAN       NOT NULL,
    _LOADED_AT              TIMESTAMP_NTZ NOT NULL,
    _UPDATED_AT             TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging: cleaned payments with payment-method boolean flags';

-- ── STG_ORDER_REVIEWS ─────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.STG_ORDER_REVIEWS (
    REVIEW_ID           VARCHAR(50)   NOT NULL,
    ORDER_ID            VARCHAR(50)   NOT NULL,
    REVIEW_SCORE        NUMBER(1,0)   NOT NULL,
    REVIEW_TITLE        VARCHAR(500),            -- TRIM; NULL when blank
    REVIEW_MESSAGE      VARCHAR(5000),           -- TRIM; NULL when blank
    REVIEW_CREATED_DT   DATE          NOT NULL,
    REVIEW_ANSWERED_AT  TIMESTAMP_NTZ,
    IS_POSITIVE         BOOLEAN       NOT NULL,  -- score >= 4
    IS_NEUTRAL          BOOLEAN       NOT NULL,  -- score = 3
    IS_NEGATIVE         BOOLEAN       NOT NULL,  -- score <= 2
    HAS_COMMENT         BOOLEAN       NOT NULL,  -- message is not null/blank
    _IS_DUPLICATE       BOOLEAN       NOT NULL DEFAULT FALSE,
    _LOADED_AT          TIMESTAMP_NTZ NOT NULL,
    _UPDATED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging: cleaned reviews, deduped per order (latest kept), sentiment flags';

-- ── STG_PRODUCTS ──────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.STG_PRODUCTS (
    PRODUCT_ID          VARCHAR(50)   NOT NULL,
    CATEGORY_NAME_PT    VARCHAR(100),
    CATEGORY_NAME_EN    VARCHAR(100),            -- joined from LKP_CATEGORY_NAMES
    PHOTO_CNT           NUMBER(5,0),
    WEIGHT_G            NUMBER(10,2),
    LENGTH_CM           NUMBER(8,2),
    HEIGHT_CM           NUMBER(8,2),
    WIDTH_CM            NUMBER(8,2),
    _LOADED_AT          TIMESTAMP_NTZ NOT NULL,
    _UPDATED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging: cleaned products enriched with English category name';

-- ── STG_SELLERS ───────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.STG_SELLERS (
    SELLER_ID           VARCHAR(50)   NOT NULL,
    ZIP_CODE_PREFIX     VARCHAR(10),
    CITY                VARCHAR(100),
    STATE               VARCHAR(10),
    _LOADED_AT          TIMESTAMP_NTZ NOT NULL,
    _UPDATED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Staging: cleaned sellers';


-- =============================================================================
-- SECTION 2 : INSERT INTO STAGING — TRANSFORMATIONS
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 2.1  STG_CUSTOMERS
--
-- Standardizations applied:
--   TEXT  : CITY → INITCAP(TRIM(...))  e.g. "sao paulo" → "Sao Paulo"
--   TEXT  : STATE → UPPER(TRIM(...))   e.g. "sp" → "SP"
--   TEXT  : CUSTOMER_ID / UNIQUE_ID → LOWER(TRIM(...))  (hex IDs, keep lower)
--   DEDUP : ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY _LOADED_AT DESC)
--           Mark duplicate rows with _IS_DUPLICATE = TRUE
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO STAGING.STG_CUSTOMERS (
    CUSTOMER_ID, CUSTOMER_UNIQUE_ID, ZIP_CODE_PREFIX,
    CITY, STATE,
    _IS_DUPLICATE, _LOADED_AT
)
WITH deduped AS (
    SELECT
        LOWER(TRIM(CUSTOMER_ID))        AS CUSTOMER_ID,
        LOWER(TRIM(CUSTOMER_UNIQUE_ID)) AS CUSTOMER_UNIQUE_ID,
        TRIM(ZIP_CODE_PREFIX)           AS ZIP_CODE_PREFIX,
        INITCAP(TRIM(CITY))             AS CITY,
        UPPER(TRIM(STATE))              AS STATE,
        _LOADED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(TRIM(CUSTOMER_ID))
            ORDER BY _LOADED_AT DESC
        ) AS rn
    FROM RAW.RAW_CUSTOMERS
    WHERE CUSTOMER_ID IS NOT NULL
)
SELECT
    CUSTOMER_ID,
    CUSTOMER_UNIQUE_ID,
    ZIP_CODE_PREFIX,
    CITY,
    STATE,
    (rn > 1)        AS _IS_DUPLICATE,
    _LOADED_AT
FROM deduped;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2.2  STG_GEOLOCATION
--
-- Standardizations applied:
--   DEDUP / AGG : Collapse ~1M GPS samples → one centroid per ZIP prefix
--                 AVG(LAT), AVG(LNG); most-frequent CITY; any STATE
--   TEXT        : CITY → INITCAP(TRIM(...))
--   TEXT        : STATE → UPPER(TRIM(...))
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO STAGING.STG_GEOLOCATION (
    ZIP_CODE_PREFIX, CITY, STATE, AVG_LAT, AVG_LNG
)
WITH city_ranked AS (
    -- find the most-frequent city name per ZIP
    SELECT
        TRIM(ZIP_CODE_PREFIX)  AS ZIP_CODE_PREFIX,
        INITCAP(TRIM(CITY))    AS CITY,
        UPPER(TRIM(STATE))     AS STATE,
        COUNT(*)               AS city_cnt,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(ZIP_CODE_PREFIX)
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM RAW.RAW_GEOLOCATION
    WHERE ZIP_CODE_PREFIX IS NOT NULL
    GROUP BY TRIM(ZIP_CODE_PREFIX), INITCAP(TRIM(CITY)), UPPER(TRIM(STATE))
),
coords AS (
    SELECT
        TRIM(ZIP_CODE_PREFIX)  AS ZIP_CODE_PREFIX,
        AVG(LAT)               AS AVG_LAT,
        AVG(LNG)               AS AVG_LNG
    FROM RAW.RAW_GEOLOCATION
    WHERE ZIP_CODE_PREFIX IS NOT NULL
    GROUP BY TRIM(ZIP_CODE_PREFIX)
)
SELECT
    c.ZIP_CODE_PREFIX,
    r.CITY,
    r.STATE,
    c.AVG_LAT,
    c.AVG_LNG
FROM coords        c
JOIN city_ranked   r ON r.ZIP_CODE_PREFIX = c.ZIP_CODE_PREFIX AND r.rn = 1;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2.3  STG_ORDERS
--
-- Standardizations applied:
--   TIMESTAMP : All timestamps are already TIMESTAMP_NTZ from RAW; kept as-is.
--               ESTIMATED_DELIVERY_DT stored as DATE (time part always 00:00:00).
--   STATUS    : UPPER(TRIM(ORDER_STATUS))
--               Known values: DELIVERED, SHIPPED, CANCELED, INVOICED,
--                             PROCESSING, UNAVAILABLE, APPROVED, CREATED
--   DERIVED   : IS_DELIVERED       = (STATUS = 'DELIVERED')
--               IS_CANCELED        = (STATUS = 'CANCELED')
--               DELIVERY_DELAY_DAYS = DATEDIFF('day', ESTIMATED_DELIVERY_DT,
--                                              DELIVERED_CUSTOMER_AT)
--               IS_LATE            = (DELIVERY_DELAY_DAYS > 0)
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO STAGING.STG_ORDERS (
    ORDER_ID, CUSTOMER_ID, ORDER_STATUS,
    PURCHASE_TS, APPROVED_AT,
    DELIVERED_CARRIER_AT, DELIVERED_CUSTOMER_AT,
    ESTIMATED_DELIVERY_DT,
    IS_DELIVERED, IS_CANCELED,
    IS_LATE, DELIVERY_DELAY_DAYS,
    _LOADED_AT
)
SELECT
    LOWER(TRIM(ORDER_ID))       AS ORDER_ID,
    LOWER(TRIM(CUSTOMER_ID))    AS CUSTOMER_ID,
    UPPER(TRIM(ORDER_STATUS))   AS ORDER_STATUS,

    -- Timestamps: cast nulls safely, keep TIMESTAMP_NTZ
    PURCHASE_TS,
    APPROVED_AT,
    DELIVERED_CARRIER_AT,
    DELIVERED_CUSTOMER_AT,

    -- Estimated delivery: source has time 00:00:00 — store as DATE only
    CAST(ESTIMATED_DELIVERY_DT AS DATE) AS ESTIMATED_DELIVERY_DT,

    -- Status flags
    (UPPER(TRIM(ORDER_STATUS)) = 'DELIVERED')  AS IS_DELIVERED,
    (UPPER(TRIM(ORDER_STATUS)) = 'CANCELED')   AS IS_CANCELED,

    -- Delivery performance (NULL when actual delivery ts is missing)
    CASE
        WHEN DELIVERED_CUSTOMER_AT IS NOT NULL AND ESTIMATED_DELIVERY_DT IS NOT NULL
        THEN (DATEDIFF('day', ESTIMATED_DELIVERY_DT, DELIVERED_CUSTOMER_AT) > 0)
        ELSE NULL
    END AS IS_LATE,

    CASE
        WHEN DELIVERED_CUSTOMER_AT IS NOT NULL AND ESTIMATED_DELIVERY_DT IS NOT NULL
        THEN DATEDIFF('day', ESTIMATED_DELIVERY_DT, DELIVERED_CUSTOMER_AT)
        ELSE NULL
    END AS DELIVERY_DELAY_DAYS,

    _LOADED_AT
FROM RAW.RAW_ORDERS
WHERE ORDER_ID IS NOT NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2.4  STG_ORDER_ITEMS
--
-- Standardizations applied:
--   MONETARY : PRICE_AMT, FREIGHT_AMT → ROUND(..., 2); NULL → 0.00
--   DERIVED  : TOTAL_ITEM_AMT = PRICE_AMT + FREIGHT_AMT
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO STAGING.STG_ORDER_ITEMS (
    ORDER_ID, ORDER_ITEM_ID, PRODUCT_ID, SELLER_ID,
    SHIPPING_LIMIT_AT,
    PRICE_AMT, FREIGHT_AMT, TOTAL_ITEM_AMT,
    _LOADED_AT
)
SELECT
    LOWER(TRIM(ORDER_ID))    AS ORDER_ID,
    ORDER_ITEM_ID,
    LOWER(TRIM(PRODUCT_ID))  AS PRODUCT_ID,
    LOWER(TRIM(SELLER_ID))   AS SELLER_ID,
    SHIPPING_LIMIT_AT,

    -- Monetary: coerce NULL to 0, then round to 2dp
    ROUND(COALESCE(PRICE_AMT,   0), 2) AS PRICE_AMT,
    ROUND(COALESCE(FREIGHT_AMT, 0), 2) AS FREIGHT_AMT,
    ROUND(COALESCE(PRICE_AMT,   0) + COALESCE(FREIGHT_AMT, 0), 2) AS TOTAL_ITEM_AMT,

    _LOADED_AT
FROM RAW.RAW_ORDER_ITEMS
WHERE ORDER_ID IS NOT NULL
  AND ORDER_ITEM_ID IS NOT NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2.5  STG_ORDER_PAYMENTS
--
-- Standardizations applied:
--   TEXT     : PAYMENT_TYPE → UPPER(TRIM(...))
--              Raw values: credit_card, boleto, voucher, debit_card, not_defined
--   MONETARY : PAYMENT_AMT → ROUND(..., 2); NULL → 0.00
--   DERIVED  : IS_VOUCHER, IS_BOLETO, IS_CREDIT_CARD, IS_DEBIT_CARD
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO STAGING.STG_ORDER_PAYMENTS (
    ORDER_ID, PAYMENT_SEQUENTIAL, PAYMENT_TYPE,
    PAYMENT_INSTALLMENTS, PAYMENT_AMT,
    IS_VOUCHER, IS_BOLETO, IS_CREDIT_CARD, IS_DEBIT_CARD,
    _LOADED_AT
)
SELECT
    LOWER(TRIM(ORDER_ID))           AS ORDER_ID,
    COALESCE(PAYMENT_SEQUENTIAL, 1) AS PAYMENT_SEQUENTIAL,
    UPPER(TRIM(PAYMENT_TYPE))       AS PAYMENT_TYPE,
    COALESCE(PAYMENT_INSTALLMENTS, 1) AS PAYMENT_INSTALLMENTS,
    ROUND(COALESCE(PAYMENT_AMT, 0), 2) AS PAYMENT_AMT,

    -- Payment type flags
    (UPPER(TRIM(PAYMENT_TYPE)) = 'VOUCHER')     AS IS_VOUCHER,
    (UPPER(TRIM(PAYMENT_TYPE)) = 'BOLETO')      AS IS_BOLETO,
    (UPPER(TRIM(PAYMENT_TYPE)) = 'CREDIT_CARD') AS IS_CREDIT_CARD,
    (UPPER(TRIM(PAYMENT_TYPE)) = 'DEBIT_CARD')  AS IS_DEBIT_CARD,

    _LOADED_AT
FROM RAW.RAW_ORDER_PAYMENTS
WHERE ORDER_ID IS NOT NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2.6  STG_ORDER_REVIEWS
--
-- Standardizations applied:
--   TEXT  : REVIEW_TITLE, REVIEW_MESSAGE → TRIM; set to NULL when empty string
--   DATE  : REVIEW_CREATED_DT → CAST AS DATE (time part always 00:00:00 in source)
--   DEDUP : One review per ORDER_ID — keep the row with the latest REVIEW_ANSWERED_AT;
--           mark others as _IS_DUPLICATE = TRUE
--   DERIVED : IS_POSITIVE (score >= 4), IS_NEUTRAL (score = 3), IS_NEGATIVE (score <= 2)
--             HAS_COMMENT (message not null/blank)
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO STAGING.STG_ORDER_REVIEWS (
    REVIEW_ID, ORDER_ID, REVIEW_SCORE,
    REVIEW_TITLE, REVIEW_MESSAGE,
    REVIEW_CREATED_DT, REVIEW_ANSWERED_AT,
    IS_POSITIVE, IS_NEUTRAL, IS_NEGATIVE,
    HAS_COMMENT, _IS_DUPLICATE,
    _LOADED_AT
)
WITH cleaned AS (
    SELECT
        LOWER(TRIM(REVIEW_ID))   AS REVIEW_ID,
        LOWER(TRIM(ORDER_ID))    AS ORDER_ID,
        REVIEW_SCORE,
        -- Blank string → NULL for text fields
        NULLIF(TRIM(REVIEW_TITLE),   '') AS REVIEW_TITLE,
        NULLIF(TRIM(REVIEW_MESSAGE), '') AS REVIEW_MESSAGE,
        CAST(REVIEW_CREATED_DT AS DATE)  AS REVIEW_CREATED_DT,
        REVIEW_ANSWERED_AT,
        _LOADED_AT,
        -- Dedup: for the same ORDER_ID, keep the latest answer
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(TRIM(ORDER_ID))
            ORDER BY REVIEW_ANSWERED_AT DESC NULLS LAST
        ) AS rn
    FROM RAW.RAW_ORDER_REVIEWS
    WHERE REVIEW_ID IS NOT NULL
      AND ORDER_ID  IS NOT NULL
      AND REVIEW_SCORE BETWEEN 1 AND 5
)
SELECT
    REVIEW_ID,
    ORDER_ID,
    REVIEW_SCORE,
    REVIEW_TITLE,
    REVIEW_MESSAGE,
    REVIEW_CREATED_DT,
    REVIEW_ANSWERED_AT,
    (REVIEW_SCORE >= 4)                          AS IS_POSITIVE,
    (REVIEW_SCORE  = 3)                          AS IS_NEUTRAL,
    (REVIEW_SCORE <= 2)                          AS IS_NEGATIVE,
    (REVIEW_MESSAGE IS NOT NULL)                 AS HAS_COMMENT,
    (rn > 1)                                     AS _IS_DUPLICATE,
    _LOADED_AT
FROM cleaned;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2.7  STG_PRODUCTS
--
-- Standardizations applied:
--   TEXT    : CATEGORY_NAME_PT → LOWER(TRIM(...))  (source is already lowercase)
--   ENRICH  : CATEGORY_NAME_EN → joined from COMMON.LKP_CATEGORY_NAMES
--   NUMERIC : All dimension columns (WEIGHT_G, LENGTH_CM, etc.) → ROUND(..., 2)
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO STAGING.STG_PRODUCTS (
    PRODUCT_ID, CATEGORY_NAME_PT, CATEGORY_NAME_EN,
    PHOTO_CNT,
    WEIGHT_G, LENGTH_CM, HEIGHT_CM, WIDTH_CM,
    _LOADED_AT
)
SELECT
    LOWER(TRIM(p.PRODUCT_ID))          AS PRODUCT_ID,
    LOWER(TRIM(p.CATEGORY_NAME_PT))    AS CATEGORY_NAME_PT,
    lkp.CATEGORY_NAME_EN,              -- NULL when no translation exists

    p.PHOTO_CNT,

    -- Physical dimensions: round to 2dp, keep NULLs as NULL (not 0)
    ROUND(p.WEIGHT_G,   2) AS WEIGHT_G,
    ROUND(p.LENGTH_CM,  2) AS LENGTH_CM,
    ROUND(p.HEIGHT_CM,  2) AS HEIGHT_CM,
    ROUND(p.WIDTH_CM,   2) AS WIDTH_CM,

    p._LOADED_AT
FROM RAW.RAW_PRODUCTS p
LEFT JOIN COMMON.LKP_CATEGORY_NAMES lkp
       ON LOWER(TRIM(p.CATEGORY_NAME_PT)) = LOWER(TRIM(lkp.CATEGORY_NAME_PT))
WHERE p.PRODUCT_ID IS NOT NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2.8  STG_SELLERS
--
-- Standardizations applied:
--   TEXT : CITY  → INITCAP(TRIM(...))
--          STATE → UPPER(TRIM(...))
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO STAGING.STG_SELLERS (
    SELLER_ID, ZIP_CODE_PREFIX, CITY, STATE,
    _LOADED_AT
)
SELECT
    LOWER(TRIM(SELLER_ID))   AS SELLER_ID,
    TRIM(ZIP_CODE_PREFIX)    AS ZIP_CODE_PREFIX,
    INITCAP(TRIM(CITY))      AS CITY,
    UPPER(TRIM(STATE))       AS STATE,
    _LOADED_AT
FROM RAW.RAW_SELLERS
WHERE SELLER_ID IS NOT NULL;


-- =============================================================================
-- SECTION 3 : VERIFY STAGING COUNTS & SPOT-CHECK TRANSFORMATIONS
-- =============================================================================

-- ── Row counts ────────────────────────────────────────────────────────────────
SELECT 'STG_CUSTOMERS'      AS table_name, COUNT(*) AS row_count FROM STAGING.STG_CUSTOMERS      UNION ALL
SELECT 'STG_GEOLOCATION'    AS table_name, COUNT(*) AS row_count FROM STAGING.STG_GEOLOCATION    UNION ALL
SELECT 'STG_ORDERS'         AS table_name, COUNT(*) AS row_count FROM STAGING.STG_ORDERS         UNION ALL
SELECT 'STG_ORDER_ITEMS'    AS table_name, COUNT(*) AS row_count FROM STAGING.STG_ORDER_ITEMS    UNION ALL
SELECT 'STG_ORDER_PAYMENTS' AS table_name, COUNT(*) AS row_count FROM STAGING.STG_ORDER_PAYMENTS UNION ALL
SELECT 'STG_ORDER_REVIEWS'  AS table_name, COUNT(*) AS row_count FROM STAGING.STG_ORDER_REVIEWS  UNION ALL
SELECT 'STG_PRODUCTS'       AS table_name, COUNT(*) AS row_count FROM STAGING.STG_PRODUCTS       UNION ALL
SELECT 'STG_SELLERS'        AS table_name, COUNT(*) AS row_count FROM STAGING.STG_SELLERS
ORDER BY table_name;

-- ── Duplicate flags ───────────────────────────────────────────────────────────
SELECT 'Duplicate customers' AS check_name, COUNT(*) AS cnt
FROM STAGING.STG_CUSTOMERS WHERE _IS_DUPLICATE = TRUE
UNION ALL
SELECT 'Duplicate reviews',  COUNT(*)
FROM STAGING.STG_ORDER_REVIEWS WHERE _IS_DUPLICATE = TRUE;

-- ── Order status distribution (should all be UPPER) ───────────────────────────
SELECT ORDER_STATUS, COUNT(*) AS cnt
FROM STAGING.STG_ORDERS
GROUP BY ORDER_STATUS
ORDER BY cnt DESC;

-- ── Payment type distribution ─────────────────────────────────────────────────
SELECT PAYMENT_TYPE, COUNT(*) AS cnt,
       ROUND(SUM(PAYMENT_AMT), 2) AS total_amt
FROM STAGING.STG_ORDER_PAYMENTS
GROUP BY PAYMENT_TYPE
ORDER BY cnt DESC;

-- ── Review sentiment split ────────────────────────────────────────────────────
SELECT
    REVIEW_SCORE,
    COUNT(*)                                            AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct,
    SUM(CASE WHEN HAS_COMMENT THEN 1 ELSE 0 END)       AS with_comment_cnt
FROM STAGING.STG_ORDER_REVIEWS
WHERE _IS_DUPLICATE = FALSE
GROUP BY REVIEW_SCORE
ORDER BY REVIEW_SCORE DESC;

-- ── Monetary sanity — no negative prices ─────────────────────────────────────
SELECT
    COUNT(CASE WHEN PRICE_AMT   < 0 THEN 1 END) AS neg_price_cnt,
    COUNT(CASE WHEN FREIGHT_AMT < 0 THEN 1 END) AS neg_freight_cnt,
    MIN(PRICE_AMT)                               AS min_price,
    MAX(PRICE_AMT)                               AS max_price,
    ROUND(AVG(PRICE_AMT), 2)                     AS avg_price
FROM STAGING.STG_ORDER_ITEMS;

-- ── Geolocation collapsed correctly ──────────────────────────────────────────
SELECT COUNT(*) AS unique_zip_count FROM STAGING.STG_GEOLOCATION;
-- Expected: ~19,015 unique ZIPs (down from 1,000,163 raw GPS samples)

-- ── Products without English category name ───────────────────────────────────
SELECT COUNT(*) AS products_missing_en_category
FROM STAGING.STG_PRODUCTS
WHERE CATEGORY_NAME_EN IS NULL;

-- ── City text sample — should be Initcap not lowercase ───────────────────────
SELECT DISTINCT CITY
FROM STAGING.STG_CUSTOMERS
ORDER BY CITY
LIMIT 20;
