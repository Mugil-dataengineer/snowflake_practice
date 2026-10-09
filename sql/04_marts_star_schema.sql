-- =============================================================================
-- ShopSphere Data Warehouse – MARTS Layer: Star Schema
-- File   : 04_marts_star_schema.sql
-- Purpose:
--   SECTION 1  : Dimension table DDL
--                  DIM_DATE         (static calendar — no SCD)
--                  DIM_CUSTOMERS    (SCD Type 2 — tracks city/state changes)
--                  DIM_PRODUCTS     (SCD Type 2 — tracks category changes)
--                  DIM_SELLERS      (Type 1 — overwrite)
--                  DIM_GEOGRAPHY    (Type 1 — overwrite)
--
--   SECTION 2  : Dimension population (initial full load)
--
--   SECTION 3  : Fact table DDL
--                  FACT_ORDER_ITEMS  (grain: order line item)
--                  FACT_PAYMENTS     (grain: payment entry)
--                  FACT_REVIEWS      (grain: review)
--
--   SECTION 4  : Fact population (initial full load)
--
--   SECTION 5  : SCD Type 2 incremental MERGE pattern
--                  How new / changed customer rows are handled
--                  Includes a worked test for attribute changes
--
--   SECTION 6  : Late-arriving data MERGE pattern
--                  Handles order items / payments that arrive after
--                  the fact table has already been loaded
--
--   SECTION 7  : Relationship diagram (comments)
--
-- Prerequisites : 01_setup_and_load.sql, 02_staging_standardize.sql,
--                 03_data_quality.sql
-- Run in        : Snowflake worksheet using WH_M_ETL
-- =============================================================================

USE DATABASE SHOPSPHERE_DW;
USE WAREHOUSE WH_M_ETL;


-- =============================================================================
-- SECTION 1 : DIMENSION TABLE DDL
-- =============================================================================

-- ── DIM_DATE ──────────────────────────────────────────────────────────────────
-- Grain : one row per calendar date (2015-01-01 → 2030-12-31)
-- Key   : DATE_KEY = INTEGER YYYYMMDD (e.g. 20171002)
-- No SCD — dates never change.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE MARTS.DIM_DATE (
    DATE_KEY        NUMBER(8,0)   NOT NULL PRIMARY KEY,  -- YYYYMMDD integer
    FULL_DATE       DATE          NOT NULL UNIQUE,
    DAY_OF_WEEK     NUMBER(1,0)   NOT NULL,  -- 1=Mon … 7=Sun (ISO)
    DAY_NAME        VARCHAR(10)   NOT NULL,  -- 'Monday' … 'Sunday'
    DAY_OF_MONTH    NUMBER(2,0)   NOT NULL,
    DAY_OF_YEAR     NUMBER(3,0)   NOT NULL,
    WEEK_OF_YEAR    NUMBER(2,0)   NOT NULL,
    MONTH_NUM       NUMBER(2,0)   NOT NULL,
    MONTH_NAME      VARCHAR(10)   NOT NULL,  -- 'January' … 'December'
    QUARTER         NUMBER(1,0)   NOT NULL,  -- 1–4
    YEAR            NUMBER(4,0)   NOT NULL,
    IS_WEEKEND      BOOLEAN       NOT NULL
)
COMMENT = 'Calendar dimension — one row per date 2015-01-01 to 2030-12-31';


-- ── DIM_CUSTOMERS  (SCD Type 2) ───────────────────────────────────────────────
-- Grain : one row per customer version
-- Key   : CUSTOMER_KEY (surrogate, autoincrement)
-- NK    : CUSTOMER_UNIQUE_ID — the stable person-level identifier
--         NOTE: we track changes by CUSTOMER_UNIQUE_ID, not CUSTOMER_ID,
--         because CUSTOMER_ID is order-scoped and can repeat.
-- SCD cols: SCD_START_DT, SCD_END_DT, IS_CURRENT
-- Tracked attributes: CITY, STATE, ZIP_CODE_PREFIX
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE MARTS.DIM_CUSTOMERS (
    CUSTOMER_KEY        NUMBER        NOT NULL AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_UNIQUE_ID  VARCHAR(50)   NOT NULL,   -- stable NK (person level)
    CUSTOMER_ID_NK      VARCHAR(50)   NOT NULL,   -- latest order-scoped ID
    ZIP_CODE_PREFIX     VARCHAR(10),
    CITY                VARCHAR(100),
    STATE               VARCHAR(10),
    -- SCD Type 2 versioning
    SCD_START_DT        DATE          NOT NULL,
    SCD_END_DT          DATE,                     -- NULL = current version
    IS_CURRENT          BOOLEAN       NOT NULL DEFAULT TRUE,
    -- Audit
    _LOADED_AT          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _UPDATED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Customer dimension — SCD Type 2 on CITY/STATE/ZIP changes';


-- ── DIM_PRODUCTS  (SCD Type 2) ────────────────────────────────────────────────
-- Grain : one row per product version
-- Key   : PRODUCT_KEY (surrogate)
-- NK    : PRODUCT_ID_NK
-- Tracked attributes: CATEGORY_NAME_EN, CATEGORY_NAME_PT (category reassignments)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE MARTS.DIM_PRODUCTS (
    PRODUCT_KEY         NUMBER        NOT NULL AUTOINCREMENT PRIMARY KEY,
    PRODUCT_ID_NK       VARCHAR(50)   NOT NULL,
    CATEGORY_NAME_PT    VARCHAR(100),
    CATEGORY_NAME_EN    VARCHAR(100),
    PHOTO_CNT           NUMBER(5,0),
    WEIGHT_G            NUMBER(10,2),
    LENGTH_CM           NUMBER(8,2),
    HEIGHT_CM           NUMBER(8,2),
    WIDTH_CM            NUMBER(8,2),
    -- SCD Type 2 versioning
    SCD_START_DT        DATE          NOT NULL,
    SCD_END_DT          DATE,
    IS_CURRENT          BOOLEAN       NOT NULL DEFAULT TRUE,
    -- Audit
    _LOADED_AT          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _UPDATED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Product dimension — SCD Type 2 on category changes';


-- ── DIM_SELLERS  (Type 1 — overwrite) ────────────────────────────────────────
-- Grain : one row per seller
-- No history required — seller location changes are overwritten.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE MARTS.DIM_SELLERS (
    SELLER_KEY          NUMBER        NOT NULL AUTOINCREMENT PRIMARY KEY,
    SELLER_ID_NK        VARCHAR(50)   NOT NULL UNIQUE,
    ZIP_CODE_PREFIX     VARCHAR(10),
    CITY                VARCHAR(100),
    STATE               VARCHAR(10),
    _LOADED_AT          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _UPDATED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Seller dimension — Type 1 (latest values only)';


-- ── DIM_GEOGRAPHY ─────────────────────────────────────────────────────────────
-- Grain : one row per ZIP code prefix
-- Derived from STG_GEOLOCATION centroid + city/state.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE MARTS.DIM_GEOGRAPHY (
    GEOGRAPHY_KEY       NUMBER        NOT NULL AUTOINCREMENT PRIMARY KEY,
    ZIP_CODE_PREFIX     VARCHAR(10)   NOT NULL UNIQUE,
    CITY                VARCHAR(100),
    STATE               VARCHAR(10),
    AVG_LAT             FLOAT,
    AVG_LNG             FLOAT,
    _LOADED_AT          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Geography dimension — one row per ZIP code prefix with centroid coords';


-- =============================================================================
-- SECTION 2 : DIMENSION POPULATION  (INITIAL FULL LOAD)
-- =============================================================================

-- ── 2.1  DIM_DATE — generate calendar rows via GENERATOR() ───────────────────
INSERT INTO MARTS.DIM_DATE (
    DATE_KEY, FULL_DATE,
    DAY_OF_WEEK, DAY_NAME, DAY_OF_MONTH, DAY_OF_YEAR,
    WEEK_OF_YEAR, MONTH_NUM, MONTH_NAME,
    QUARTER, YEAR, IS_WEEKEND
)
WITH date_spine AS (
    -- Generate one row per day from 2015-01-01 to 2030-12-31 (5844 rows)
    SELECT DATEADD('day', SEQ4(), '2015-01-01'::DATE) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 5844))
)
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))          AS DATE_KEY,
    d                                           AS FULL_DATE,
    DAYOFWEEKISO(d)                             AS DAY_OF_WEEK,
    DAYNAME(d)                                  AS DAY_NAME,
    DAY(d)                                      AS DAY_OF_MONTH,
    DAYOFYEAR(d)                                AS DAY_OF_YEAR,
    WEEKOFYEAR(d)                               AS WEEK_OF_YEAR,
    MONTH(d)                                    AS MONTH_NUM,
    MONTHNAME(d)                                AS MONTH_NAME,
    QUARTER(d)                                  AS QUARTER,
    YEAR(d)                                     AS YEAR,
    DAYOFWEEKISO(d) IN (6, 7)                   AS IS_WEEKEND
FROM date_spine;


-- ── 2.2  DIM_CUSTOMERS — initial SCD Type 2 load ─────────────────────────────
-- One row per CUSTOMER_UNIQUE_ID at initial load.
-- SCD_START_DT = first purchase date known from orders.
-- SCD_END_DT   = NULL (all current at load time).
INSERT INTO MARTS.DIM_CUSTOMERS (
    CUSTOMER_UNIQUE_ID, CUSTOMER_ID_NK,
    ZIP_CODE_PREFIX, CITY, STATE,
    SCD_START_DT, SCD_END_DT, IS_CURRENT
)
WITH ranked AS (
    -- For customers with multiple CUSTOMER_IDs (multiple orders),
    -- pick the row associated with the earliest order as the "first" version.
    SELECT
        c.CUSTOMER_UNIQUE_ID,
        c.CUSTOMER_ID     AS CUSTOMER_ID_NK,
        c.ZIP_CODE_PREFIX,
        c.CITY,
        c.STATE,
        MIN(o.PURCHASE_TS)::DATE AS FIRST_ORDER_DT,
        ROW_NUMBER() OVER (
            PARTITION BY c.CUSTOMER_UNIQUE_ID
            ORDER BY MIN(o.PURCHASE_TS) ASC
        ) AS rn
    FROM STAGING.STG_CUSTOMERS c
    LEFT JOIN STAGING.STG_ORDERS o
           ON o.CUSTOMER_ID = c.CUSTOMER_ID
          AND o._IS_DUPLICATE = FALSE
          AND o._IS_VALID     = TRUE
    WHERE c._IS_DUPLICATE = FALSE
      AND c._IS_VALID      = TRUE
    GROUP BY c.CUSTOMER_UNIQUE_ID, c.CUSTOMER_ID,
             c.ZIP_CODE_PREFIX, c.CITY, c.STATE
)
SELECT
    CUSTOMER_UNIQUE_ID,
    CUSTOMER_ID_NK,
    ZIP_CODE_PREFIX,
    CITY,
    STATE,
    COALESCE(FIRST_ORDER_DT, CURRENT_DATE()) AS SCD_START_DT,
    NULL                                      AS SCD_END_DT,
    TRUE                                      AS IS_CURRENT
FROM ranked
WHERE rn = 1;


-- ── 2.3  DIM_PRODUCTS — initial SCD Type 2 load ──────────────────────────────
INSERT INTO MARTS.DIM_PRODUCTS (
    PRODUCT_ID_NK,
    CATEGORY_NAME_PT, CATEGORY_NAME_EN,
    PHOTO_CNT, WEIGHT_G, LENGTH_CM, HEIGHT_CM, WIDTH_CM,
    SCD_START_DT, SCD_END_DT, IS_CURRENT
)
SELECT
    PRODUCT_ID,
    CATEGORY_NAME_PT,
    CATEGORY_NAME_EN,
    PHOTO_CNT,
    WEIGHT_G, LENGTH_CM, HEIGHT_CM, WIDTH_CM,
    CURRENT_DATE() AS SCD_START_DT,
    NULL            AS SCD_END_DT,
    TRUE            AS IS_CURRENT
FROM STAGING.STG_PRODUCTS
WHERE _IS_DUPLICATE = FALSE
  AND _IS_VALID     = TRUE;


-- ── 2.4  DIM_SELLERS ─────────────────────────────────────────────────────────
INSERT INTO MARTS.DIM_SELLERS (
    SELLER_ID_NK, ZIP_CODE_PREFIX, CITY, STATE
)
SELECT SELLER_ID, ZIP_CODE_PREFIX, CITY, STATE
FROM STAGING.STG_SELLERS
WHERE _IS_DUPLICATE = FALSE
  AND _IS_VALID     = TRUE;


-- ── 2.5  DIM_GEOGRAPHY ───────────────────────────────────────────────────────
INSERT INTO MARTS.DIM_GEOGRAPHY (
    ZIP_CODE_PREFIX, CITY, STATE, AVG_LAT, AVG_LNG
)
SELECT ZIP_CODE_PREFIX, CITY, STATE, AVG_LAT, AVG_LNG
FROM STAGING.STG_GEOLOCATION;


-- =============================================================================
-- SECTION 3 : FACT TABLE DDL
-- =============================================================================

-- ── FACT_ORDER_ITEMS  (primary fact) ─────────────────────────────────────────
-- Grain      : one row per order line item
-- Measures   : PRICE_AMT, FREIGHT_AMT, TOTAL_ITEM_AMT, DELIVERY_DELAY_DAYS
-- Dimensions : DATE (purchase), CUSTOMER, PRODUCT, SELLER, GEOGRAPHY
-- Degenerate : ORDER_ID_NK, ORDER_ITEM_ID (order identifiers stored on fact)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE MARTS.FACT_ORDER_ITEMS (
    ORDER_ITEM_KEY          NUMBER        NOT NULL AUTOINCREMENT PRIMARY KEY,
    -- Degenerate dimensions (no dimension table, stored on fact)
    ORDER_ID_NK             VARCHAR(50)   NOT NULL,
    ORDER_ITEM_ID           NUMBER(5,0)   NOT NULL,
    -- Foreign keys → dimensions
    PURCHASE_DATE_KEY       NUMBER(8,0)   NOT NULL,  -- FK → DIM_DATE
    CUSTOMER_KEY            NUMBER        NOT NULL,  -- FK → DIM_CUSTOMERS
    PRODUCT_KEY             NUMBER        NOT NULL,  -- FK → DIM_PRODUCTS
    SELLER_KEY              NUMBER        NOT NULL,  -- FK → DIM_SELLERS
    GEOGRAPHY_KEY           NUMBER,                  -- FK → DIM_GEOGRAPHY (nullable)
    -- Denormalized order-level attributes (avoid join to orders for common queries)
    ORDER_STATUS            VARCHAR(30)   NOT NULL,
    IS_DELIVERED            BOOLEAN       NOT NULL,
    IS_CANCELED             BOOLEAN       NOT NULL,
    IS_LATE                 BOOLEAN,
    DELIVERY_DELAY_DAYS     NUMBER(6,0),
    PURCHASE_TS             TIMESTAMP_NTZ NOT NULL,  -- full timestamp for intraday
    -- Measures
    PRICE_AMT               NUMBER(12,2)  NOT NULL,
    FREIGHT_AMT             NUMBER(12,2)  NOT NULL,
    TOTAL_ITEM_AMT          NUMBER(12,2)  NOT NULL,
    -- Audit
    _LOADED_AT              TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _IS_LATE_ARRIVING       BOOLEAN       NOT NULL DEFAULT FALSE
)
COMMENT = 'Primary fact — grain: one row per order line item';


-- ── FACT_PAYMENTS ─────────────────────────────────────────────────────────────
-- Grain    : one row per payment entry (order × sequential)
-- Measures : PAYMENT_AMT
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE MARTS.FACT_PAYMENTS (
    PAYMENT_KEY             NUMBER        NOT NULL AUTOINCREMENT PRIMARY KEY,
    ORDER_ID_NK             VARCHAR(50)   NOT NULL,
    PAYMENT_SEQUENTIAL      NUMBER(5,0)   NOT NULL,
    -- Foreign keys
    PURCHASE_DATE_KEY       NUMBER(8,0)   NOT NULL,  -- FK → DIM_DATE (order purchase date)
    CUSTOMER_KEY            NUMBER        NOT NULL,  -- FK → DIM_CUSTOMERS
    -- Payment attributes
    PAYMENT_TYPE            VARCHAR(30)   NOT NULL,
    PAYMENT_INSTALLMENTS    NUMBER(5,0)   NOT NULL,
    IS_CREDIT_CARD          BOOLEAN       NOT NULL,
    IS_BOLETO               BOOLEAN       NOT NULL,
    IS_VOUCHER              BOOLEAN       NOT NULL,
    IS_DEBIT_CARD           BOOLEAN       NOT NULL,
    -- Measure
    PAYMENT_AMT             NUMBER(12,2)  NOT NULL,
    -- Audit
    _LOADED_AT              TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _IS_LATE_ARRIVING       BOOLEAN       NOT NULL DEFAULT FALSE
)
COMMENT = 'Payment fact — grain: one row per payment entry per order';


-- ── FACT_REVIEWS ──────────────────────────────────────────────────────────────
-- Grain    : one row per review
-- Measures : REVIEW_SCORE (additive when summed / averaged)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE MARTS.FACT_REVIEWS (
    REVIEW_KEY              NUMBER        NOT NULL AUTOINCREMENT PRIMARY KEY,
    REVIEW_ID_NK            VARCHAR(50)   NOT NULL,
    ORDER_ID_NK             VARCHAR(50)   NOT NULL,
    -- Foreign keys
    ANSWERED_DATE_KEY       NUMBER(8,0)   NOT NULL,  -- FK → DIM_DATE
    CUSTOMER_KEY            NUMBER        NOT NULL,  -- FK → DIM_CUSTOMERS
    -- Review attributes
    REVIEW_SCORE            NUMBER(1,0)   NOT NULL,
    IS_POSITIVE             BOOLEAN       NOT NULL,
    IS_NEUTRAL              BOOLEAN       NOT NULL,
    IS_NEGATIVE             BOOLEAN       NOT NULL,
    HAS_COMMENT             BOOLEAN       NOT NULL,
    REVIEW_TITLE            VARCHAR(500),
    REVIEW_MESSAGE          VARCHAR(5000),
    -- Audit
    _LOADED_AT              TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _IS_LATE_ARRIVING       BOOLEAN       NOT NULL DEFAULT FALSE
)
COMMENT = 'Review fact — grain: one row per customer review';


-- =============================================================================
-- SECTION 4 : FACT POPULATION  (INITIAL FULL LOAD)
-- =============================================================================

-- ── 4.1  FACT_ORDER_ITEMS ────────────────────────────────────────────────────
INSERT INTO MARTS.FACT_ORDER_ITEMS (
    ORDER_ID_NK, ORDER_ITEM_ID,
    PURCHASE_DATE_KEY, CUSTOMER_KEY, PRODUCT_KEY, SELLER_KEY, GEOGRAPHY_KEY,
    ORDER_STATUS, IS_DELIVERED, IS_CANCELED, IS_LATE, DELIVERY_DELAY_DAYS,
    PURCHASE_TS,
    PRICE_AMT, FREIGHT_AMT, TOTAL_ITEM_AMT
)
SELECT
    i.ORDER_ID,
    i.ORDER_ITEM_ID,

    -- DATE_KEY from purchase timestamp
    TO_NUMBER(TO_CHAR(o.PURCHASE_TS::DATE, 'YYYYMMDD'))   AS PURCHASE_DATE_KEY,

    -- CUSTOMER_KEY — join on order's CUSTOMER_ID (order-scoped NK),
    -- then resolve to the DIM_CUSTOMERS surrogate that was current on purchase date.
    dc.CUSTOMER_KEY,

    -- PRODUCT_KEY — current version at time of load
    dp.PRODUCT_KEY,

    -- SELLER_KEY
    ds.SELLER_KEY,

    -- GEOGRAPHY_KEY from customer ZIP (nullable if ZIP not in geography table)
    dg.GEOGRAPHY_KEY,

    -- Denormalized order fields
    o.ORDER_STATUS,
    o.IS_DELIVERED,
    o.IS_CANCELED,
    o.IS_LATE,
    o.DELIVERY_DELAY_DAYS,
    o.PURCHASE_TS,

    -- Measures
    i.PRICE_AMT,
    i.FREIGHT_AMT,
    i.TOTAL_ITEM_AMT

FROM STAGING.STG_ORDER_ITEMS i

-- Join to orders for order-level context
JOIN STAGING.STG_ORDERS o
     ON o.ORDER_ID      = i.ORDER_ID
    AND o._IS_DUPLICATE = FALSE
    AND o._IS_VALID     = TRUE

-- Resolve CUSTOMER_KEY: match order's CUSTOMER_ID to customer unique id,
-- then to the DIM_CUSTOMERS version current on the purchase date.
JOIN STAGING.STG_CUSTOMERS sc
     ON sc.CUSTOMER_ID  = o.CUSTOMER_ID
    AND sc._IS_DUPLICATE = FALSE
JOIN MARTS.DIM_CUSTOMERS dc
     ON dc.CUSTOMER_UNIQUE_ID = sc.CUSTOMER_UNIQUE_ID
    AND dc.SCD_START_DT  <= o.PURCHASE_TS::DATE
    AND (dc.SCD_END_DT   IS NULL OR dc.SCD_END_DT > o.PURCHASE_TS::DATE)

-- Resolve PRODUCT_KEY — current version
JOIN MARTS.DIM_PRODUCTS dp
     ON dp.PRODUCT_ID_NK = i.PRODUCT_ID
    AND dp.IS_CURRENT    = TRUE

-- Resolve SELLER_KEY
JOIN MARTS.DIM_SELLERS ds
     ON ds.SELLER_ID_NK = i.SELLER_ID

-- Geography — LEFT JOIN, nullable
LEFT JOIN STAGING.STG_CUSTOMERS sc2
          ON sc2.CUSTOMER_ID  = o.CUSTOMER_ID
         AND sc2._IS_DUPLICATE = FALSE
LEFT JOIN MARTS.DIM_GEOGRAPHY dg
          ON dg.ZIP_CODE_PREFIX = sc2.ZIP_CODE_PREFIX

WHERE i._IS_DUPLICATE = FALSE
  AND i._IS_VALID     = TRUE;


-- ── 4.2  FACT_PAYMENTS ───────────────────────────────────────────────────────
INSERT INTO MARTS.FACT_PAYMENTS (
    ORDER_ID_NK, PAYMENT_SEQUENTIAL,
    PURCHASE_DATE_KEY, CUSTOMER_KEY,
    PAYMENT_TYPE, PAYMENT_INSTALLMENTS,
    IS_CREDIT_CARD, IS_BOLETO, IS_VOUCHER, IS_DEBIT_CARD,
    PAYMENT_AMT
)
SELECT
    p.ORDER_ID,
    p.PAYMENT_SEQUENTIAL,
    TO_NUMBER(TO_CHAR(o.PURCHASE_TS::DATE, 'YYYYMMDD')) AS PURCHASE_DATE_KEY,
    dc.CUSTOMER_KEY,
    p.PAYMENT_TYPE,
    p.PAYMENT_INSTALLMENTS,
    p.IS_CREDIT_CARD,
    p.IS_BOLETO,
    p.IS_VOUCHER,
    p.IS_DEBIT_CARD,
    p.PAYMENT_AMT
FROM STAGING.STG_ORDER_PAYMENTS p
JOIN STAGING.STG_ORDERS o
     ON o.ORDER_ID      = p.ORDER_ID
    AND o._IS_DUPLICATE = FALSE
    AND o._IS_VALID     = TRUE
JOIN STAGING.STG_CUSTOMERS sc
     ON sc.CUSTOMER_ID  = o.CUSTOMER_ID
    AND sc._IS_DUPLICATE = FALSE
JOIN MARTS.DIM_CUSTOMERS dc
     ON dc.CUSTOMER_UNIQUE_ID = sc.CUSTOMER_UNIQUE_ID
    AND dc.SCD_START_DT  <= o.PURCHASE_TS::DATE
    AND (dc.SCD_END_DT   IS NULL OR dc.SCD_END_DT > o.PURCHASE_TS::DATE)
WHERE p._IS_DUPLICATE = FALSE
  AND p._IS_VALID     = TRUE;


-- ── 4.3  FACT_REVIEWS ────────────────────────────────────────────────────────
INSERT INTO MARTS.FACT_REVIEWS (
    REVIEW_ID_NK, ORDER_ID_NK,
    ANSWERED_DATE_KEY, CUSTOMER_KEY,
    REVIEW_SCORE, IS_POSITIVE, IS_NEUTRAL, IS_NEGATIVE,
    HAS_COMMENT, REVIEW_TITLE, REVIEW_MESSAGE
)
SELECT
    r.REVIEW_ID,
    r.ORDER_ID,
    -- Use review answered date; fall back to created date if null
    TO_NUMBER(TO_CHAR(
        COALESCE(r.REVIEW_ANSWERED_AT::DATE, r.REVIEW_CREATED_DT),
        'YYYYMMDD'
    ))                            AS ANSWERED_DATE_KEY,
    dc.CUSTOMER_KEY,
    r.REVIEW_SCORE,
    r.IS_POSITIVE,
    r.IS_NEUTRAL,
    r.IS_NEGATIVE,
    r.HAS_COMMENT,
    r.REVIEW_TITLE,
    r.REVIEW_MESSAGE
FROM STAGING.STG_ORDER_REVIEWS r
JOIN STAGING.STG_ORDERS o
     ON o.ORDER_ID      = r.ORDER_ID
    AND o._IS_DUPLICATE = FALSE
    AND o._IS_VALID     = TRUE
JOIN STAGING.STG_CUSTOMERS sc
     ON sc.CUSTOMER_ID  = o.CUSTOMER_ID
    AND sc._IS_DUPLICATE = FALSE
JOIN MARTS.DIM_CUSTOMERS dc
     ON dc.CUSTOMER_UNIQUE_ID = sc.CUSTOMER_UNIQUE_ID
    AND dc.SCD_START_DT  <= o.PURCHASE_TS::DATE
    AND (dc.SCD_END_DT   IS NULL OR dc.SCD_END_DT > o.PURCHASE_TS::DATE)
WHERE r._IS_DUPLICATE = FALSE
  AND r._IS_VALID     = TRUE;


-- =============================================================================
-- SECTION 5 : SCD TYPE 2 — INCREMENTAL MERGE PATTERN
--
-- Run this every time a new batch of customer data arrives from STG_CUSTOMERS.
-- The merge does three things:
--   a) INSERT brand-new customers (never seen before)
--   b) EXPIRE the old version when a tracked attribute (CITY/STATE/ZIP) changes
--      — sets SCD_END_DT = today - 1, IS_CURRENT = FALSE
--   c) INSERT the new version with SCD_START_DT = today
--
-- "Unchanged" customers (same CITY/STATE/ZIP) are ignored — no UPDATE needed.
-- =============================================================================

-- ── Step 1: Expire rows where tracked attributes have changed ─────────────────
UPDATE MARTS.DIM_CUSTOMERS dim
SET
    SCD_END_DT  = CURRENT_DATE() - 1,
    IS_CURRENT  = FALSE,
    _UPDATED_AT = CURRENT_TIMESTAMP()
WHERE dim.IS_CURRENT = TRUE
  AND EXISTS (
      SELECT 1
      FROM STAGING.STG_CUSTOMERS stg
      WHERE stg.CUSTOMER_UNIQUE_ID = dim.CUSTOMER_UNIQUE_ID
        AND stg._IS_DUPLICATE      = FALSE
        AND stg._IS_VALID          = TRUE
        -- Attribute changed check — any of the tracked columns differ
        AND (
               COALESCE(stg.CITY,            '') <> COALESCE(dim.CITY,            '')
            OR COALESCE(stg.STATE,           '') <> COALESCE(dim.STATE,           '')
            OR COALESCE(stg.ZIP_CODE_PREFIX, '') <> COALESCE(dim.ZIP_CODE_PREFIX, '')
        )
  );

-- ── Step 2: Insert new version for changed customers + brand-new customers ────
INSERT INTO MARTS.DIM_CUSTOMERS (
    CUSTOMER_UNIQUE_ID, CUSTOMER_ID_NK,
    ZIP_CODE_PREFIX, CITY, STATE,
    SCD_START_DT, SCD_END_DT, IS_CURRENT
)
SELECT
    stg.CUSTOMER_UNIQUE_ID,
    stg.CUSTOMER_ID     AS CUSTOMER_ID_NK,
    stg.ZIP_CODE_PREFIX,
    stg.CITY,
    stg.STATE,
    CURRENT_DATE()       AS SCD_START_DT,
    NULL                 AS SCD_END_DT,
    TRUE                 AS IS_CURRENT
FROM STAGING.STG_CUSTOMERS stg
WHERE stg._IS_DUPLICATE = FALSE
  AND stg._IS_VALID     = TRUE
  -- Only insert if: (a) no current row exists yet, OR (b) current row was just expired above
  AND NOT EXISTS (
      SELECT 1
      FROM MARTS.DIM_CUSTOMERS dim
      WHERE dim.CUSTOMER_UNIQUE_ID = stg.CUSTOMER_UNIQUE_ID
        AND dim.IS_CURRENT         = TRUE
  );


-- =============================================================================
-- SECTION 5B : TEST — SCD TYPE 2 ATTRIBUTE CHANGE SIMULATION
--
-- This block simulates a customer moving from "Sao Paulo" to "Rio De Janeiro".
-- Run each step manually to observe SCD versioning in action.
-- =============================================================================

-- ── TEST STEP 1: Check current state of one customer ──────────────────────────
/*
SELECT CUSTOMER_KEY, CUSTOMER_UNIQUE_ID, CITY, STATE,
       SCD_START_DT, SCD_END_DT, IS_CURRENT
FROM MARTS.DIM_CUSTOMERS
WHERE CUSTOMER_UNIQUE_ID = '861eff4711a542e4b93843c6dd7febb0'  -- example ID
ORDER BY SCD_START_DT;
-- Expected: one row, IS_CURRENT = TRUE, SCD_END_DT = NULL
*/

-- ── TEST STEP 2: Simulate a source update (customer moved city) ───────────────
/*
UPDATE STAGING.STG_CUSTOMERS
SET    CITY = 'Rio De Janeiro',
       STATE = 'RJ',
       ZIP_CODE_PREFIX = '20040'
WHERE  CUSTOMER_UNIQUE_ID = '861eff4711a542e4b93843c6dd7febb0'
  AND  _IS_DUPLICATE = FALSE;
*/

-- ── TEST STEP 3: Re-run the SCD merge (Section 5 above) ──────────────────────
-- After running Section 5, re-query:
/*
SELECT CUSTOMER_KEY, CUSTOMER_UNIQUE_ID, CITY, STATE,
       SCD_START_DT, SCD_END_DT, IS_CURRENT
FROM MARTS.DIM_CUSTOMERS
WHERE CUSTOMER_UNIQUE_ID = '861eff4711a542e4b93843c6dd7febb0'
ORDER BY SCD_START_DT;
-- Expected: TWO rows
--   Row 1: IS_CURRENT = FALSE, SCD_END_DT = yesterday, CITY = 'Sao Paulo'
--   Row 2: IS_CURRENT = TRUE,  SCD_END_DT = NULL,      CITY = 'Rio De Janeiro'
*/

-- ── TEST STEP 4: Historical fact integrity ────────────────────────────────────
-- Old orders should still resolve to the OLD customer version (CITY = 'Sao Paulo')
/*
SELECT f.ORDER_ID_NK, f.PURCHASE_TS, dc.CITY, dc.STATE, dc.IS_CURRENT
FROM MARTS.FACT_ORDER_ITEMS f
JOIN MARTS.DIM_CUSTOMERS dc ON dc.CUSTOMER_KEY = f.CUSTOMER_KEY
WHERE dc.CUSTOMER_UNIQUE_ID = '861eff4711a542e4b93843c6dd7febb0'
ORDER BY f.PURCHASE_TS;
-- All historical orders keep the CITY = 'Sao Paulo' version because
-- CUSTOMER_KEY on the fact was set to the old surrogate at insert time.
*/


-- =============================================================================
-- SECTION 6 : LATE-ARRIVING DATA MERGE PATTERN
--
-- Scenario: An order item or payment arrives AFTER the fact table was already
-- loaded (e.g., a delayed feed, retry, or backfill).
--
-- Strategy:
--   - MERGE into the fact using the natural key (ORDER_ID + ORDER_ITEM_ID)
--   - If the row already exists: UPDATE measures if they changed
--   - If new:                    INSERT with _IS_LATE_ARRIVING = TRUE
--
-- This pattern is idempotent — safe to re-run on any batch.
-- =============================================================================

-- ── 6.1  Late-arriving ORDER ITEMS ───────────────────────────────────────────
MERGE INTO MARTS.FACT_ORDER_ITEMS tgt
USING (
    -- Same SELECT as Section 4.1 — re-resolves surrogate keys at merge time
    SELECT
        i.ORDER_ID                                                AS ORDER_ID_NK,
        i.ORDER_ITEM_ID,
        TO_NUMBER(TO_CHAR(o.PURCHASE_TS::DATE, 'YYYYMMDD'))       AS PURCHASE_DATE_KEY,
        dc.CUSTOMER_KEY,
        dp.PRODUCT_KEY,
        ds.SELLER_KEY,
        dg.GEOGRAPHY_KEY,
        o.ORDER_STATUS, o.IS_DELIVERED, o.IS_CANCELED,
        o.IS_LATE, o.DELIVERY_DELAY_DAYS,
        o.PURCHASE_TS,
        i.PRICE_AMT, i.FREIGHT_AMT, i.TOTAL_ITEM_AMT
    FROM STAGING.STG_ORDER_ITEMS i
    JOIN STAGING.STG_ORDERS o
         ON o.ORDER_ID = i.ORDER_ID AND o._IS_DUPLICATE = FALSE AND o._IS_VALID = TRUE
    JOIN STAGING.STG_CUSTOMERS sc
         ON sc.CUSTOMER_ID = o.CUSTOMER_ID AND sc._IS_DUPLICATE = FALSE
    JOIN MARTS.DIM_CUSTOMERS dc
         ON dc.CUSTOMER_UNIQUE_ID = sc.CUSTOMER_UNIQUE_ID
        AND dc.SCD_START_DT  <= o.PURCHASE_TS::DATE
        AND (dc.SCD_END_DT IS NULL OR dc.SCD_END_DT > o.PURCHASE_TS::DATE)
    JOIN MARTS.DIM_PRODUCTS dp ON dp.PRODUCT_ID_NK = i.PRODUCT_ID AND dp.IS_CURRENT = TRUE
    JOIN MARTS.DIM_SELLERS  ds ON ds.SELLER_ID_NK  = i.SELLER_ID
    LEFT JOIN STAGING.STG_CUSTOMERS sc2
              ON sc2.CUSTOMER_ID = o.CUSTOMER_ID AND sc2._IS_DUPLICATE = FALSE
    LEFT JOIN MARTS.DIM_GEOGRAPHY dg ON dg.ZIP_CODE_PREFIX = sc2.ZIP_CODE_PREFIX
    WHERE i._IS_DUPLICATE = FALSE AND i._IS_VALID = TRUE
) src
ON (tgt.ORDER_ID_NK = src.ORDER_ID_NK AND tgt.ORDER_ITEM_ID = src.ORDER_ITEM_ID)

WHEN MATCHED AND (
    -- Update only if a measure actually changed (avoids unnecessary I/O)
    tgt.PRICE_AMT      <> src.PRICE_AMT   OR
    tgt.FREIGHT_AMT    <> src.FREIGHT_AMT OR
    tgt.ORDER_STATUS   <> src.ORDER_STATUS
) THEN UPDATE SET
    tgt.ORDER_STATUS        = src.ORDER_STATUS,
    tgt.IS_DELIVERED        = src.IS_DELIVERED,
    tgt.IS_CANCELED         = src.IS_CANCELED,
    tgt.IS_LATE             = src.IS_LATE,
    tgt.DELIVERY_DELAY_DAYS = src.DELIVERY_DELAY_DAYS,
    tgt.PRICE_AMT           = src.PRICE_AMT,
    tgt.FREIGHT_AMT         = src.FREIGHT_AMT,
    tgt.TOTAL_ITEM_AMT      = src.TOTAL_ITEM_AMT,
    tgt._LOADED_AT          = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    ORDER_ID_NK, ORDER_ITEM_ID,
    PURCHASE_DATE_KEY, CUSTOMER_KEY, PRODUCT_KEY, SELLER_KEY, GEOGRAPHY_KEY,
    ORDER_STATUS, IS_DELIVERED, IS_CANCELED, IS_LATE, DELIVERY_DELAY_DAYS,
    PURCHASE_TS, PRICE_AMT, FREIGHT_AMT, TOTAL_ITEM_AMT,
    _IS_LATE_ARRIVING
) VALUES (
    src.ORDER_ID_NK, src.ORDER_ITEM_ID,
    src.PURCHASE_DATE_KEY, src.CUSTOMER_KEY, src.PRODUCT_KEY, src.SELLER_KEY, src.GEOGRAPHY_KEY,
    src.ORDER_STATUS, src.IS_DELIVERED, src.IS_CANCELED, src.IS_LATE, src.DELIVERY_DELAY_DAYS,
    src.PURCHASE_TS, src.PRICE_AMT, src.FREIGHT_AMT, src.TOTAL_ITEM_AMT,
    TRUE  -- flagged as late-arriving
);


-- ── 6.2  Late-arriving PAYMENTS ──────────────────────────────────────────────
MERGE INTO MARTS.FACT_PAYMENTS tgt
USING (
    SELECT
        p.ORDER_ID                                                AS ORDER_ID_NK,
        p.PAYMENT_SEQUENTIAL,
        TO_NUMBER(TO_CHAR(o.PURCHASE_TS::DATE, 'YYYYMMDD'))       AS PURCHASE_DATE_KEY,
        dc.CUSTOMER_KEY,
        p.PAYMENT_TYPE, p.PAYMENT_INSTALLMENTS,
        p.IS_CREDIT_CARD, p.IS_BOLETO, p.IS_VOUCHER, p.IS_DEBIT_CARD,
        p.PAYMENT_AMT
    FROM STAGING.STG_ORDER_PAYMENTS p
    JOIN STAGING.STG_ORDERS o
         ON o.ORDER_ID = p.ORDER_ID AND o._IS_DUPLICATE = FALSE AND o._IS_VALID = TRUE
    JOIN STAGING.STG_CUSTOMERS sc
         ON sc.CUSTOMER_ID = o.CUSTOMER_ID AND sc._IS_DUPLICATE = FALSE
    JOIN MARTS.DIM_CUSTOMERS dc
         ON dc.CUSTOMER_UNIQUE_ID = sc.CUSTOMER_UNIQUE_ID
        AND dc.SCD_START_DT  <= o.PURCHASE_TS::DATE
        AND (dc.SCD_END_DT IS NULL OR dc.SCD_END_DT > o.PURCHASE_TS::DATE)
    WHERE p._IS_DUPLICATE = FALSE AND p._IS_VALID = TRUE
) src
ON (tgt.ORDER_ID_NK = src.ORDER_ID_NK AND tgt.PAYMENT_SEQUENTIAL = src.PAYMENT_SEQUENTIAL)

WHEN MATCHED AND tgt.PAYMENT_AMT <> src.PAYMENT_AMT THEN UPDATE SET
    tgt.PAYMENT_AMT   = src.PAYMENT_AMT,
    tgt._LOADED_AT    = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    ORDER_ID_NK, PAYMENT_SEQUENTIAL,
    PURCHASE_DATE_KEY, CUSTOMER_KEY,
    PAYMENT_TYPE, PAYMENT_INSTALLMENTS,
    IS_CREDIT_CARD, IS_BOLETO, IS_VOUCHER, IS_DEBIT_CARD,
    PAYMENT_AMT, _IS_LATE_ARRIVING
) VALUES (
    src.ORDER_ID_NK, src.PAYMENT_SEQUENTIAL,
    src.PURCHASE_DATE_KEY, src.CUSTOMER_KEY,
    src.PAYMENT_TYPE, src.PAYMENT_INSTALLMENTS,
    src.IS_CREDIT_CARD, src.IS_BOLETO, src.IS_VOUCHER, src.IS_DEBIT_CARD,
    src.PAYMENT_AMT, TRUE
);


-- =============================================================================
-- SECTION 7 : VERIFICATION QUERIES
-- =============================================================================

-- ── Row counts ────────────────────────────────────────────────────────────────
SELECT 'DIM_DATE'          AS table_name, COUNT(*) AS rows FROM MARTS.DIM_DATE         UNION ALL
SELECT 'DIM_CUSTOMERS',                   COUNT(*)         FROM MARTS.DIM_CUSTOMERS    UNION ALL
SELECT 'DIM_PRODUCTS',                    COUNT(*)         FROM MARTS.DIM_PRODUCTS     UNION ALL
SELECT 'DIM_SELLERS',                     COUNT(*)         FROM MARTS.DIM_SELLERS      UNION ALL
SELECT 'DIM_GEOGRAPHY',                   COUNT(*)         FROM MARTS.DIM_GEOGRAPHY    UNION ALL
SELECT 'FACT_ORDER_ITEMS',                COUNT(*)         FROM MARTS.FACT_ORDER_ITEMS UNION ALL
SELECT 'FACT_PAYMENTS',                   COUNT(*)         FROM MARTS.FACT_PAYMENTS    UNION ALL
SELECT 'FACT_REVIEWS',                    COUNT(*)         FROM MARTS.FACT_REVIEWS
ORDER BY table_name;

-- ── SCD Type 2 integrity: no overlapping versions per customer ────────────────
SELECT CUSTOMER_UNIQUE_ID, COUNT(*) AS version_cnt,
       SUM(IFF(IS_CURRENT, 1, 0))   AS current_versions   -- must = 1
FROM MARTS.DIM_CUSTOMERS
GROUP BY CUSTOMER_UNIQUE_ID
HAVING current_versions <> 1 OR version_cnt > 10          -- flag anomalies
LIMIT 20;

-- ── Fact → Dim join integrity: no broken CUSTOMER_KEY FKs ────────────────────
SELECT COUNT(*) AS orphan_fact_rows
FROM MARTS.FACT_ORDER_ITEMS f
LEFT JOIN MARTS.DIM_CUSTOMERS dc ON dc.CUSTOMER_KEY = f.CUSTOMER_KEY
WHERE dc.CUSTOMER_KEY IS NULL;
-- Expected: 0

-- ── Late-arriving rows flagged ────────────────────────────────────────────────
SELECT 'FACT_ORDER_ITEMS' AS fact, COUNT(*) AS late_rows
FROM MARTS.FACT_ORDER_ITEMS WHERE _IS_LATE_ARRIVING = TRUE
UNION ALL
SELECT 'FACT_PAYMENTS',           COUNT(*)
FROM MARTS.FACT_PAYMENTS    WHERE _IS_LATE_ARRIVING = TRUE;

-- ── Sample star-schema query: monthly revenue by state ────────────────────────
SELECT
    d.YEAR,
    d.MONTH_NAME,
    dc.STATE,
    COUNT(DISTINCT f.ORDER_ID_NK)          AS order_cnt,
    ROUND(SUM(f.TOTAL_ITEM_AMT), 2)        AS total_revenue,
    ROUND(AVG(f.TOTAL_ITEM_AMT), 2)        AS avg_order_item_value
FROM MARTS.FACT_ORDER_ITEMS f
JOIN MARTS.DIM_DATE      d  ON d.DATE_KEY     = f.PURCHASE_DATE_KEY
JOIN MARTS.DIM_CUSTOMERS dc ON dc.CUSTOMER_KEY = f.CUSTOMER_KEY
WHERE f.IS_CANCELED = FALSE
GROUP BY d.YEAR, d.MONTH_NUM, d.MONTH_NAME, dc.STATE
ORDER BY d.YEAR, d.MONTH_NUM, total_revenue DESC;
