-- =============================================================================
-- ShopSphere Data Warehouse – Dynamic Tables + Streams & Tasks Hybrid Pipeline
-- File   : 06_dynamic_tables_and_pipeline.sql
--
-- Architecture:
--
--   RAW.*  ──[Dynamic Tables]──▶  STAGING.DT_*  ──[Streams]──▶  Task DAG  ──▶  MARTS.*
--                (initial transforms)             (CDC delta)   (MERGE+SCD2)
--
--   SECTION 1  : Why Dynamic Tables vs Streams+Tasks per step
--   SECTION 2  : Dynamic Table definitions  (one per RAW source)
--                  DT_CUSTOMERS   – INITCAP, UPPER, LOWER, QUALIFY dedup
--                  DT_GEOLOCATION – centroid per ZIP (AVG lat/lng + mode city)
--                  DT_ORDERS      – UPPER status, DATE cast, derived flags
--                  DT_ORDER_ITEMS – ROUND + TOTAL_ITEM_AMT, validity guard
--                  DT_ORDER_PAYMENTS – UPPER type, ROUND, boolean flags
--                  DT_ORDER_REVIEWS  – NULLIF blanks, DATE cast, sentiment flags
--                  DT_PRODUCTS    – LEFT JOIN LKP for English category name
--                  DT_SELLERS     – INITCAP city, UPPER state
--   SECTION 3  : Streams on DT_* tables  (not on RAW)
--   SECTION 4  : Task DAG  (same topology as 05, but reading DT_* streams)
--                  TASK_DT_MASTER_TRIGGER         (root, every 5 min)
--                  TASK_DT_MERGE_DIM_CUSTOMERS    (SCD2, AFTER root)
--                  TASK_DT_MERGE_DIM_PRODUCTS     (SCD2, AFTER root)
--                  TASK_DT_MERGE_DIM_SELLERS      (Type1, AFTER root)
--                  TASK_DT_MERGE_FACT_ITEMS       (AFTER all 3 dims)
--                  TASK_DT_MERGE_FACT_PAYMENTS    (AFTER all 3 dims)
--                  TASK_DT_MERGE_FACT_REVIEWS     (AFTER all 3 dims)
--                  TASK_DT_AUDIT_LOG              (leaf, AFTER all 3 facts)
--   SECTION 5  : SP_AUDIT_PIPELINE – multi-step audit stored procedure
--   SECTION 6  : Start / monitor / control queries
--
-- Note   : This file is STANDALONE — it does not depend on 02_staging_standardize.sql.
--          Both files write to STAGING schema but use different table names (DT_* vs STG_*).
-- Prerequisites : 01_setup_and_load.sql must have populated the RAW tables.
-- Run in        : Snowflake worksheet using SYSADMIN + WH_M_ETL
-- =============================================================================

USE DATABASE SHOPSPHERE_DW;
USE ROLE    SYSADMIN;
USE WAREHOUSE WH_M_ETL;


-- =============================================================================
-- SECTION 1 : DESIGN DECISION — WHAT GOES WHERE
-- =============================================================================
--
--  Dynamic Tables handle (pure SELECT, auto-refresh, zero pipeline code):
--    ✓ TRIM / INITCAP / UPPER / LOWER text normalisation
--    ✓ ROUND / COALESCE for monetary values
--    ✓ Derived boolean flags  (IS_DELIVERED, IS_CANCELED, IS_POSITIVE …)
--    ✓ DATE cast for timestamp columns whose time is always 00:00:00
--    ✓ QUALIFY ROW_NUMBER() deduplication
--    ✓ LEFT JOIN enrichment  (LKP_CATEGORY_NAMES → English category)
--    ✓ AVG(lat/lng) + mode-city aggregation for geolocation
--
--  Streams + Tasks handle (procedural, stateful, needs MERGE):
--    ✓ SCD Type 2  — expire old version, insert new version
--    ✓ Surrogate key resolution when loading fact tables
--    ✓ Conditional update logic on facts  (status changed → UPDATE)
--    ✓ Multi-step audit logging  (counts, DQ checks, pipeline run record)
--
-- =============================================================================


-- =============================================================================
-- SECTION 2 : DYNAMIC TABLE DEFINITIONS
--
-- TARGET_LAG = '1 minute'   → Snowflake refreshes within 1 min of RAW change.
-- Use 'DOWNSTREAM' in production to let Snowflake batch refreshes automatically.
-- All tables use QUALIFY ROW_NUMBER() ... = 1 for inline dedup (cleaner than CTE).
-- =============================================================================

-- ── DT_CUSTOMERS ──────────────────────────────────────────────────────────────
-- Grain  : one row per CUSTOMER_ID  (order-scoped identity, deduped)
-- Source : RAW.RAW_CUSTOMERS
-- Transforms applied:
--   • LOWER(TRIM())   on hex IDs
--   • INITCAP(TRIM()) on CITY
--   • UPPER(TRIM())   on STATE
--   • QUALIFY ROW_NUMBER() keeps latest loaded row per CUSTOMER_ID
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_CUSTOMERS
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_M_ETL
    COMMENT    = 'Dynamic Table: cleaned & deduped customers — auto-refreshed from RAW_CUSTOMERS'
AS
SELECT
    LOWER(TRIM(CUSTOMER_ID))        AS CUSTOMER_ID,
    LOWER(TRIM(CUSTOMER_UNIQUE_ID)) AS CUSTOMER_UNIQUE_ID,
    TRIM(ZIP_CODE_PREFIX)           AS ZIP_CODE_PREFIX,
    INITCAP(TRIM(CITY))             AS CITY,
    UPPER(TRIM(STATE))              AS STATE,
    _LOADED_AT
FROM RAW.RAW_CUSTOMERS
WHERE CUSTOMER_ID        IS NOT NULL
  AND CUSTOMER_UNIQUE_ID IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LOWER(TRIM(CUSTOMER_ID))
    ORDER BY _LOADED_AT DESC
) = 1;


-- ── DT_GEOLOCATION ────────────────────────────────────────────────────────────
-- Grain  : one row per ZIP code prefix  (aggregated centroid)
-- Source : RAW.RAW_GEOLOCATION  (1M rows → ~19K unique ZIPs)
-- Transforms applied:
--   • AVG(lat/lng) per ZIP
--   • Most-frequent city name selected via QUALIFY ROW_NUMBER()
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_GEOLOCATION
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_M_ETL
    COMMENT    = 'Dynamic Table: one centroid per ZIP prefix — auto-refreshed from RAW_GEOLOCATION'
AS
WITH city_mode AS (
    SELECT
        TRIM(ZIP_CODE_PREFIX)  AS ZIP_CODE_PREFIX,
        INITCAP(TRIM(CITY))    AS CITY,
        UPPER(TRIM(STATE))     AS STATE,
        COUNT(*)               AS cnt
    FROM RAW.RAW_GEOLOCATION
    WHERE ZIP_CODE_PREFIX IS NOT NULL
    GROUP BY TRIM(ZIP_CODE_PREFIX), INITCAP(TRIM(CITY)), UPPER(TRIM(STATE))
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRIM(ZIP_CODE_PREFIX)
        ORDER BY COUNT(*) DESC
    ) = 1
),
coords AS (
    SELECT
        TRIM(ZIP_CODE_PREFIX) AS ZIP_CODE_PREFIX,
        AVG(LAT)              AS AVG_LAT,
        AVG(LNG)              AS AVG_LNG
    FROM RAW.RAW_GEOLOCATION
    WHERE ZIP_CODE_PREFIX IS NOT NULL
    GROUP BY TRIM(ZIP_CODE_PREFIX)
)
SELECT
    c.ZIP_CODE_PREFIX,
    m.CITY,
    m.STATE,
    c.AVG_LAT,
    c.AVG_LNG
FROM coords      c
JOIN city_mode   m ON m.ZIP_CODE_PREFIX = c.ZIP_CODE_PREFIX;


-- ── DT_ORDERS ─────────────────────────────────────────────────────────────────
-- Grain  : one row per order  (deduped, latest version)
-- Source : RAW.RAW_ORDERS
-- Transforms applied:
--   • UPPER(TRIM()) on ORDER_STATUS
--   • CAST(ESTIMATED_DELIVERY_DT AS DATE)  — source always has 00:00:00 time
--   • Derived flags: IS_DELIVERED, IS_CANCELED
--   • Derived metrics: IS_LATE, DELIVERY_DELAY_DAYS
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_ORDERS
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_M_ETL
    COMMENT    = 'Dynamic Table: cleaned orders with status flags — auto-refreshed from RAW_ORDERS'
AS
SELECT
    LOWER(TRIM(ORDER_ID))      AS ORDER_ID,
    LOWER(TRIM(CUSTOMER_ID))   AS CUSTOMER_ID,
    UPPER(TRIM(ORDER_STATUS))  AS ORDER_STATUS,
    PURCHASE_TS,
    APPROVED_AT,
    DELIVERED_CARRIER_AT,
    DELIVERED_CUSTOMER_AT,
    CAST(ESTIMATED_DELIVERY_DT AS DATE)              AS ESTIMATED_DELIVERY_DT,
    -- Status flags
    (UPPER(TRIM(ORDER_STATUS)) = 'DELIVERED')        AS IS_DELIVERED,
    (UPPER(TRIM(ORDER_STATUS)) = 'CANCELED')         AS IS_CANCELED,
    -- Delivery performance (NULL when timestamps missing)
    CASE
        WHEN DELIVERED_CUSTOMER_AT IS NOT NULL
         AND ESTIMATED_DELIVERY_DT IS NOT NULL
        THEN DATEDIFF('day', ESTIMATED_DELIVERY_DT, DELIVERED_CUSTOMER_AT) > 0
        ELSE NULL
    END                                              AS IS_LATE,
    CASE
        WHEN DELIVERED_CUSTOMER_AT IS NOT NULL
         AND ESTIMATED_DELIVERY_DT IS NOT NULL
        THEN DATEDIFF('day', ESTIMATED_DELIVERY_DT, DELIVERED_CUSTOMER_AT)
        ELSE NULL
    END                                              AS DELIVERY_DELAY_DAYS,
    _LOADED_AT
FROM RAW.RAW_ORDERS
WHERE ORDER_ID    IS NOT NULL
  AND PURCHASE_TS IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LOWER(TRIM(ORDER_ID))
    ORDER BY _LOADED_AT DESC
) = 1;


-- ── DT_ORDER_ITEMS ────────────────────────────────────────────────────────────
-- Grain  : one row per order line item  (deduped)
-- Source : RAW.RAW_ORDER_ITEMS
-- Transforms applied:
--   • ROUND(COALESCE(..., 0), 2) on monetary columns
--   • TOTAL_ITEM_AMT = PRICE_AMT + FREIGHT_AMT  (derived)
--   • Validity guard: exclude zero-price items
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_ORDER_ITEMS
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_M_ETL
    COMMENT    = 'Dynamic Table: cleaned order items with total — auto-refreshed from RAW_ORDER_ITEMS'
AS
SELECT
    LOWER(TRIM(ORDER_ID))                                              AS ORDER_ID,
    ORDER_ITEM_ID,
    LOWER(TRIM(PRODUCT_ID))                                            AS PRODUCT_ID,
    LOWER(TRIM(SELLER_ID))                                             AS SELLER_ID,
    SHIPPING_LIMIT_AT,
    ROUND(COALESCE(PRICE_AMT,   0), 2)                                 AS PRICE_AMT,
    ROUND(COALESCE(FREIGHT_AMT, 0), 2)                                 AS FREIGHT_AMT,
    ROUND(COALESCE(PRICE_AMT,0) + COALESCE(FREIGHT_AMT,0), 2)         AS TOTAL_ITEM_AMT,
    _LOADED_AT
FROM RAW.RAW_ORDER_ITEMS
WHERE ORDER_ID      IS NOT NULL
  AND ORDER_ITEM_ID IS NOT NULL
  AND COALESCE(PRICE_AMT, 0) > 0      -- validity: no zero-price items
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LOWER(TRIM(ORDER_ID)), ORDER_ITEM_ID
    ORDER BY _LOADED_AT DESC
) = 1;


-- ── DT_ORDER_PAYMENTS ─────────────────────────────────────────────────────────
-- Grain  : one row per payment entry  (ORDER_ID + PAYMENT_SEQUENTIAL, deduped)
-- Source : RAW.RAW_ORDER_PAYMENTS
-- Transforms applied:
--   • UPPER(TRIM()) on PAYMENT_TYPE
--   • ROUND + COALESCE on PAYMENT_AMT
--   • Boolean flags: IS_CREDIT_CARD, IS_BOLETO, IS_VOUCHER, IS_DEBIT_CARD
--   • Validity guard: exclude zero-payment rows
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_ORDER_PAYMENTS
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_M_ETL
    COMMENT    = 'Dynamic Table: cleaned payments with type flags — auto-refreshed from RAW_ORDER_PAYMENTS'
AS
SELECT
    LOWER(TRIM(ORDER_ID))                        AS ORDER_ID,
    COALESCE(PAYMENT_SEQUENTIAL, 1)              AS PAYMENT_SEQUENTIAL,
    UPPER(TRIM(PAYMENT_TYPE))                    AS PAYMENT_TYPE,
    COALESCE(PAYMENT_INSTALLMENTS, 1)            AS PAYMENT_INSTALLMENTS,
    ROUND(COALESCE(PAYMENT_AMT, 0), 2)           AS PAYMENT_AMT,
    (UPPER(TRIM(PAYMENT_TYPE)) = 'CREDIT_CARD')  AS IS_CREDIT_CARD,
    (UPPER(TRIM(PAYMENT_TYPE)) = 'BOLETO')       AS IS_BOLETO,
    (UPPER(TRIM(PAYMENT_TYPE)) = 'VOUCHER')      AS IS_VOUCHER,
    (UPPER(TRIM(PAYMENT_TYPE)) = 'DEBIT_CARD')   AS IS_DEBIT_CARD,
    _LOADED_AT
FROM RAW.RAW_ORDER_PAYMENTS
WHERE ORDER_ID IS NOT NULL
  AND COALESCE(PAYMENT_AMT, 0) > 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LOWER(TRIM(ORDER_ID)), COALESCE(PAYMENT_SEQUENTIAL, 1)
    ORDER BY _LOADED_AT DESC
) = 1;


-- ── DT_ORDER_REVIEWS ──────────────────────────────────────────────────────────
-- Grain  : one row per ORDER_ID  (latest review per order)
-- Source : RAW.RAW_ORDER_REVIEWS
-- Transforms applied:
--   • NULLIF(TRIM(...), '') converts empty strings to NULL for text columns
--   • CAST(REVIEW_CREATED_DT AS DATE)
--   • Sentiment flags: IS_POSITIVE (≥4), IS_NEUTRAL (=3), IS_NEGATIVE (≤2)
--   • HAS_COMMENT derived from REVIEW_MESSAGE
--   • QUALIFY keeps latest REVIEW_ANSWERED_AT per order (dedup)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_ORDER_REVIEWS
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_M_ETL
    COMMENT    = 'Dynamic Table: cleaned reviews, one per order (latest) — auto-refreshed from RAW_ORDER_REVIEWS'
AS
SELECT
    LOWER(TRIM(REVIEW_ID))                               AS REVIEW_ID,
    LOWER(TRIM(ORDER_ID))                                AS ORDER_ID,
    REVIEW_SCORE,
    NULLIF(TRIM(REVIEW_TITLE),   '')                     AS REVIEW_TITLE,
    NULLIF(TRIM(REVIEW_MESSAGE), '')                     AS REVIEW_MESSAGE,
    CAST(REVIEW_CREATED_DT AS DATE)                      AS REVIEW_CREATED_DT,
    REVIEW_ANSWERED_AT,
    (REVIEW_SCORE >= 4)                                  AS IS_POSITIVE,
    (REVIEW_SCORE  = 3)                                  AS IS_NEUTRAL,
    (REVIEW_SCORE <= 2)                                  AS IS_NEGATIVE,
    (NULLIF(TRIM(REVIEW_MESSAGE), '') IS NOT NULL)       AS HAS_COMMENT,
    _LOADED_AT
FROM RAW.RAW_ORDER_REVIEWS
WHERE REVIEW_ID    IS NOT NULL
  AND ORDER_ID     IS NOT NULL
  AND REVIEW_SCORE BETWEEN 1 AND 5
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LOWER(TRIM(ORDER_ID))
    ORDER BY REVIEW_ANSWERED_AT DESC NULLS LAST
) = 1;


-- ── DT_PRODUCTS ───────────────────────────────────────────────────────────────
-- Grain  : one row per PRODUCT_ID  (deduped, latest version)
-- Source : RAW.RAW_PRODUCTS  LEFT JOIN  COMMON.LKP_CATEGORY_NAMES
-- Transforms applied:
--   • LOWER(TRIM()) on CATEGORY_NAME_PT
--   • LEFT JOIN to LKP_CATEGORY_NAMES for English translation
--   • ROUND on all dimension measurements
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_PRODUCTS
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_M_ETL
    COMMENT    = 'Dynamic Table: products enriched with English category — auto-refreshed from RAW_PRODUCTS'
AS
SELECT
    LOWER(TRIM(p.PRODUCT_ID))        AS PRODUCT_ID,
    LOWER(TRIM(p.CATEGORY_NAME_PT))  AS CATEGORY_NAME_PT,
    lkp.CATEGORY_NAME_EN,            -- NULL when no translation exists
    p.PHOTO_CNT,
    ROUND(p.WEIGHT_G,  2)            AS WEIGHT_G,
    ROUND(p.LENGTH_CM, 2)            AS LENGTH_CM,
    ROUND(p.HEIGHT_CM, 2)            AS HEIGHT_CM,
    ROUND(p.WIDTH_CM,  2)            AS WIDTH_CM,
    p._LOADED_AT
FROM RAW.RAW_PRODUCTS p
LEFT JOIN COMMON.LKP_CATEGORY_NAMES lkp
       ON LOWER(TRIM(p.CATEGORY_NAME_PT)) = LOWER(TRIM(lkp.CATEGORY_NAME_PT))
WHERE p.PRODUCT_ID IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LOWER(TRIM(p.PRODUCT_ID))
    ORDER BY p._LOADED_AT DESC
) = 1;


-- ── DT_SELLERS ────────────────────────────────────────────────────────────────
-- Grain  : one row per SELLER_ID  (deduped)
-- Source : RAW.RAW_SELLERS
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_SELLERS
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_M_ETL
    COMMENT    = 'Dynamic Table: cleaned sellers — auto-refreshed from RAW_SELLERS'
AS
SELECT
    LOWER(TRIM(SELLER_ID))  AS SELLER_ID,
    TRIM(ZIP_CODE_PREFIX)   AS ZIP_CODE_PREFIX,
    INITCAP(TRIM(CITY))     AS CITY,
    UPPER(TRIM(STATE))      AS STATE,
    _LOADED_AT
FROM RAW.RAW_SELLERS
WHERE SELLER_ID IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LOWER(TRIM(SELLER_ID))
    ORDER BY _LOADED_AT DESC
) = 1;


-- Verify all Dynamic Tables:
SHOW DYNAMIC TABLES IN SCHEMA STAGING;


-- =============================================================================
-- SECTION 3 : STREAMS ON DT_* TABLES
--
-- Streams sit on DT_* (not RAW) so Tasks always consume clean, transformed data.
--
-- Stream type decisions:
--   DT_CUSTOMERS / DT_PRODUCTS / DT_SELLERS → DEFAULT
--     Need INSERT + UPDATE CDC events for SCD2 and Type-1 overwrite logic.
--   DT_ORDER_ITEMS / DT_ORDER_PAYMENTS / DT_ORDER_REVIEWS → APPEND_ONLY
--     Facts only grow; QUALIFY in the DT already eliminates duplicates.
--     APPEND_ONLY is cheaper — tracks no update/delete metadata.
-- =============================================================================

-- Dimension streams (DEFAULT — capture all DML including UPDATEs)
CREATE OR REPLACE STREAM STAGING.STRM_DT_CUSTOMERS
    ON TABLE STAGING.DT_CUSTOMERS
    COMMENT = 'DEFAULT stream on DT_CUSTOMERS → SCD2 merge into DIM_CUSTOMERS';

CREATE OR REPLACE STREAM STAGING.STRM_DT_PRODUCTS
    ON TABLE STAGING.DT_PRODUCTS
    COMMENT = 'DEFAULT stream on DT_PRODUCTS → SCD2 merge into DIM_PRODUCTS';

CREATE OR REPLACE STREAM STAGING.STRM_DT_SELLERS
    ON TABLE STAGING.DT_SELLERS
    COMMENT = 'DEFAULT stream on DT_SELLERS → Type-1 merge into DIM_SELLERS';

-- Fact streams (APPEND_ONLY — new rows only)
CREATE OR REPLACE STREAM STAGING.STRM_DT_ORDER_ITEMS
    ON TABLE STAGING.DT_ORDER_ITEMS
    APPEND_ONLY = TRUE
    COMMENT = 'APPEND_ONLY stream on DT_ORDER_ITEMS → MERGE into FACT_ORDER_ITEMS';

CREATE OR REPLACE STREAM STAGING.STRM_DT_ORDER_PAYMENTS
    ON TABLE STAGING.DT_ORDER_PAYMENTS
    APPEND_ONLY = TRUE
    COMMENT = 'APPEND_ONLY stream on DT_ORDER_PAYMENTS → MERGE into FACT_PAYMENTS';

CREATE OR REPLACE STREAM STAGING.STRM_DT_ORDER_REVIEWS
    ON TABLE STAGING.DT_ORDER_REVIEWS
    APPEND_ONLY = TRUE
    COMMENT = 'APPEND_ONLY stream on DT_ORDER_REVIEWS → MERGE into FACT_REVIEWS';

SHOW STREAMS IN SCHEMA STAGING;


-- =============================================================================
-- SECTION 4 : TASK DAG
--
--   TASK_DT_MASTER_TRIGGER          ← root, every 5 min
--           │
--   ┌───────┼───────────┐
--   │       │           │
-- DIM_     DIM_        DIM_
-- CUST     PROD        SELL
--   │       │           │
--   └───────┼───────────┘
--           │  (all 3 dims must complete before any fact)
--   ┌───────┼───────────┐
--   │       │           │
-- FACT_   FACT_       FACT_
-- ITEMS   PMTS        REVWS
--   │       │           │
--   └───────┼───────────┘
--           │
--   TASK_DT_AUDIT_LOG  ← leaf, calls SP_AUDIT_PIPELINE
-- =============================================================================

-- ── ROOT TASK ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE TASK AUDIT.TASK_DT_MASTER_TRIGGER
    WAREHOUSE = WH_M_ETL
    SCHEDULE  = '5 MINUTE'
    COMMENT   = 'Root scheduler — fires every 5 min, children guard with SYSTEM$STREAM_HAS_DATA'
AS
    SELECT CURRENT_TIMESTAMP() AS trigger_ts;


-- ── DIM_CUSTOMERS — SCD Type 2 ────────────────────────────────────────────────
CREATE OR REPLACE TASK AUDIT.TASK_DT_MERGE_DIM_CUSTOMERS
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_DT_MASTER_TRIGGER
    COMMENT   = 'SCD Type 2 merge from STRM_DT_CUSTOMERS into MARTS.DIM_CUSTOMERS'
AS
$$
BEGIN
    IF (NOT SYSTEM$STREAM_HAS_DATA('STAGING.STRM_DT_CUSTOMERS')) THEN
        RETURN 'STRM_DT_CUSTOMERS empty — skipped';
    END IF;

    -- Step 1: expire rows where CITY / STATE / ZIP changed
    UPDATE MARTS.DIM_CUSTOMERS dim
    SET    SCD_END_DT  = CURRENT_DATE() - 1,
           IS_CURRENT  = FALSE,
           _UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE  dim.IS_CURRENT = TRUE
      AND  EXISTS (
               SELECT 1
               FROM   STAGING.STRM_DT_CUSTOMERS s
               WHERE  s.METADATA$ACTION    = 'INSERT'
                 AND  s.METADATA$ISUPDATE  = TRUE
                 AND  s.CUSTOMER_UNIQUE_ID = dim.CUSTOMER_UNIQUE_ID
                 AND  (
                           COALESCE(s.CITY,            '') <> COALESCE(dim.CITY,            '')
                        OR COALESCE(s.STATE,           '') <> COALESCE(dim.STATE,           '')
                        OR COALESCE(s.ZIP_CODE_PREFIX, '') <> COALESCE(dim.ZIP_CODE_PREFIX, '')
                      )
           );

    -- Step 2: insert new version for changed + brand-new customers
    INSERT INTO MARTS.DIM_CUSTOMERS (
        CUSTOMER_UNIQUE_ID, CUSTOMER_ID_NK,
        ZIP_CODE_PREFIX, CITY, STATE,
        SCD_START_DT, SCD_END_DT, IS_CURRENT
    )
    SELECT
        s.CUSTOMER_UNIQUE_ID,
        s.CUSTOMER_ID AS CUSTOMER_ID_NK,
        s.ZIP_CODE_PREFIX, s.CITY, s.STATE,
        CURRENT_DATE(), NULL, TRUE
    FROM   STAGING.STRM_DT_CUSTOMERS s
    WHERE  s.METADATA$ACTION = 'INSERT'
      AND  NOT EXISTS (
               SELECT 1 FROM MARTS.DIM_CUSTOMERS d
               WHERE  d.CUSTOMER_UNIQUE_ID = s.CUSTOMER_UNIQUE_ID
                 AND  d.IS_CURRENT = TRUE
           );

    RETURN 'DIM_CUSTOMERS SCD2 merge complete';
END;
$$;


-- ── DIM_PRODUCTS — SCD Type 2 ─────────────────────────────────────────────────
CREATE OR REPLACE TASK AUDIT.TASK_DT_MERGE_DIM_PRODUCTS
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_DT_MASTER_TRIGGER
    COMMENT   = 'SCD Type 2 merge from STRM_DT_PRODUCTS into MARTS.DIM_PRODUCTS'
AS
$$
BEGIN
    IF (NOT SYSTEM$STREAM_HAS_DATA('STAGING.STRM_DT_PRODUCTS')) THEN
        RETURN 'STRM_DT_PRODUCTS empty — skipped';
    END IF;

    -- Expire old version when English category name changed
    UPDATE MARTS.DIM_PRODUCTS dim
    SET    SCD_END_DT  = CURRENT_DATE() - 1,
           IS_CURRENT  = FALSE,
           _UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE  dim.IS_CURRENT = TRUE
      AND  EXISTS (
               SELECT 1
               FROM   STAGING.STRM_DT_PRODUCTS s
               WHERE  s.METADATA$ACTION   = 'INSERT'
                 AND  s.METADATA$ISUPDATE = TRUE
                 AND  s.PRODUCT_ID        = dim.PRODUCT_ID_NK
                 AND  COALESCE(s.CATEGORY_NAME_EN, '') <> COALESCE(dim.CATEGORY_NAME_EN, '')
           );

    -- Insert new version
    INSERT INTO MARTS.DIM_PRODUCTS (
        PRODUCT_ID_NK, CATEGORY_NAME_PT, CATEGORY_NAME_EN,
        PHOTO_CNT, WEIGHT_G, LENGTH_CM, HEIGHT_CM, WIDTH_CM,
        SCD_START_DT, SCD_END_DT, IS_CURRENT
    )
    SELECT
        s.PRODUCT_ID, s.CATEGORY_NAME_PT, s.CATEGORY_NAME_EN,
        s.PHOTO_CNT, s.WEIGHT_G, s.LENGTH_CM, s.HEIGHT_CM, s.WIDTH_CM,
        CURRENT_DATE(), NULL, TRUE
    FROM   STAGING.STRM_DT_PRODUCTS s
    WHERE  s.METADATA$ACTION = 'INSERT'
      AND  NOT EXISTS (
               SELECT 1 FROM MARTS.DIM_PRODUCTS d
               WHERE  d.PRODUCT_ID_NK = s.PRODUCT_ID AND d.IS_CURRENT = TRUE
           );

    RETURN 'DIM_PRODUCTS SCD2 merge complete';
END;
$$;


-- ── DIM_SELLERS — Type 1 overwrite ────────────────────────────────────────────
CREATE OR REPLACE TASK AUDIT.TASK_DT_MERGE_DIM_SELLERS
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_DT_MASTER_TRIGGER
    COMMENT   = 'Type-1 MERGE from STRM_DT_SELLERS into MARTS.DIM_SELLERS'
AS
$$
BEGIN
    IF (NOT SYSTEM$STREAM_HAS_DATA('STAGING.STRM_DT_SELLERS')) THEN
        RETURN 'STRM_DT_SELLERS empty — skipped';
    END IF;

    MERGE INTO MARTS.DIM_SELLERS tgt
    USING (
        SELECT SELLER_ID, ZIP_CODE_PREFIX, CITY, STATE
        FROM   STAGING.STRM_DT_SELLERS
        WHERE  METADATA$ACTION = 'INSERT'
    ) src ON tgt.SELLER_ID_NK = src.SELLER_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.ZIP_CODE_PREFIX = src.ZIP_CODE_PREFIX,
        tgt.CITY            = src.CITY,
        tgt.STATE           = src.STATE,
        tgt._UPDATED_AT     = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (SELLER_ID_NK, ZIP_CODE_PREFIX, CITY, STATE)
    VALUES (src.SELLER_ID, src.ZIP_CODE_PREFIX, src.CITY, src.STATE);

    RETURN 'DIM_SELLERS Type-1 merge complete';
END;
$$;


-- ── FACT_ORDER_ITEMS — MERGE ──────────────────────────────────────────────────
-- Depends on all 3 dim tasks completing — surrogate keys must exist first.
CREATE OR REPLACE TASK AUDIT.TASK_DT_MERGE_FACT_ITEMS
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_DT_MERGE_DIM_CUSTOMERS,
              AUDIT.TASK_DT_MERGE_DIM_PRODUCTS,
              AUDIT.TASK_DT_MERGE_DIM_SELLERS
    COMMENT   = 'MERGE new order items from STRM_DT_ORDER_ITEMS into MARTS.FACT_ORDER_ITEMS'
AS
$$
BEGIN
    IF (NOT SYSTEM$STREAM_HAS_DATA('STAGING.STRM_DT_ORDER_ITEMS')) THEN
        RETURN 'STRM_DT_ORDER_ITEMS empty — skipped';
    END IF;

    MERGE INTO MARTS.FACT_ORDER_ITEMS tgt
    USING (
        -- Join DT_* tables — already clean, no further casting needed
        SELECT
            i.ORDER_ID,
            i.ORDER_ITEM_ID,
            TO_NUMBER(TO_CHAR(o.PURCHASE_TS::DATE, 'YYYYMMDD'))  AS PURCHASE_DATE_KEY,
            dc.CUSTOMER_KEY,
            dp.PRODUCT_KEY,
            ds.SELLER_KEY,
            dg.GEOGRAPHY_KEY,
            o.ORDER_STATUS, o.IS_DELIVERED, o.IS_CANCELED,
            o.IS_LATE, o.DELIVERY_DELAY_DAYS, o.PURCHASE_TS,
            i.PRICE_AMT, i.FREIGHT_AMT, i.TOTAL_ITEM_AMT
        FROM  STAGING.STRM_DT_ORDER_ITEMS i          -- stream as source
        JOIN  STAGING.DT_ORDERS      o  ON o.ORDER_ID    = i.ORDER_ID
        JOIN  STAGING.DT_CUSTOMERS   sc ON sc.CUSTOMER_ID = o.CUSTOMER_ID
        JOIN  MARTS.DIM_CUSTOMERS    dc
              ON  dc.CUSTOMER_UNIQUE_ID = sc.CUSTOMER_UNIQUE_ID
              AND dc.SCD_START_DT  <= o.PURCHASE_TS::DATE
              AND (dc.SCD_END_DT IS NULL OR dc.SCD_END_DT > o.PURCHASE_TS::DATE)
        JOIN  MARTS.DIM_PRODUCTS     dp ON dp.PRODUCT_ID_NK = i.PRODUCT_ID AND dp.IS_CURRENT = TRUE
        JOIN  MARTS.DIM_SELLERS      ds ON ds.SELLER_ID_NK  = i.SELLER_ID
        LEFT JOIN MARTS.DIM_GEOGRAPHY dg ON dg.ZIP_CODE_PREFIX = sc.ZIP_CODE_PREFIX
    ) src
    ON (tgt.ORDER_ID_NK = src.ORDER_ID AND tgt.ORDER_ITEM_ID = src.ORDER_ITEM_ID)

    -- Update if order status progressed (e.g. shipped → delivered) or price corrected
    WHEN MATCHED AND (
        tgt.ORDER_STATUS <> src.ORDER_STATUS OR
        tgt.PRICE_AMT    <> src.PRICE_AMT
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
        PURCHASE_TS, PRICE_AMT, FREIGHT_AMT, TOTAL_ITEM_AMT, _IS_LATE_ARRIVING
    ) VALUES (
        src.ORDER_ID, src.ORDER_ITEM_ID,
        src.PURCHASE_DATE_KEY, src.CUSTOMER_KEY, src.PRODUCT_KEY,
        src.SELLER_KEY, src.GEOGRAPHY_KEY,
        src.ORDER_STATUS, src.IS_DELIVERED, src.IS_CANCELED,
        src.IS_LATE, src.DELIVERY_DELAY_DAYS,
        src.PURCHASE_TS, src.PRICE_AMT, src.FREIGHT_AMT, src.TOTAL_ITEM_AMT,
        FALSE
    );

    RETURN 'FACT_ORDER_ITEMS merge complete';
END;
$$;


-- ── FACT_PAYMENTS — MERGE ─────────────────────────────────────────────────────
CREATE OR REPLACE TASK AUDIT.TASK_DT_MERGE_FACT_PAYMENTS
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_DT_MERGE_DIM_CUSTOMERS,
              AUDIT.TASK_DT_MERGE_DIM_PRODUCTS,
              AUDIT.TASK_DT_MERGE_DIM_SELLERS
    COMMENT   = 'MERGE new payments from STRM_DT_ORDER_PAYMENTS into MARTS.FACT_PAYMENTS'
AS
$$
BEGIN
    IF (NOT SYSTEM$STREAM_HAS_DATA('STAGING.STRM_DT_ORDER_PAYMENTS')) THEN
        RETURN 'STRM_DT_ORDER_PAYMENTS empty — skipped';
    END IF;

    MERGE INTO MARTS.FACT_PAYMENTS tgt
    USING (
        SELECT
            p.ORDER_ID, p.PAYMENT_SEQUENTIAL,
            TO_NUMBER(TO_CHAR(o.PURCHASE_TS::DATE, 'YYYYMMDD'))  AS PURCHASE_DATE_KEY,
            dc.CUSTOMER_KEY,
            p.PAYMENT_TYPE, p.PAYMENT_INSTALLMENTS,
            p.IS_CREDIT_CARD, p.IS_BOLETO, p.IS_VOUCHER, p.IS_DEBIT_CARD,
            p.PAYMENT_AMT
        FROM  STAGING.STRM_DT_ORDER_PAYMENTS p
        JOIN  STAGING.DT_ORDERS    o  ON o.ORDER_ID    = p.ORDER_ID
        JOIN  STAGING.DT_CUSTOMERS sc ON sc.CUSTOMER_ID = o.CUSTOMER_ID
        JOIN  MARTS.DIM_CUSTOMERS  dc
              ON  dc.CUSTOMER_UNIQUE_ID = sc.CUSTOMER_UNIQUE_ID
              AND dc.SCD_START_DT  <= o.PURCHASE_TS::DATE
              AND (dc.SCD_END_DT IS NULL OR dc.SCD_END_DT > o.PURCHASE_TS::DATE)
    ) src
    ON (tgt.ORDER_ID_NK = src.ORDER_ID AND tgt.PAYMENT_SEQUENTIAL = src.PAYMENT_SEQUENTIAL)
    WHEN MATCHED AND tgt.PAYMENT_AMT <> src.PAYMENT_AMT THEN UPDATE SET
        tgt.PAYMENT_AMT = src.PAYMENT_AMT,
        tgt._LOADED_AT  = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        ORDER_ID_NK, PAYMENT_SEQUENTIAL, PURCHASE_DATE_KEY, CUSTOMER_KEY,
        PAYMENT_TYPE, PAYMENT_INSTALLMENTS,
        IS_CREDIT_CARD, IS_BOLETO, IS_VOUCHER, IS_DEBIT_CARD,
        PAYMENT_AMT, _IS_LATE_ARRIVING
    ) VALUES (
        src.ORDER_ID, src.PAYMENT_SEQUENTIAL,
        src.PURCHASE_DATE_KEY, src.CUSTOMER_KEY,
        src.PAYMENT_TYPE, src.PAYMENT_INSTALLMENTS,
        src.IS_CREDIT_CARD, src.IS_BOLETO, src.IS_VOUCHER, src.IS_DEBIT_CARD,
        src.PAYMENT_AMT, FALSE
    );

    RETURN 'FACT_PAYMENTS merge complete';
END;
$$;


-- ── FACT_REVIEWS — MERGE ──────────────────────────────────────────────────────
CREATE OR REPLACE TASK AUDIT.TASK_DT_MERGE_FACT_REVIEWS
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_DT_MERGE_DIM_CUSTOMERS,
              AUDIT.TASK_DT_MERGE_DIM_PRODUCTS,
              AUDIT.TASK_DT_MERGE_DIM_SELLERS
    COMMENT   = 'MERGE new reviews from STRM_DT_ORDER_REVIEWS into MARTS.FACT_REVIEWS'
AS
$$
BEGIN
    IF (NOT SYSTEM$STREAM_HAS_DATA('STAGING.STRM_DT_ORDER_REVIEWS')) THEN
        RETURN 'STRM_DT_ORDER_REVIEWS empty — skipped';
    END IF;

    MERGE INTO MARTS.FACT_REVIEWS tgt
    USING (
        SELECT
            r.REVIEW_ID, r.ORDER_ID,
            TO_NUMBER(TO_CHAR(
                COALESCE(r.REVIEW_ANSWERED_AT::DATE, r.REVIEW_CREATED_DT),
                'YYYYMMDD'
            ))                                               AS ANSWERED_DATE_KEY,
            dc.CUSTOMER_KEY,
            r.REVIEW_SCORE, r.IS_POSITIVE, r.IS_NEUTRAL, r.IS_NEGATIVE,
            r.HAS_COMMENT, r.REVIEW_TITLE, r.REVIEW_MESSAGE
        FROM  STAGING.STRM_DT_ORDER_REVIEWS r
        JOIN  STAGING.DT_ORDERS    o  ON o.ORDER_ID    = r.ORDER_ID
        JOIN  STAGING.DT_CUSTOMERS sc ON sc.CUSTOMER_ID = o.CUSTOMER_ID
        JOIN  MARTS.DIM_CUSTOMERS  dc
              ON  dc.CUSTOMER_UNIQUE_ID = sc.CUSTOMER_UNIQUE_ID
              AND dc.SCD_START_DT  <= o.PURCHASE_TS::DATE
              AND (dc.SCD_END_DT IS NULL OR dc.SCD_END_DT > o.PURCHASE_TS::DATE)
    ) src
    ON tgt.REVIEW_ID_NK = src.REVIEW_ID
    WHEN MATCHED AND tgt.REVIEW_SCORE <> src.REVIEW_SCORE THEN UPDATE SET
        tgt.REVIEW_SCORE   = src.REVIEW_SCORE,
        tgt.IS_POSITIVE    = src.IS_POSITIVE,
        tgt.IS_NEUTRAL     = src.IS_NEUTRAL,
        tgt.IS_NEGATIVE    = src.IS_NEGATIVE,
        tgt.REVIEW_TITLE   = src.REVIEW_TITLE,
        tgt.REVIEW_MESSAGE = src.REVIEW_MESSAGE,
        tgt._LOADED_AT     = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        REVIEW_ID_NK, ORDER_ID_NK, ANSWERED_DATE_KEY, CUSTOMER_KEY,
        REVIEW_SCORE, IS_POSITIVE, IS_NEUTRAL, IS_NEGATIVE,
        HAS_COMMENT, REVIEW_TITLE, REVIEW_MESSAGE, _IS_LATE_ARRIVING
    ) VALUES (
        src.REVIEW_ID, src.ORDER_ID,
        src.ANSWERED_DATE_KEY, src.CUSTOMER_KEY,
        src.REVIEW_SCORE, src.IS_POSITIVE, src.IS_NEUTRAL, src.IS_NEGATIVE,
        src.HAS_COMMENT, src.REVIEW_TITLE, src.REVIEW_MESSAGE, FALSE
    );

    RETURN 'FACT_REVIEWS merge complete';
END;
$$;


-- =============================================================================
-- SECTION 5 : SP_AUDIT_PIPELINE — MULTI-STEP AUDIT STORED PROCEDURE
--
-- Called by the leaf task. Performs four steps in one execution:
--   Step 1 — Count rows loaded across all 3 fact tables in this run window
--   Step 2 — Run 3 DQ spot-checks on freshly loaded rows
--   Step 3 — Write one row per DQ check to AUDIT.AUD_DQ_CHECKS
--   Step 4 — Write one pipeline summary row to AUDIT.AUD_PIPELINE_RUNS
-- =============================================================================

CREATE OR REPLACE PROCEDURE AUDIT.SP_AUDIT_PIPELINE(run_window_minutes NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_items_loaded  NUMBER        DEFAULT 0;
    v_pmts_loaded   NUMBER        DEFAULT 0;
    v_revws_loaded  NUMBER        DEFAULT 0;
    v_total_loaded  NUMBER        DEFAULT 0;
    v_null_keys     NUMBER        DEFAULT 0;
    v_neg_price     NUMBER        DEFAULT 0;
    v_bad_score     NUMBER        DEFAULT 0;
    v_status        VARCHAR       DEFAULT 'SUCCESS';
    v_notes         VARCHAR       DEFAULT '';
    v_started       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
    -- ── Step 1: Count rows loaded in this run window ──────────────────────────
    SELECT COUNT(*) INTO :v_items_loaded FROM MARTS.FACT_ORDER_ITEMS
    WHERE _LOADED_AT >= DATEADD('minute', -:run_window_minutes, CURRENT_TIMESTAMP());

    SELECT COUNT(*) INTO :v_pmts_loaded  FROM MARTS.FACT_PAYMENTS
    WHERE _LOADED_AT >= DATEADD('minute', -:run_window_minutes, CURRENT_TIMESTAMP());

    SELECT COUNT(*) INTO :v_revws_loaded FROM MARTS.FACT_REVIEWS
    WHERE _LOADED_AT >= DATEADD('minute', -:run_window_minutes, CURRENT_TIMESTAMP());

    v_total_loaded := v_items_loaded + v_pmts_loaded + v_revws_loaded;

    -- ── Step 2: DQ spot-checks on freshly loaded rows ─────────────────────────
    -- Check A: null surrogate keys in FACT_ORDER_ITEMS
    SELECT COUNT(*) INTO :v_null_keys FROM MARTS.FACT_ORDER_ITEMS
    WHERE  _LOADED_AT >= DATEADD('minute', -:run_window_minutes, CURRENT_TIMESTAMP())
      AND  (CUSTOMER_KEY IS NULL OR PRODUCT_KEY IS NULL OR SELLER_KEY IS NULL);

    -- Check B: negative prices
    SELECT COUNT(*) INTO :v_neg_price FROM MARTS.FACT_ORDER_ITEMS
    WHERE  _LOADED_AT >= DATEADD('minute', -:run_window_minutes, CURRENT_TIMESTAMP())
      AND  PRICE_AMT < 0;

    -- Check C: review score out of 1-5 range
    SELECT COUNT(*) INTO :v_bad_score FROM MARTS.FACT_REVIEWS
    WHERE  _LOADED_AT >= DATEADD('minute', -:run_window_minutes, CURRENT_TIMESTAMP())
      AND  REVIEW_SCORE NOT BETWEEN 1 AND 5;

    -- Determine pipeline status
    IF (v_null_keys > 0 OR v_neg_price > 0 OR v_bad_score > 0) THEN
        v_status := 'PARTIAL';
        v_notes  := 'DQ issues — null_keys=' || v_null_keys
                    || ' neg_price=' || v_neg_price
                    || ' bad_score=' || v_bad_score;
    END IF;

    -- ── Step 3: Write DQ check results ────────────────────────────────────────
    INSERT INTO AUDIT.AUD_DQ_CHECKS
        (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
         TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
    VALUES
        ('DT_NULL_SURROGATE_KEYS', 'MARTS.FACT_ORDER_ITEMS', 'NULL_CHECK',
         IFF(v_null_keys = 0, 'PASS', 'FAIL'),
         v_items_loaded, v_null_keys,
         ROUND(100.0 * (v_items_loaded - v_null_keys) / NULLIF(v_items_loaded, 0), 3),
         'Null CUSTOMER_KEY / PRODUCT_KEY / SELLER_KEY in run window'),
        ('DT_NEGATIVE_PRICE',      'MARTS.FACT_ORDER_ITEMS', 'RANGE',
         IFF(v_neg_price = 0, 'PASS', 'FAIL'),
         v_items_loaded, v_neg_price,
         ROUND(100.0 * (v_items_loaded - v_neg_price) / NULLIF(v_items_loaded, 0), 3),
         'PRICE_AMT < 0 in run window'),
        ('DT_BAD_REVIEW_SCORE',    'MARTS.FACT_REVIEWS',     'RANGE',
         IFF(v_bad_score = 0, 'PASS', 'FAIL'),
         v_revws_loaded, v_bad_score,
         ROUND(100.0 * (v_revws_loaded - v_bad_score) / NULLIF(v_revws_loaded, 0), 3),
         'REVIEW_SCORE outside 1-5 in run window');

    -- ── Step 4: Write pipeline run summary ────────────────────────────────────
    INSERT INTO AUDIT.AUD_PIPELINE_RUNS
        (PIPELINE_NAME, TARGET_TABLE, STATUS,
         ROWS_LOADED, ROWS_REJECTED,
         STARTED_AT, FINISHED_AT, ERROR_MSG)
    VALUES (
        'DT_PIPELINE_DAG',
        'MARTS.FACT_ORDER_ITEMS + FACT_PAYMENTS + FACT_REVIEWS',
        v_status,
        v_total_loaded,
        v_null_keys + v_neg_price + v_bad_score,
        v_started,
        CURRENT_TIMESTAMP(),
        NULLIF(v_notes, '')
    );

    RETURN v_status || ' | loaded=' || v_total_loaded
           || IFF(v_notes = '', '', ' | ' || v_notes);
END;
$$;


-- ── LEAF TASK — calls the audit SP ───────────────────────────────────────────
CREATE OR REPLACE TASK AUDIT.TASK_DT_AUDIT_LOG
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_DT_MERGE_FACT_ITEMS,
              AUDIT.TASK_DT_MERGE_FACT_PAYMENTS,
              AUDIT.TASK_DT_MERGE_FACT_REVIEWS
    COMMENT   = 'Leaf task — calls SP_AUDIT_PIPELINE for multi-step audit logging'
AS
    CALL AUDIT.SP_AUDIT_PIPELINE(6);   -- 6-min window = 5-min schedule + 1-min buffer


-- =============================================================================
-- SECTION 6 : START / MONITOR / CONTROL
-- =============================================================================

-- ── Resume tasks — MUST go leaf → root ───────────────────────────────────────
ALTER TASK AUDIT.TASK_DT_AUDIT_LOG              RESUME;
ALTER TASK AUDIT.TASK_DT_MERGE_FACT_ITEMS       RESUME;
ALTER TASK AUDIT.TASK_DT_MERGE_FACT_PAYMENTS    RESUME;
ALTER TASK AUDIT.TASK_DT_MERGE_FACT_REVIEWS     RESUME;
ALTER TASK AUDIT.TASK_DT_MERGE_DIM_SELLERS      RESUME;
ALTER TASK AUDIT.TASK_DT_MERGE_DIM_PRODUCTS     RESUME;
ALTER TASK AUDIT.TASK_DT_MERGE_DIM_CUSTOMERS    RESUME;
ALTER TASK AUDIT.TASK_DT_MASTER_TRIGGER         RESUME;  -- root always last

-- ── Suspend all (maintenance window) ─────────────────────────────────────────
/*
ALTER TASK AUDIT.TASK_DT_MASTER_TRIGGER         SUSPEND;
ALTER TASK AUDIT.TASK_DT_MERGE_DIM_CUSTOMERS    SUSPEND;
ALTER TASK AUDIT.TASK_DT_MERGE_DIM_PRODUCTS     SUSPEND;
ALTER TASK AUDIT.TASK_DT_MERGE_DIM_SELLERS      SUSPEND;
ALTER TASK AUDIT.TASK_DT_MERGE_FACT_ITEMS       SUSPEND;
ALTER TASK AUDIT.TASK_DT_MERGE_FACT_PAYMENTS    SUSPEND;
ALTER TASK AUDIT.TASK_DT_MERGE_FACT_REVIEWS     SUSPEND;
ALTER TASK AUDIT.TASK_DT_AUDIT_LOG              SUSPEND;
*/

-- ── Manual trigger for testing (no need to wait 5 min) ───────────────────────
-- EXECUTE TASK AUDIT.TASK_DT_MASTER_TRIGGER;

-- ── Dynamic Table refresh status ─────────────────────────────────────────────
SHOW DYNAMIC TABLES IN SCHEMA STAGING;

-- ── Stream pending-data check ─────────────────────────────────────────────────
SELECT 'STRM_DT_CUSTOMERS'       AS stream_name, SYSTEM$STREAM_HAS_DATA('STAGING.STRM_DT_CUSTOMERS')       AS has_data UNION ALL
SELECT 'STRM_DT_PRODUCTS',                        SYSTEM$STREAM_HAS_DATA('STAGING.STRM_DT_PRODUCTS')               UNION ALL
SELECT 'STRM_DT_SELLERS',                         SYSTEM$STREAM_HAS_DATA('STAGING.STRM_DT_SELLERS')                UNION ALL
SELECT 'STRM_DT_ORDER_ITEMS',                     SYSTEM$STREAM_HAS_DATA('STAGING.STRM_DT_ORDER_ITEMS')            UNION ALL
SELECT 'STRM_DT_ORDER_PAYMENTS',                  SYSTEM$STREAM_HAS_DATA('STAGING.STRM_DT_ORDER_PAYMENTS')         UNION ALL
SELECT 'STRM_DT_ORDER_REVIEWS',                   SYSTEM$STREAM_HAS_DATA('STAGING.STRM_DT_ORDER_REVIEWS');

-- ── Task run history (last hour) ─────────────────────────────────────────────
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, RETURN_VALUE, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 50
))
WHERE NAME LIKE '%DT%'
ORDER BY SCHEDULED_TIME DESC;

-- ── Pipeline audit trail ──────────────────────────────────────────────────────
SELECT RUN_ID, STATUS, ROWS_LOADED, ROWS_REJECTED,
       DATEDIFF('second', STARTED_AT, FINISHED_AT) AS duration_secs,
       ERROR_MSG
FROM AUDIT.AUD_PIPELINE_RUNS
WHERE PIPELINE_NAME = 'DT_PIPELINE_DAG'
ORDER BY STARTED_AT DESC
LIMIT 20;

-- ── DQ results from this pipeline ────────────────────────────────────────────
SELECT CHECK_NAME, RESULT, FAIL_ROW_CNT, PASS_RATE_PCT, RUN_AT
FROM AUDIT.AUD_DQ_CHECKS
WHERE CHECK_NAME LIKE 'DT_%'
ORDER BY RUN_AT DESC, RESULT DESC;
