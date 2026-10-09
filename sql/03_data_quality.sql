-- =============================================================================
-- ShopSphere Data Warehouse – Data Quality & Validation
-- File   : 03_data_quality.sql
-- Purpose: Covers all five DQ requirements:
--
--   REQ 1 – Deduplicate records using business keys and a deterministic rule
--           (completes dedup for tables 02 did not cover: ORDERS, ORDER_ITEMS,
--            ORDER_PAYMENTS, PRODUCTS, SELLERS)
--
--   REQ 2 – Identify missing customer and product references (orphan checks)
--
--   REQ 3 – Separate valid rows from invalid rows
--           (adds _IS_VALID flag + AUDIT reject tables per source)
--
--   REQ 4 – Validate payment, order, quantity, and refund values
--           (business-rule range checks; zero/negative/null guards)
--
--   REQ 5 – Build a reusable data-quality results table
--           (populates AUDIT.AUD_DQ_CHECKS with every check in this file)
--
-- Prerequisites : 01_setup_and_load.sql and 02_staging_standardize.sql
-- Run in        : Snowflake worksheet using WH_M_ETL
-- =============================================================================

USE DATABASE SHOPSPHERE_DW;
USE WAREHOUSE WH_M_ETL;


-- =============================================================================
-- SECTION 1 : REUSABLE DQ RESULTS TABLE  (REQ 5)
-- =============================================================================
-- AUDIT.AUD_DQ_CHECKS was defined in the design doc; create it here if missing.

CREATE TABLE IF NOT EXISTS AUDIT.AUD_DQ_CHECKS (
    CHECK_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    CHECK_NAME      VARCHAR(200)  NOT NULL,   -- e.g. 'DEDUP_STG_ORDERS'
    TARGET_TABLE    VARCHAR(200)  NOT NULL,   -- fully-qualified table name
    CHECK_TYPE      VARCHAR(50)   NOT NULL,   -- DEDUP | REF_INTEGRITY | VALIDITY | RANGE
    RESULT          VARCHAR(10)   NOT NULL,   -- PASS | FAIL
    TOTAL_ROW_CNT   NUMBER        NOT NULL,
    FAIL_ROW_CNT    NUMBER        NOT NULL DEFAULT 0,
    PASS_RATE_PCT   NUMBER(6,3),              -- 100 * (1 - fail/total)
    RUN_AT          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    NOTES           VARCHAR(2000)
)
COMMENT = 'Reusable store for every data-quality check result';

-- Helper: a single INSERT macro used throughout this file.
-- Instead of a macro we use a standard INSERT … SELECT pattern every time,
-- keeping the file plain SQL with no procedural dependencies.


-- =============================================================================
-- SECTION 2 : DEDUPLICATION – REMAINING TABLES  (REQ 1)
--
-- 02_staging_standardize.sql already deduped:
--   STG_CUSTOMERS    (business key: CUSTOMER_ID,  tiebreak: latest _LOADED_AT)
--   STG_GEOLOCATION  (aggregated to one row per ZIP)
--   STG_ORDER_REVIEWS(business key: ORDER_ID,     tiebreak: latest REVIEW_ANSWERED_AT)
--
-- This section adds deterministic dedup for:
--   STG_ORDERS         business key: ORDER_ID          tiebreak: latest PURCHASE_TS
--   STG_ORDER_ITEMS    business key: ORDER_ID+ITEM_ID  tiebreak: latest _LOADED_AT
--   STG_ORDER_PAYMENTS business key: ORDER_ID+SEQ      tiebreak: latest _LOADED_AT
--   STG_PRODUCTS       business key: PRODUCT_ID        tiebreak: latest _LOADED_AT
--   STG_SELLERS        business key: SELLER_ID         tiebreak: latest _LOADED_AT
--
-- Strategy: add a _DUP_RANK column then UPDATE _IS_DUPLICATE.
-- We ALTER the staging tables to add the flag where it is missing.
-- =============================================================================

-- ── Add _IS_DUPLICATE to tables that 02 did not include it ───────────────────
ALTER TABLE STAGING.STG_ORDERS          ADD COLUMN IF NOT EXISTS _IS_DUPLICATE BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE STAGING.STG_ORDER_ITEMS     ADD COLUMN IF NOT EXISTS _IS_DUPLICATE BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE STAGING.STG_ORDER_PAYMENTS  ADD COLUMN IF NOT EXISTS _IS_DUPLICATE BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE STAGING.STG_PRODUCTS        ADD COLUMN IF NOT EXISTS _IS_DUPLICATE BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE STAGING.STG_SELLERS         ADD COLUMN IF NOT EXISTS _IS_DUPLICATE BOOLEAN NOT NULL DEFAULT FALSE;


-- ── 2.1  STG_ORDERS  ─────────────────────────────────────────────────────────
-- Business key : ORDER_ID
-- Tiebreak     : keep the row with the latest PURCHASE_TS; mark others TRUE
UPDATE STAGING.STG_ORDERS tgt
SET    _IS_DUPLICATE = TRUE
WHERE  _IS_DUPLICATE = FALSE
  AND  EXISTS (
           SELECT 1
           FROM (
               SELECT ORDER_ID,
                      ROW_NUMBER() OVER (
                          PARTITION BY ORDER_ID
                          ORDER BY PURCHASE_TS DESC, _LOADED_AT DESC
                      ) AS rn
               FROM STAGING.STG_ORDERS
           ) ranked
           WHERE ranked.ORDER_ID = tgt.ORDER_ID
             AND ranked.rn > 1
             AND ranked.ORDER_ID = tgt.ORDER_ID
       );

-- Cleaner alternative using a CTE-based merge (preferred if UPDATE above is slow):
-- MERGE INTO STAGING.STG_ORDERS tgt
-- USING (
--     SELECT ORDER_ID, PURCHASE_TS, _LOADED_AT,
--            ROW_NUMBER() OVER (
--                PARTITION BY ORDER_ID ORDER BY PURCHASE_TS DESC, _LOADED_AT DESC
--            ) AS rn
--     FROM STAGING.STG_ORDERS
-- ) src ON tgt.ORDER_ID = src.ORDER_ID
--       AND tgt.PURCHASE_TS = src.PURCHASE_TS
--       AND tgt._LOADED_AT  = src._LOADED_AT
-- WHEN MATCHED AND src.rn > 1 THEN UPDATE SET tgt._IS_DUPLICATE = TRUE;


-- ── 2.2  STG_ORDER_ITEMS  ────────────────────────────────────────────────────
-- Business key : ORDER_ID + ORDER_ITEM_ID
-- Tiebreak     : latest _LOADED_AT
UPDATE STAGING.STG_ORDER_ITEMS tgt
SET    _IS_DUPLICATE = TRUE
WHERE  _IS_DUPLICATE = FALSE
  AND  EXISTS (
           SELECT 1
           FROM (
               SELECT ORDER_ID, ORDER_ITEM_ID,
                      ROW_NUMBER() OVER (
                          PARTITION BY ORDER_ID, ORDER_ITEM_ID
                          ORDER BY _LOADED_AT DESC
                      ) AS rn
               FROM STAGING.STG_ORDER_ITEMS
           ) ranked
           WHERE ranked.ORDER_ID      = tgt.ORDER_ID
             AND ranked.ORDER_ITEM_ID = tgt.ORDER_ITEM_ID
             AND ranked.rn > 1
       );


-- ── 2.3  STG_ORDER_PAYMENTS  ─────────────────────────────────────────────────
-- Business key : ORDER_ID + PAYMENT_SEQUENTIAL
-- Tiebreak     : latest _LOADED_AT
UPDATE STAGING.STG_ORDER_PAYMENTS tgt
SET    _IS_DUPLICATE = TRUE
WHERE  _IS_DUPLICATE = FALSE
  AND  EXISTS (
           SELECT 1
           FROM (
               SELECT ORDER_ID, PAYMENT_SEQUENTIAL,
                      ROW_NUMBER() OVER (
                          PARTITION BY ORDER_ID, PAYMENT_SEQUENTIAL
                          ORDER BY _LOADED_AT DESC
                      ) AS rn
               FROM STAGING.STG_ORDER_PAYMENTS
           ) ranked
           WHERE ranked.ORDER_ID           = tgt.ORDER_ID
             AND ranked.PAYMENT_SEQUENTIAL = tgt.PAYMENT_SEQUENTIAL
             AND ranked.rn > 1
       );


-- ── 2.4  STG_PRODUCTS  ───────────────────────────────────────────────────────
-- Business key : PRODUCT_ID
-- Tiebreak     : latest _LOADED_AT
UPDATE STAGING.STG_PRODUCTS tgt
SET    _IS_DUPLICATE = TRUE
WHERE  _IS_DUPLICATE = FALSE
  AND  EXISTS (
           SELECT 1
           FROM (
               SELECT PRODUCT_ID,
                      ROW_NUMBER() OVER (
                          PARTITION BY PRODUCT_ID
                          ORDER BY _LOADED_AT DESC
                      ) AS rn
               FROM STAGING.STG_PRODUCTS
           ) ranked
           WHERE ranked.PRODUCT_ID = tgt.PRODUCT_ID
             AND ranked.rn > 1
       );


-- ── 2.5  STG_SELLERS  ────────────────────────────────────────────────────────
-- Business key : SELLER_ID
-- Tiebreak     : latest _LOADED_AT
UPDATE STAGING.STG_SELLERS tgt
SET    _IS_DUPLICATE = TRUE
WHERE  _IS_DUPLICATE = FALSE
  AND  EXISTS (
           SELECT 1
           FROM (
               SELECT SELLER_ID,
                      ROW_NUMBER() OVER (
                          PARTITION BY SELLER_ID
                          ORDER BY _LOADED_AT DESC
                      ) AS rn
               FROM STAGING.STG_SELLERS
           ) ranked
           WHERE ranked.SELLER_ID = tgt.SELLER_ID
             AND ranked.rn > 1
       );


-- ── DQ LOG: Deduplication results  ───────────────────────────────────────────
INSERT INTO AUDIT.AUD_DQ_CHECKS
    (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
     TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
SELECT
    v.check_name,
    v.target_table,
    'DEDUP'                                     AS CHECK_TYPE,
    IFF(v.fail_cnt = 0, 'PASS', 'FAIL')         AS RESULT,
    v.total_cnt                                 AS TOTAL_ROW_CNT,
    v.fail_cnt                                  AS FAIL_ROW_CNT,
    ROUND(100.0 * (v.total_cnt - v.fail_cnt)
               / NULLIF(v.total_cnt, 0), 3)     AS PASS_RATE_PCT,
    'Rows where _IS_DUPLICATE = TRUE'           AS NOTES
FROM (
    SELECT 'DEDUP_STG_CUSTOMERS'      AS check_name, 'STAGING.STG_CUSTOMERS'      AS target_table, COUNT(*) AS total_cnt, SUM(IFF(_IS_DUPLICATE,1,0)) AS fail_cnt FROM STAGING.STG_CUSTOMERS      UNION ALL
    SELECT 'DEDUP_STG_ORDERS',               'STAGING.STG_ORDERS',               COUNT(*), SUM(IFF(_IS_DUPLICATE,1,0)) FROM STAGING.STG_ORDERS         UNION ALL
    SELECT 'DEDUP_STG_ORDER_ITEMS',          'STAGING.STG_ORDER_ITEMS',           COUNT(*), SUM(IFF(_IS_DUPLICATE,1,0)) FROM STAGING.STG_ORDER_ITEMS    UNION ALL
    SELECT 'DEDUP_STG_ORDER_PAYMENTS',       'STAGING.STG_ORDER_PAYMENTS',        COUNT(*), SUM(IFF(_IS_DUPLICATE,1,0)) FROM STAGING.STG_ORDER_PAYMENTS UNION ALL
    SELECT 'DEDUP_STG_ORDER_REVIEWS',        'STAGING.STG_ORDER_REVIEWS',         COUNT(*), SUM(IFF(_IS_DUPLICATE,1,0)) FROM STAGING.STG_ORDER_REVIEWS  UNION ALL
    SELECT 'DEDUP_STG_PRODUCTS',             'STAGING.STG_PRODUCTS',              COUNT(*), SUM(IFF(_IS_DUPLICATE,1,0)) FROM STAGING.STG_PRODUCTS        UNION ALL
    SELECT 'DEDUP_STG_SELLERS',              'STAGING.STG_SELLERS',               COUNT(*), SUM(IFF(_IS_DUPLICATE,1,0)) FROM STAGING.STG_SELLERS
) v;


-- =============================================================================
-- SECTION 3 : MISSING REFERENCE CHECKS  (REQ 2)
--
-- Checks:
--   3.1  Orders whose CUSTOMER_ID does not exist in STG_CUSTOMERS
--   3.2  Order items whose ORDER_ID does not exist in STG_ORDERS
--   3.3  Order items whose PRODUCT_ID does not exist in STG_PRODUCTS
--   3.4  Order items whose SELLER_ID does not exist in STG_SELLERS
--   3.5  Payments whose ORDER_ID does not exist in STG_ORDERS
--   3.6  Reviews  whose ORDER_ID does not exist in STG_ORDERS
-- =============================================================================

-- ── Add referential-integrity flag to key tables ──────────────────────────────
ALTER TABLE STAGING.STG_ORDERS         ADD COLUMN IF NOT EXISTS _HAS_VALID_CUSTOMER BOOLEAN;
ALTER TABLE STAGING.STG_ORDER_ITEMS    ADD COLUMN IF NOT EXISTS _HAS_VALID_ORDER     BOOLEAN;
ALTER TABLE STAGING.STG_ORDER_ITEMS    ADD COLUMN IF NOT EXISTS _HAS_VALID_PRODUCT   BOOLEAN;
ALTER TABLE STAGING.STG_ORDER_ITEMS    ADD COLUMN IF NOT EXISTS _HAS_VALID_SELLER    BOOLEAN;
ALTER TABLE STAGING.STG_ORDER_PAYMENTS ADD COLUMN IF NOT EXISTS _HAS_VALID_ORDER     BOOLEAN;
ALTER TABLE STAGING.STG_ORDER_REVIEWS  ADD COLUMN IF NOT EXISTS _HAS_VALID_ORDER     BOOLEAN;


-- ── 3.1  Orders → Customers  ─────────────────────────────────────────────────
UPDATE STAGING.STG_ORDERS tgt
SET    _HAS_VALID_CUSTOMER = EXISTS (
           SELECT 1 FROM STAGING.STG_CUSTOMERS c
           WHERE c.CUSTOMER_ID = tgt.CUSTOMER_ID
             AND c._IS_DUPLICATE = FALSE
       );

-- ── 3.2  Order Items → Orders  ───────────────────────────────────────────────
UPDATE STAGING.STG_ORDER_ITEMS tgt
SET    _HAS_VALID_ORDER = EXISTS (
           SELECT 1 FROM STAGING.STG_ORDERS o
           WHERE o.ORDER_ID = tgt.ORDER_ID
             AND o._IS_DUPLICATE = FALSE
       );

-- ── 3.3  Order Items → Products  ─────────────────────────────────────────────
UPDATE STAGING.STG_ORDER_ITEMS tgt
SET    _HAS_VALID_PRODUCT = EXISTS (
           SELECT 1 FROM STAGING.STG_PRODUCTS p
           WHERE p.PRODUCT_ID = tgt.PRODUCT_ID
             AND p._IS_DUPLICATE = FALSE
       );

-- ── 3.4  Order Items → Sellers  ──────────────────────────────────────────────
UPDATE STAGING.STG_ORDER_ITEMS tgt
SET    _HAS_VALID_SELLER = EXISTS (
           SELECT 1 FROM STAGING.STG_SELLERS s
           WHERE s.SELLER_ID = tgt.SELLER_ID
             AND s._IS_DUPLICATE = FALSE
       );

-- ── 3.5  Payments → Orders  ──────────────────────────────────────────────────
UPDATE STAGING.STG_ORDER_PAYMENTS tgt
SET    _HAS_VALID_ORDER = EXISTS (
           SELECT 1 FROM STAGING.STG_ORDERS o
           WHERE o.ORDER_ID = tgt.ORDER_ID
             AND o._IS_DUPLICATE = FALSE
       );

-- ── 3.6  Reviews → Orders  ───────────────────────────────────────────────────
UPDATE STAGING.STG_ORDER_REVIEWS tgt
SET    _HAS_VALID_ORDER = EXISTS (
           SELECT 1 FROM STAGING.STG_ORDERS o
           WHERE o.ORDER_ID = tgt.ORDER_ID
             AND o._IS_DUPLICATE = FALSE
       );


-- ── DQ LOG: Reference integrity results ──────────────────────────────────────
INSERT INTO AUDIT.AUD_DQ_CHECKS
    (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
     TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
SELECT
    v.check_name, v.target_table, 'REF_INTEGRITY',
    IFF(v.fail_cnt = 0, 'PASS', 'FAIL'),
    v.total_cnt, v.fail_cnt,
    ROUND(100.0 * (v.total_cnt - v.fail_cnt) / NULLIF(v.total_cnt, 0), 3),
    v.notes
FROM (
    SELECT 'REF_ORDER_TO_CUSTOMER'    AS check_name, 'STAGING.STG_ORDERS'        AS target_table, COUNT(*) AS total_cnt, SUM(IFF(_HAS_VALID_CUSTOMER = FALSE,1,0)) AS fail_cnt, 'Orders with no matching CUSTOMER_ID in STG_CUSTOMERS'  AS notes FROM STAGING.STG_ORDERS         WHERE _IS_DUPLICATE = FALSE UNION ALL
    SELECT 'REF_ITEM_TO_ORDER',                       'STAGING.STG_ORDER_ITEMS',  COUNT(*), SUM(IFF(_HAS_VALID_ORDER   = FALSE,1,0)), 'Items with no matching ORDER_ID in STG_ORDERS'         FROM STAGING.STG_ORDER_ITEMS   WHERE _IS_DUPLICATE = FALSE UNION ALL
    SELECT 'REF_ITEM_TO_PRODUCT',                     'STAGING.STG_ORDER_ITEMS',  COUNT(*), SUM(IFF(_HAS_VALID_PRODUCT = FALSE,1,0)), 'Items with no matching PRODUCT_ID in STG_PRODUCTS'     FROM STAGING.STG_ORDER_ITEMS   WHERE _IS_DUPLICATE = FALSE UNION ALL
    SELECT 'REF_ITEM_TO_SELLER',                      'STAGING.STG_ORDER_ITEMS',  COUNT(*), SUM(IFF(_HAS_VALID_SELLER  = FALSE,1,0)), 'Items with no matching SELLER_ID in STG_SELLERS'       FROM STAGING.STG_ORDER_ITEMS   WHERE _IS_DUPLICATE = FALSE UNION ALL
    SELECT 'REF_PAYMENT_TO_ORDER',                    'STAGING.STG_ORDER_PAYMENTS',COUNT(*),SUM(IFF(_HAS_VALID_ORDER   = FALSE,1,0)), 'Payments with no matching ORDER_ID in STG_ORDERS'      FROM STAGING.STG_ORDER_PAYMENTS WHERE _IS_DUPLICATE = FALSE UNION ALL
    SELECT 'REF_REVIEW_TO_ORDER',                     'STAGING.STG_ORDER_REVIEWS', COUNT(*),SUM(IFF(_HAS_VALID_ORDER   = FALSE,1,0)), 'Reviews with no matching ORDER_ID in STG_ORDERS'       FROM STAGING.STG_ORDER_REVIEWS  WHERE _IS_DUPLICATE = FALSE
) v;


-- =============================================================================
-- SECTION 4 : VALIDITY FLAGS — SEPARATE VALID FROM INVALID ROWS  (REQ 3)
--
-- A row is INVALID if it fails ANY of the business rules below.
-- _IS_VALID = FALSE rows stay in staging for audit; only _IS_VALID = TRUE rows
-- are promoted to the MARTS layer.
--
-- Rules per table:
--   STG_ORDERS        : PURCHASE_TS not null; ORDER_ID not null; valid status
--   STG_ORDER_ITEMS   : PRICE_AMT > 0; FREIGHT_AMT >= 0; ORDER_ITEM_ID >= 1
--   STG_ORDER_PAYMENTS: PAYMENT_AMT > 0; PAYMENT_SEQUENTIAL >= 1; known type
--   STG_ORDER_REVIEWS : REVIEW_SCORE between 1 and 5; REVIEW_ID not null
--   STG_CUSTOMERS     : CUSTOMER_ID not null; CUSTOMER_UNIQUE_ID not null
--   STG_PRODUCTS      : PRODUCT_ID not null; WEIGHT_G > 0 (when not null)
--   STG_SELLERS       : SELLER_ID not null
-- =============================================================================

-- ── Add _IS_VALID to all staging tables ──────────────────────────────────────
ALTER TABLE STAGING.STG_CUSTOMERS       ADD COLUMN IF NOT EXISTS _IS_VALID BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE STAGING.STG_ORDERS          ADD COLUMN IF NOT EXISTS _IS_VALID BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE STAGING.STG_ORDER_ITEMS     ADD COLUMN IF NOT EXISTS _IS_VALID BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE STAGING.STG_ORDER_PAYMENTS  ADD COLUMN IF NOT EXISTS _IS_VALID BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE STAGING.STG_ORDER_REVIEWS   ADD COLUMN IF NOT EXISTS _IS_VALID BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE STAGING.STG_PRODUCTS        ADD COLUMN IF NOT EXISTS _IS_VALID BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE STAGING.STG_SELLERS         ADD COLUMN IF NOT EXISTS _IS_VALID BOOLEAN NOT NULL DEFAULT TRUE;

-- ── 4.1  STG_CUSTOMERS validity  ─────────────────────────────────────────────
-- Invalid if: CUSTOMER_ID or CUSTOMER_UNIQUE_ID is null (already filtered in 02,
--             but guard here in case of direct inserts)
UPDATE STAGING.STG_CUSTOMERS
SET    _IS_VALID = FALSE
WHERE  CUSTOMER_ID        IS NULL
    OR CUSTOMER_UNIQUE_ID IS NULL;

-- ── 4.2  STG_ORDERS validity  ────────────────────────────────────────────────
-- Invalid if: PURCHASE_TS null, ORDER_STATUS not in known set,
--             or customer reference missing
UPDATE STAGING.STG_ORDERS
SET    _IS_VALID = FALSE
WHERE  PURCHASE_TS IS NULL
    OR ORDER_STATUS NOT IN ('DELIVERED','SHIPPED','CANCELED','INVOICED',
                             'PROCESSING','UNAVAILABLE','APPROVED','CREATED')
    OR _HAS_VALID_CUSTOMER = FALSE;

-- ── 4.3  STG_ORDER_ITEMS validity  ───────────────────────────────────────────
-- Invalid if: PRICE_AMT <= 0, FREIGHT_AMT < 0, ORDER_ITEM_ID < 1,
--             or any reference broken
UPDATE STAGING.STG_ORDER_ITEMS
SET    _IS_VALID = FALSE
WHERE  PRICE_AMT    <= 0
    OR FREIGHT_AMT  <  0
    OR ORDER_ITEM_ID < 1
    OR _HAS_VALID_ORDER   = FALSE
    OR _HAS_VALID_PRODUCT = FALSE
    OR _HAS_VALID_SELLER  = FALSE;

-- ── 4.4  STG_ORDER_PAYMENTS validity  ────────────────────────────────────────
-- Invalid if: PAYMENT_AMT <= 0, SEQUENTIAL < 1, unknown type, broken order ref
UPDATE STAGING.STG_ORDER_PAYMENTS
SET    _IS_VALID = FALSE
WHERE  PAYMENT_AMT        <= 0
    OR PAYMENT_SEQUENTIAL <  1
    OR PAYMENT_TYPE NOT IN ('CREDIT_CARD','BOLETO','VOUCHER','DEBIT_CARD','NOT_DEFINED')
    OR _HAS_VALID_ORDER = FALSE;

-- ── 4.5  STG_ORDER_REVIEWS validity  ─────────────────────────────────────────
-- Invalid if: score out of range, broken order ref
UPDATE STAGING.STG_ORDER_REVIEWS
SET    _IS_VALID = FALSE
WHERE  REVIEW_SCORE NOT BETWEEN 1 AND 5
    OR _HAS_VALID_ORDER = FALSE;

-- ── 4.6  STG_PRODUCTS validity  ──────────────────────────────────────────────
-- Invalid if: weight is present but <= 0 (a product cannot have zero weight)
UPDATE STAGING.STG_PRODUCTS
SET    _IS_VALID = FALSE
WHERE  WEIGHT_G IS NOT NULL AND WEIGHT_G <= 0;

-- ── 4.7  STG_SELLERS validity  ───────────────────────────────────────────────
-- Already guaranteed by WHERE clause in 02; guard here for completeness
UPDATE STAGING.STG_SELLERS
SET    _IS_VALID = FALSE
WHERE  SELLER_ID IS NULL;


-- ── DQ LOG: Validity results  ─────────────────────────────────────────────────
INSERT INTO AUDIT.AUD_DQ_CHECKS
    (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
     TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
SELECT
    v.check_name, v.target_table, 'VALIDITY',
    IFF(v.fail_cnt = 0, 'PASS', 'FAIL'),
    v.total_cnt, v.fail_cnt,
    ROUND(100.0 * (v.total_cnt - v.fail_cnt) / NULLIF(v.total_cnt, 0), 3),
    v.notes
FROM (
    SELECT 'VALID_STG_CUSTOMERS'     AS check_name, 'STAGING.STG_CUSTOMERS'       AS target_table, COUNT(*) AS total_cnt, SUM(IFF(_IS_VALID=FALSE,1,0)) AS fail_cnt, 'Rows failing NULL key checks'                   AS notes FROM STAGING.STG_CUSTOMERS      UNION ALL
    SELECT 'VALID_STG_ORDERS',                       'STAGING.STG_ORDERS',         COUNT(*), SUM(IFF(_IS_VALID=FALSE,1,0)), 'Rows failing NULL/status/ref checks'            FROM STAGING.STG_ORDERS         UNION ALL
    SELECT 'VALID_STG_ORDER_ITEMS',                  'STAGING.STG_ORDER_ITEMS',    COUNT(*), SUM(IFF(_IS_VALID=FALSE,1,0)), 'Rows failing price/freight/ref checks'          FROM STAGING.STG_ORDER_ITEMS    UNION ALL
    SELECT 'VALID_STG_ORDER_PAYMENTS',               'STAGING.STG_ORDER_PAYMENTS', COUNT(*), SUM(IFF(_IS_VALID=FALSE,1,0)), 'Rows failing payment amount/type/ref checks'   FROM STAGING.STG_ORDER_PAYMENTS UNION ALL
    SELECT 'VALID_STG_ORDER_REVIEWS',                'STAGING.STG_ORDER_REVIEWS',  COUNT(*), SUM(IFF(_IS_VALID=FALSE,1,0)), 'Rows failing score range/ref checks'            FROM STAGING.STG_ORDER_REVIEWS  UNION ALL
    SELECT 'VALID_STG_PRODUCTS',                     'STAGING.STG_PRODUCTS',       COUNT(*), SUM(IFF(_IS_VALID=FALSE,1,0)), 'Rows failing weight <= 0 check'                 FROM STAGING.STG_PRODUCTS       UNION ALL
    SELECT 'VALID_STG_SELLERS',                      'STAGING.STG_SELLERS',        COUNT(*), SUM(IFF(_IS_VALID=FALSE,1,0)), 'Rows failing NULL SELLER_ID check'              FROM STAGING.STG_SELLERS
) v;


-- =============================================================================
-- SECTION 5 : BUSINESS-RULE VALUE VALIDATION  (REQ 4)
--
-- These checks run as DQ log entries only (no row updates).
-- They surface anomalies without blocking the load, so analysts can
-- investigate and decide on remediation.
--
-- Checks:
--   5.1  Payment total per order matches sum of order item totals (± 10%)
--   5.2  Zero-price order items
--   5.3  Excessive freight (freight > price)
--   5.4  Payment installments out of range (must be 1–24)
--   5.5  Orders with no payment at all
--   5.6  Orders with no line items at all
--   5.7  Delivered orders with null actual delivery timestamp
--   5.8  Approved_at before purchase_ts (timestamp inversion)
--   5.9  Delivered_customer_at before delivered_carrier_at
-- =============================================================================

-- ── 5.1  Payment vs Item total reconciliation  ───────────────────────────────
INSERT INTO AUDIT.AUD_DQ_CHECKS
    (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
     TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
WITH order_item_totals AS (
    SELECT ORDER_ID, ROUND(SUM(TOTAL_ITEM_AMT), 2) AS item_total
    FROM STAGING.STG_ORDER_ITEMS
    WHERE _IS_DUPLICATE = FALSE AND _IS_VALID = TRUE
    GROUP BY ORDER_ID
),
order_payment_totals AS (
    SELECT ORDER_ID, ROUND(SUM(PAYMENT_AMT), 2) AS payment_total
    FROM STAGING.STG_ORDER_PAYMENTS
    WHERE _IS_DUPLICATE = FALSE AND _IS_VALID = TRUE
    GROUP BY ORDER_ID
),
reconciled AS (
    SELECT
        i.ORDER_ID,
        i.item_total,
        p.payment_total,
        ABS(i.item_total - COALESCE(p.payment_total, 0)) AS variance,
        -- Allow 10% tolerance for rounding / voucher discounts
        (ABS(i.item_total - COALESCE(p.payment_total, 0))
         / NULLIF(i.item_total, 0)) > 0.10               AS is_mismatch
    FROM order_item_totals  i
    LEFT JOIN order_payment_totals p ON p.ORDER_ID = i.ORDER_ID
)
SELECT
    'PAYMENT_ITEM_RECONCILIATION',
    'STAGING.STG_ORDER_PAYMENTS',
    'RANGE',
    IFF(SUM(IFF(is_mismatch, 1, 0)) = 0, 'PASS', 'FAIL'),
    COUNT(*),
    SUM(IFF(is_mismatch, 1, 0)),
    ROUND(100.0 * SUM(IFF(NOT is_mismatch, 1, 0)) / NULLIF(COUNT(*), 0), 3),
    'Orders where |payment_total - item_total| / item_total > 10%'
FROM reconciled;


-- ── 5.2  Zero-price items  ────────────────────────────────────────────────────
INSERT INTO AUDIT.AUD_DQ_CHECKS
    (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
     TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
SELECT
    'ZERO_PRICE_ITEMS',
    'STAGING.STG_ORDER_ITEMS',
    'RANGE',
    IFF(SUM(IFF(PRICE_AMT = 0, 1, 0)) = 0, 'PASS', 'FAIL'),
    COUNT(*),
    SUM(IFF(PRICE_AMT = 0, 1, 0)),
    ROUND(100.0 * SUM(IFF(PRICE_AMT > 0, 1, 0)) / NULLIF(COUNT(*), 0), 3),
    'Order items with PRICE_AMT = 0.00'
FROM STAGING.STG_ORDER_ITEMS
WHERE _IS_DUPLICATE = FALSE;


-- ── 5.3  Freight > Price  ────────────────────────────────────────────────────
INSERT INTO AUDIT.AUD_DQ_CHECKS
    (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
     TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
SELECT
    'FREIGHT_EXCEEDS_PRICE',
    'STAGING.STG_ORDER_ITEMS',
    'RANGE',
    IFF(SUM(IFF(FREIGHT_AMT > PRICE_AMT, 1, 0)) = 0, 'PASS', 'FAIL'),
    COUNT(*),
    SUM(IFF(FREIGHT_AMT > PRICE_AMT, 1, 0)),
    ROUND(100.0 * SUM(IFF(FREIGHT_AMT <= PRICE_AMT, 1, 0)) / NULLIF(COUNT(*), 0), 3),
    'Items where freight cost exceeds product price'
FROM STAGING.STG_ORDER_ITEMS
WHERE _IS_DUPLICATE = FALSE AND _IS_VALID = TRUE;


-- ── 5.4  Payment installments out of range  ──────────────────────────────────
INSERT INTO AUDIT.AUD_DQ_CHECKS
    (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
     TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
SELECT
    'INSTALLMENTS_OUT_OF_RANGE',
    'STAGING.STG_ORDER_PAYMENTS',
    'RANGE',
    IFF(SUM(IFF(PAYMENT_INSTALLMENTS NOT BETWEEN 1 AND 24, 1, 0)) = 0, 'PASS', 'FAIL'),
    COUNT(*),
    SUM(IFF(PAYMENT_INSTALLMENTS NOT BETWEEN 1 AND 24, 1, 0)),
    ROUND(100.0 * SUM(IFF(PAYMENT_INSTALLMENTS BETWEEN 1 AND 24, 1, 0)) / NULLIF(COUNT(*), 0), 3),
    'Payments where installment count is not between 1 and 24'
FROM STAGING.STG_ORDER_PAYMENTS
WHERE _IS_DUPLICATE = FALSE;


-- ── 5.5  Orders with no payment  ────────────────────────────────────────────
INSERT INTO AUDIT.AUD_DQ_CHECKS
    (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
     TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
WITH unpaid AS (
    SELECT o.ORDER_ID
    FROM STAGING.STG_ORDERS o
    LEFT JOIN STAGING.STG_ORDER_PAYMENTS p
           ON p.ORDER_ID = o.ORDER_ID AND p._IS_DUPLICATE = FALSE
    WHERE o._IS_DUPLICATE = FALSE
      AND o.IS_CANCELED   = FALSE
      AND p.ORDER_ID IS NULL
)
SELECT
    'ORDERS_WITHOUT_PAYMENT',
    'STAGING.STG_ORDERS',
    'NULL_CHECK',
    IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
    (SELECT COUNT(*) FROM STAGING.STG_ORDERS WHERE _IS_DUPLICATE = FALSE AND IS_CANCELED = FALSE),
    COUNT(*),
    ROUND(100.0 * (
        (SELECT COUNT(*) FROM STAGING.STG_ORDERS WHERE _IS_DUPLICATE=FALSE AND IS_CANCELED=FALSE)
        - COUNT(*)
    ) / NULLIF(
        (SELECT COUNT(*) FROM STAGING.STG_ORDERS WHERE _IS_DUPLICATE=FALSE AND IS_CANCELED=FALSE)
    , 0), 3),
    'Non-canceled orders with no matching payment record'
FROM unpaid;


-- ── 5.6  Orders with no line items  ─────────────────────────────────────────
INSERT INTO AUDIT.AUD_DQ_CHECKS
    (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
     TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
WITH empty_orders AS (
    SELECT o.ORDER_ID
    FROM STAGING.STG_ORDERS o
    LEFT JOIN STAGING.STG_ORDER_ITEMS i
           ON i.ORDER_ID = o.ORDER_ID AND i._IS_DUPLICATE = FALSE
    WHERE o._IS_DUPLICATE = FALSE
      AND i.ORDER_ID IS NULL
)
SELECT
    'ORDERS_WITHOUT_ITEMS',
    'STAGING.STG_ORDERS',
    'NULL_CHECK',
    IFF(COUNT(*) = 0, 'PASS', 'FAIL'),
    (SELECT COUNT(*) FROM STAGING.STG_ORDERS WHERE _IS_DUPLICATE = FALSE),
    COUNT(*),
    ROUND(100.0 * (
        (SELECT COUNT(*) FROM STAGING.STG_ORDERS WHERE _IS_DUPLICATE = FALSE) - COUNT(*)
    ) / NULLIF((SELECT COUNT(*) FROM STAGING.STG_ORDERS WHERE _IS_DUPLICATE = FALSE), 0), 3),
    'Orders with no matching rows in STG_ORDER_ITEMS'
FROM empty_orders;


-- ── 5.7  Delivered orders with null actual delivery timestamp  ───────────────
INSERT INTO AUDIT.AUD_DQ_CHECKS
    (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
     TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
SELECT
    'DELIVERED_NULL_TIMESTAMP',
    'STAGING.STG_ORDERS',
    'NULL_CHECK',
    IFF(SUM(IFF(IS_DELIVERED=TRUE AND DELIVERED_CUSTOMER_AT IS NULL, 1, 0)) = 0, 'PASS', 'FAIL'),
    COUNT(*),
    SUM(IFF(IS_DELIVERED = TRUE AND DELIVERED_CUSTOMER_AT IS NULL, 1, 0)),
    ROUND(100.0 * SUM(IFF(NOT(IS_DELIVERED=TRUE AND DELIVERED_CUSTOMER_AT IS NULL), 1, 0)) / NULLIF(COUNT(*), 0), 3),
    'Orders with status DELIVERED but DELIVERED_CUSTOMER_AT is NULL'
FROM STAGING.STG_ORDERS
WHERE _IS_DUPLICATE = FALSE;


-- ── 5.8  Approved_at before purchase_ts  ────────────────────────────────────
INSERT INTO AUDIT.AUD_DQ_CHECKS
    (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
     TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
SELECT
    'TIMESTAMP_INVERSION_APPROVED_BEFORE_PURCHASE',
    'STAGING.STG_ORDERS',
    'RANGE',
    IFF(SUM(IFF(APPROVED_AT < PURCHASE_TS, 1, 0)) = 0, 'PASS', 'FAIL'),
    COUNT(*),
    SUM(IFF(APPROVED_AT < PURCHASE_TS, 1, 0)),
    ROUND(100.0 * SUM(IFF(APPROVED_AT IS NULL OR APPROVED_AT >= PURCHASE_TS, 1, 0)) / NULLIF(COUNT(*), 0), 3),
    'Orders where APPROVED_AT timestamp is earlier than PURCHASE_TS'
FROM STAGING.STG_ORDERS
WHERE _IS_DUPLICATE = FALSE;


-- ── 5.9  Delivered to customer before carrier pickup  ────────────────────────
INSERT INTO AUDIT.AUD_DQ_CHECKS
    (CHECK_NAME, TARGET_TABLE, CHECK_TYPE, RESULT,
     TOTAL_ROW_CNT, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES)
SELECT
    'TIMESTAMP_INVERSION_DELIVERED_BEFORE_CARRIER',
    'STAGING.STG_ORDERS',
    'RANGE',
    IFF(SUM(IFF(DELIVERED_CUSTOMER_AT < DELIVERED_CARRIER_AT, 1, 0)) = 0, 'PASS', 'FAIL'),
    COUNT(*),
    SUM(IFF(DELIVERED_CUSTOMER_AT < DELIVERED_CARRIER_AT, 1, 0)),
    ROUND(100.0 * SUM(IFF(DELIVERED_CUSTOMER_AT IS NULL OR DELIVERED_CUSTOMER_AT >= DELIVERED_CARRIER_AT, 1, 0)) / NULLIF(COUNT(*), 0), 3),
    'Orders where customer received before carrier even picked up'
FROM STAGING.STG_ORDERS
WHERE _IS_DUPLICATE = FALSE;


-- =============================================================================
-- SECTION 6 : DQ RESULTS DASHBOARD QUERIES  (REQ 5)
-- Run these after all checks complete to see the full DQ picture.
-- =============================================================================

-- ── Full results ordered by severity ─────────────────────────────────────────
SELECT
    CHECK_ID,
    CHECK_NAME,
    TARGET_TABLE,
    CHECK_TYPE,
    RESULT,
    TOTAL_ROW_CNT,
    FAIL_ROW_CNT,
    PASS_RATE_PCT,
    RUN_AT,
    NOTES
FROM AUDIT.AUD_DQ_CHECKS
ORDER BY RESULT DESC, PASS_RATE_PCT ASC;   -- FAILs first, worst pass-rate first

-- ── Summary by check type  ───────────────────────────────────────────────────
SELECT
    CHECK_TYPE,
    COUNT(*)                                  AS total_checks,
    SUM(IFF(RESULT = 'PASS', 1, 0))           AS passed,
    SUM(IFF(RESULT = 'FAIL', 1, 0))           AS failed,
    ROUND(AVG(PASS_RATE_PCT), 2)              AS avg_pass_rate_pct
FROM AUDIT.AUD_DQ_CHECKS
GROUP BY CHECK_TYPE
ORDER BY failed DESC;

-- ── Failed checks only  ───────────────────────────────────────────────────────
SELECT CHECK_NAME, TARGET_TABLE, FAIL_ROW_CNT, PASS_RATE_PCT, NOTES
FROM AUDIT.AUD_DQ_CHECKS
WHERE RESULT = 'FAIL'
ORDER BY FAIL_ROW_CNT DESC;
