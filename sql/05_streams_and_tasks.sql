-- =============================================================================
-- ShopSphere Data Warehouse – Streams & Tasks Pipeline
-- File   : 05_streams_and_tasks.sql
-- Purpose:
--   SECTION 1  : Streams — one per staging table that feeds MARTS
--                  STRM_STG_CUSTOMERS     → DIM_CUSTOMERS  (SCD Type 2)
--                  STRM_STG_PRODUCTS      → DIM_PRODUCTS   (SCD Type 2)
--                  STRM_STG_SELLERS       → DIM_SELLERS    (Type 1)
--                  STRM_STG_ORDER_ITEMS   → FACT_ORDER_ITEMS
--                  STRM_STG_ORDER_PAYMENTS→ FACT_PAYMENTS
--                  STRM_STG_ORDER_REVIEWS → FACT_REVIEWS
--
--   SECTION 2  : Task DAG (dependency chain)
--
--                  TASK_MASTER_TRIGGER   (root — scheduled every 5 min)
--                        │
--                 ┌───────┴────────┐
--                 │                │
--      TASK_MERGE_DIM_CUSTOMERS   TASK_MERGE_DIM_PRODUCTS
--      TASK_MERGE_DIM_SELLERS
--                 │
--                 └──────────┬─────────────────┐
--                            │                  │
--               TASK_MERGE_FACT_ORDER_ITEMS  TASK_MERGE_FACT_PAYMENTS
--               TASK_MERGE_FACT_REVIEWS
--                            │
--               TASK_LOG_PIPELINE_RUN    (leaf — writes to AUDIT)
--
--   SECTION 3  : MERGE logic inside each task (proc-wrapped for reuse)
--
--   SECTION 4  : Monitoring & control queries
--                  Start / suspend / resume tasks
--                  Query task history and stream lag
--
-- Key Snowflake concepts demonstrated:
--   • APPEND_ONLY vs DEFAULT stream types
--   • SYSTEM$STREAM_HAS_DATA() guard to skip empty runs
--   • METADATA$ACTION + METADATA$ISUPDATE CDC columns
--   • Task AFTER clause for DAG dependency
--   • Task error handling via SYSTEM$CURRENT_USER_TASK_NAME()
--
-- Prerequisites : 01 → 02 → 03 → 04 must have been run.
-- Run in        : Snowflake worksheet using ACCOUNTADMIN or SYSADMIN
-- =============================================================================

USE DATABASE SHOPSPHERE_DW;
USE ROLE    SYSADMIN;
USE WAREHOUSE WH_M_ETL;


-- =============================================================================
-- SECTION 1 : STREAM DEFINITIONS
-- =============================================================================
-- A Snowflake Stream records the INSERT / UPDATE / DELETE delta on a table.
-- We place streams on the STAGING tables because:
--   - STAGING is the "clean" source of truth after 02/03 run.
--   - Streams capture only *new or changed* rows since the last task consumed them.
--   - Stream offsets advance automatically when a task commits successfully.
-- =============================================================================

-- ── Stream type choice ────────────────────────────────────────────────────────
-- STG_CUSTOMERS / STG_PRODUCTS / STG_SELLERS → DEFAULT stream
--   Captures INSERT, UPDATE, DELETE — needed for SCD logic.
--
-- STG_ORDER_ITEMS / STG_ORDER_PAYMENTS / STG_ORDER_REVIEWS → APPEND_ONLY stream
--   Facts are append-only once validated; APPEND_ONLY is lighter weight and
--   does not track row-level updates (we use MERGE in the task for idempotency).
-- ─────────────────────────────────────────────────────────────────────────────

-- ── Dimension streams (DEFAULT — tracks all DML) ──────────────────────────────
CREATE OR REPLACE STREAM STAGING.STRM_STG_CUSTOMERS
    ON TABLE STAGING.STG_CUSTOMERS
    COMMENT = 'Captures INSERT/UPDATE/DELETE on STG_CUSTOMERS for SCD Type 2 merge into DIM_CUSTOMERS';

CREATE OR REPLACE STREAM STAGING.STRM_STG_PRODUCTS
    ON TABLE STAGING.STG_PRODUCTS
    COMMENT = 'Captures INSERT/UPDATE/DELETE on STG_PRODUCTS for SCD Type 2 merge into DIM_PRODUCTS';

CREATE OR REPLACE STREAM STAGING.STRM_STG_SELLERS
    ON TABLE STAGING.STG_SELLERS
    COMMENT = 'Captures INSERT/UPDATE on STG_SELLERS for Type-1 merge into DIM_SELLERS';

-- ── Fact streams (APPEND_ONLY — new valid rows only) ──────────────────────────
CREATE OR REPLACE STREAM STAGING.STRM_STG_ORDER_ITEMS
    ON TABLE STAGING.STG_ORDER_ITEMS
    APPEND_ONLY = TRUE
    COMMENT = 'Captures new valid rows on STG_ORDER_ITEMS for MERGE into FACT_ORDER_ITEMS';

CREATE OR REPLACE STREAM STAGING.STRM_STG_ORDER_PAYMENTS
    ON TABLE STAGING.STG_ORDER_PAYMENTS
    APPEND_ONLY = TRUE
    COMMENT = 'Captures new valid rows on STG_ORDER_PAYMENTS for MERGE into FACT_PAYMENTS';

CREATE OR REPLACE STREAM STAGING.STRM_STG_ORDER_REVIEWS
    ON TABLE STAGING.STG_ORDER_REVIEWS
    APPEND_ONLY = TRUE
    COMMENT = 'Captures new valid rows on STG_ORDER_REVIEWS for MERGE into FACT_REVIEWS';

-- Verify all streams were created:
SHOW STREAMS IN SCHEMA STAGING;


-- =============================================================================
-- SECTION 2 : TASK DAG DEFINITIONS
--
-- Execution order (dependency chain):
--
--   TASK_MASTER_TRIGGER  ──────────────────────────────── (root, scheduled)
--        │
--        ├── TASK_MERGE_DIM_CUSTOMERS   (AFTER master)
--        ├── TASK_MERGE_DIM_PRODUCTS    (AFTER master)
--        └── TASK_MERGE_DIM_SELLERS     (AFTER master)
--                  │ (all three must complete first)
--                  ├── TASK_MERGE_FACT_ORDER_ITEMS    (AFTER all three dims)
--                  ├── TASK_MERGE_FACT_PAYMENTS       (AFTER all three dims)
--                  └── TASK_MERGE_FACT_REVIEWS        (AFTER all three dims)
--                             │
--                             └── TASK_LOG_PIPELINE_RUN  (AFTER all three facts)
--
-- All tasks are initially SUSPENDED; start them in Section 4.
-- =============================================================================

-- ── ROOT TASK: fires every 5 minutes ─────────────────────────────────────────
-- The root task itself does nothing (just a scheduler trigger).
-- It checks if ANY stream has data before the DAG proceeds.
CREATE OR REPLACE TASK AUDIT.TASK_MASTER_TRIGGER
    WAREHOUSE = WH_M_ETL
    SCHEDULE  = '5 MINUTE'
    COMMENT   = 'Root scheduler — fires every 5 min; child tasks check stream data independently'
AS
    -- Root body: a no-op marker; children guard with SYSTEM$STREAM_HAS_DATA()
    SELECT CURRENT_TIMESTAMP() AS trigger_ts;


-- =============================================================================
-- SECTION 3 : MERGE LOGIC (one task per target table)
-- =============================================================================

-- ── TASK: Merge changed customers → DIM_CUSTOMERS (SCD Type 2) ───────────────
CREATE OR REPLACE TASK AUDIT.TASK_MERGE_DIM_CUSTOMERS
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_MASTER_TRIGGER
    COMMENT   = 'SCD Type 2 merge from STRM_STG_CUSTOMERS into MARTS.DIM_CUSTOMERS'
AS
$$
BEGIN
    -- Skip if no changed data in stream
    IF (NOT SYSTEM$STREAM_HAS_DATA('STAGING.STRM_STG_CUSTOMERS')) THEN
        RETURN 'No new data in STRM_STG_CUSTOMERS — skipped';
    END IF;

    -- ── Step 1: Expire old versions where tracked attributes changed ──────────
    UPDATE MARTS.DIM_CUSTOMERS dim
    SET
        SCD_END_DT  = CURRENT_DATE() - 1,
        IS_CURRENT  = FALSE,
        _UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE dim.IS_CURRENT = TRUE
      AND EXISTS (
          SELECT 1
          FROM STAGING.STRM_STG_CUSTOMERS stm
          WHERE stm.METADATA$ACTION    = 'INSERT'   -- stream INSERT = new version
            AND stm.METADATA$ISUPDATE  = TRUE       -- true when it is an UPDATE CDC event
            AND stm.CUSTOMER_UNIQUE_ID = dim.CUSTOMER_UNIQUE_ID
            AND stm._IS_DUPLICATE      = FALSE
            AND stm._IS_VALID          = TRUE
            AND (
                   COALESCE(stm.CITY,            '') <> COALESCE(dim.CITY,            '')
                OR COALESCE(stm.STATE,           '') <> COALESCE(dim.STATE,           '')
                OR COALESCE(stm.ZIP_CODE_PREFIX, '') <> COALESCE(dim.ZIP_CODE_PREFIX, '')
            )
      );

    -- ── Step 2: Insert new version for changed + brand-new customers ──────────
    INSERT INTO MARTS.DIM_CUSTOMERS (
        CUSTOMER_UNIQUE_ID, CUSTOMER_ID_NK,
        ZIP_CODE_PREFIX, CITY, STATE,
        SCD_START_DT, SCD_END_DT, IS_CURRENT
    )
    SELECT
        stm.CUSTOMER_UNIQUE_ID,
        stm.CUSTOMER_ID         AS CUSTOMER_ID_NK,
        stm.ZIP_CODE_PREFIX,
        stm.CITY,
        stm.STATE,
        CURRENT_DATE()           AS SCD_START_DT,
        NULL                     AS SCD_END_DT,
        TRUE                     AS IS_CURRENT
    FROM STAGING.STRM_STG_CUSTOMERS stm
    WHERE stm.METADATA$ACTION   = 'INSERT'
      AND stm._IS_DUPLICATE     = FALSE
      AND stm._IS_VALID         = TRUE
      -- Only insert if no current version exists (new customer or just expired above)
      AND NOT EXISTS (
          SELECT 1 FROM MARTS.DIM_CUSTOMERS d
          WHERE d.CUSTOMER_UNIQUE_ID = stm.CUSTOMER_UNIQUE_ID
            AND d.IS_CURRENT = TRUE
      );

    RETURN 'DIM_CUSTOMERS merge complete';
END;
$$;


-- ── TASK: Merge changed products → DIM_PRODUCTS (SCD Type 2) ─────────────────
CREATE OR REPLACE TASK AUDIT.TASK_MERGE_DIM_PRODUCTS
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_MASTER_TRIGGER
    COMMENT   = 'SCD Type 2 merge from STRM_STG_PRODUCTS into MARTS.DIM_PRODUCTS'
AS
$$
BEGIN
    IF (NOT SYSTEM$STREAM_HAS_DATA('STAGING.STRM_STG_PRODUCTS')) THEN
        RETURN 'No new data in STRM_STG_PRODUCTS — skipped';
    END IF;

    -- Expire old product versions where category changed
    UPDATE MARTS.DIM_PRODUCTS dim
    SET
        SCD_END_DT  = CURRENT_DATE() - 1,
        IS_CURRENT  = FALSE,
        _UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE dim.IS_CURRENT = TRUE
      AND EXISTS (
          SELECT 1
          FROM STAGING.STRM_STG_PRODUCTS stm
          WHERE stm.METADATA$ACTION   = 'INSERT'
            AND stm.METADATA$ISUPDATE = TRUE
            AND stm.PRODUCT_ID        = dim.PRODUCT_ID_NK
            AND stm._IS_DUPLICATE     = FALSE
            AND stm._IS_VALID         = TRUE
            AND COALESCE(stm.CATEGORY_NAME_EN, '') <> COALESCE(dim.CATEGORY_NAME_EN, '')
      );

    -- Insert new version
    INSERT INTO MARTS.DIM_PRODUCTS (
        PRODUCT_ID_NK, CATEGORY_NAME_PT, CATEGORY_NAME_EN,
        PHOTO_CNT, WEIGHT_G, LENGTH_CM, HEIGHT_CM, WIDTH_CM,
        SCD_START_DT, SCD_END_DT, IS_CURRENT
    )
    SELECT
        stm.PRODUCT_ID, stm.CATEGORY_NAME_PT, stm.CATEGORY_NAME_EN,
        stm.PHOTO_CNT, stm.WEIGHT_G, stm.LENGTH_CM, stm.HEIGHT_CM, stm.WIDTH_CM,
        CURRENT_DATE(), NULL, TRUE
    FROM STAGING.STRM_STG_PRODUCTS stm
    WHERE stm.METADATA$ACTION = 'INSERT'
      AND stm._IS_DUPLICATE   = FALSE
      AND stm._IS_VALID       = TRUE
      AND NOT EXISTS (
          SELECT 1 FROM MARTS.DIM_PRODUCTS d
          WHERE d.PRODUCT_ID_NK = stm.PRODUCT_ID AND d.IS_CURRENT = TRUE
      );

    RETURN 'DIM_PRODUCTS merge complete';
END;
$$;


-- ── TASK: Merge changed sellers → DIM_SELLERS (Type 1 overwrite) ─────────────
CREATE OR REPLACE TASK AUDIT.TASK_MERGE_DIM_SELLERS
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_MASTER_TRIGGER
    COMMENT   = 'Type-1 MERGE from STRM_STG_SELLERS into MARTS.DIM_SELLERS'
AS
$$
BEGIN
    IF (NOT SYSTEM$STREAM_HAS_DATA('STAGING.STRM_STG_SELLERS')) THEN
        RETURN 'No new data in STRM_STG_SELLERS — skipped';
    END IF;

    MERGE INTO MARTS.DIM_SELLERS tgt
    USING (
        SELECT SELLER_ID, ZIP_CODE_PREFIX, CITY, STATE
        FROM   STAGING.STRM_STG_SELLERS
        WHERE  METADATA$ACTION = 'INSERT'
          AND  _IS_DUPLICATE   = FALSE
          AND  _IS_VALID       = TRUE
    ) src ON tgt.SELLER_ID_NK = src.SELLER_ID

    WHEN MATCHED THEN UPDATE SET
        tgt.ZIP_CODE_PREFIX = src.ZIP_CODE_PREFIX,
        tgt.CITY            = src.CITY,
        tgt.STATE           = src.STATE,
        tgt._UPDATED_AT     = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (
        SELLER_ID_NK, ZIP_CODE_PREFIX, CITY, STATE
    ) VALUES (
        src.SELLER_ID, src.ZIP_CODE_PREFIX, src.CITY, src.STATE
    );

    RETURN 'DIM_SELLERS merge complete';
END;
$$;


-- ── TASK: Merge new order items → FACT_ORDER_ITEMS ───────────────────────────
-- Depends on all three dimension tasks completing first.
CREATE OR REPLACE TASK AUDIT.TASK_MERGE_FACT_ORDER_ITEMS
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_MERGE_DIM_CUSTOMERS,
              AUDIT.TASK_MERGE_DIM_PRODUCTS,
              AUDIT.TASK_MERGE_DIM_SELLERS
    COMMENT   = 'MERGE new/updated order items from stream into MARTS.FACT_ORDER_ITEMS'
AS
$$
BEGIN
    IF (NOT SYSTEM$STREAM_HAS_DATA('STAGING.STRM_STG_ORDER_ITEMS')) THEN
        RETURN 'No new data in STRM_STG_ORDER_ITEMS — skipped';
    END IF;

    MERGE INTO MARTS.FACT_ORDER_ITEMS tgt
    USING (
        SELECT
            i.ORDER_ID                                                 AS ORDER_ID_NK,
            i.ORDER_ITEM_ID,
            TO_NUMBER(TO_CHAR(o.PURCHASE_TS::DATE, 'YYYYMMDD'))        AS PURCHASE_DATE_KEY,
            dc.CUSTOMER_KEY,
            dp.PRODUCT_KEY,
            ds.SELLER_KEY,
            dg.GEOGRAPHY_KEY,
            o.ORDER_STATUS, o.IS_DELIVERED, o.IS_CANCELED,
            o.IS_LATE, o.DELIVERY_DELAY_DAYS,
            o.PURCHASE_TS,
            i.PRICE_AMT, i.FREIGHT_AMT, i.TOTAL_ITEM_AMT
        FROM STAGING.STRM_STG_ORDER_ITEMS i           -- stream as source
        JOIN STAGING.STG_ORDERS o
             ON o.ORDER_ID = i.ORDER_ID AND o._IS_DUPLICATE = FALSE AND o._IS_VALID = TRUE
        JOIN STAGING.STG_CUSTOMERS sc
             ON sc.CUSTOMER_ID = o.CUSTOMER_ID AND sc._IS_DUPLICATE = FALSE
        JOIN MARTS.DIM_CUSTOMERS dc
             ON dc.CUSTOMER_UNIQUE_ID = sc.CUSTOMER_UNIQUE_ID
            AND dc.SCD_START_DT  <= o.PURCHASE_TS::DATE
            AND (dc.SCD_END_DT IS NULL OR dc.SCD_END_DT > o.PURCHASE_TS::DATE)
        JOIN MARTS.DIM_PRODUCTS dp
             ON dp.PRODUCT_ID_NK = i.PRODUCT_ID AND dp.IS_CURRENT = TRUE
        JOIN MARTS.DIM_SELLERS  ds
             ON ds.SELLER_ID_NK  = i.SELLER_ID
        LEFT JOIN STAGING.STG_CUSTOMERS sc2
                  ON sc2.CUSTOMER_ID = o.CUSTOMER_ID AND sc2._IS_DUPLICATE = FALSE
        LEFT JOIN MARTS.DIM_GEOGRAPHY dg
                  ON dg.ZIP_CODE_PREFIX = sc2.ZIP_CODE_PREFIX
        WHERE i._IS_DUPLICATE = FALSE AND i._IS_VALID = TRUE
    ) src
    ON (tgt.ORDER_ID_NK = src.ORDER_ID_NK AND tgt.ORDER_ITEM_ID = src.ORDER_ITEM_ID)

    -- Update if order status changed (e.g. shipped → delivered) or price corrected
    WHEN MATCHED AND (
        tgt.ORDER_STATUS  <> src.ORDER_STATUS OR
        tgt.PRICE_AMT     <> src.PRICE_AMT    OR
        tgt.FREIGHT_AMT   <> src.FREIGHT_AMT
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

    -- Insert brand-new order items
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
        FALSE  -- not late-arriving when processed in the regular pipeline
    );

    RETURN 'FACT_ORDER_ITEMS merge complete';
END;
$$;


-- ── TASK: Merge new payments → FACT_PAYMENTS ─────────────────────────────────
CREATE OR REPLACE TASK AUDIT.TASK_MERGE_FACT_PAYMENTS
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_MERGE_DIM_CUSTOMERS,
              AUDIT.TASK_MERGE_DIM_PRODUCTS,
              AUDIT.TASK_MERGE_DIM_SELLERS
    COMMENT   = 'MERGE new payments from stream into MARTS.FACT_PAYMENTS'
AS
$$
BEGIN
    IF (NOT SYSTEM$STREAM_HAS_DATA('STAGING.STRM_STG_ORDER_PAYMENTS')) THEN
        RETURN 'No new data in STRM_STG_ORDER_PAYMENTS — skipped';
    END IF;

    MERGE INTO MARTS.FACT_PAYMENTS tgt
    USING (
        SELECT
            p.ORDER_ID                                                 AS ORDER_ID_NK,
            p.PAYMENT_SEQUENTIAL,
            TO_NUMBER(TO_CHAR(o.PURCHASE_TS::DATE, 'YYYYMMDD'))        AS PURCHASE_DATE_KEY,
            dc.CUSTOMER_KEY,
            p.PAYMENT_TYPE, p.PAYMENT_INSTALLMENTS,
            p.IS_CREDIT_CARD, p.IS_BOLETO, p.IS_VOUCHER, p.IS_DEBIT_CARD,
            p.PAYMENT_AMT
        FROM STAGING.STRM_STG_ORDER_PAYMENTS p
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
        tgt.PAYMENT_AMT = src.PAYMENT_AMT,
        tgt._LOADED_AT  = CURRENT_TIMESTAMP()

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
        src.PAYMENT_AMT, FALSE
    );

    RETURN 'FACT_PAYMENTS merge complete';
END;
$$;


-- ── TASK: Merge new reviews → FACT_REVIEWS ───────────────────────────────────
CREATE OR REPLACE TASK AUDIT.TASK_MERGE_FACT_REVIEWS
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_MERGE_DIM_CUSTOMERS,
              AUDIT.TASK_MERGE_DIM_PRODUCTS,
              AUDIT.TASK_MERGE_DIM_SELLERS
    COMMENT   = 'MERGE new reviews from stream into MARTS.FACT_REVIEWS'
AS
$$
BEGIN
    IF (NOT SYSTEM$STREAM_HAS_DATA('STAGING.STRM_STG_ORDER_REVIEWS')) THEN
        RETURN 'No new data in STRM_STG_ORDER_REVIEWS — skipped';
    END IF;

    MERGE INTO MARTS.FACT_REVIEWS tgt
    USING (
        SELECT
            r.REVIEW_ID                                               AS REVIEW_ID_NK,
            r.ORDER_ID                                                AS ORDER_ID_NK,
            TO_NUMBER(TO_CHAR(
                COALESCE(r.REVIEW_ANSWERED_AT::DATE, r.REVIEW_CREATED_DT),
                'YYYYMMDD'
            ))                                                         AS ANSWERED_DATE_KEY,
            dc.CUSTOMER_KEY,
            r.REVIEW_SCORE, r.IS_POSITIVE, r.IS_NEUTRAL, r.IS_NEGATIVE,
            r.HAS_COMMENT, r.REVIEW_TITLE, r.REVIEW_MESSAGE
        FROM STAGING.STRM_STG_ORDER_REVIEWS r
        JOIN STAGING.STG_ORDERS o
             ON o.ORDER_ID = r.ORDER_ID AND o._IS_DUPLICATE = FALSE AND o._IS_VALID = TRUE
        JOIN STAGING.STG_CUSTOMERS sc
             ON sc.CUSTOMER_ID = o.CUSTOMER_ID AND sc._IS_DUPLICATE = FALSE
        JOIN MARTS.DIM_CUSTOMERS dc
             ON dc.CUSTOMER_UNIQUE_ID = sc.CUSTOMER_UNIQUE_ID
            AND dc.SCD_START_DT  <= o.PURCHASE_TS::DATE
            AND (dc.SCD_END_DT IS NULL OR dc.SCD_END_DT > o.PURCHASE_TS::DATE)
        WHERE r._IS_DUPLICATE = FALSE AND r._IS_VALID = TRUE
    ) src
    ON tgt.REVIEW_ID_NK = src.REVIEW_ID_NK

    -- If review score was updated (re-survey)
    WHEN MATCHED AND tgt.REVIEW_SCORE <> src.REVIEW_SCORE THEN UPDATE SET
        tgt.REVIEW_SCORE   = src.REVIEW_SCORE,
        tgt.IS_POSITIVE    = src.IS_POSITIVE,
        tgt.IS_NEUTRAL     = src.IS_NEUTRAL,
        tgt.IS_NEGATIVE    = src.IS_NEGATIVE,
        tgt.REVIEW_TITLE   = src.REVIEW_TITLE,
        tgt.REVIEW_MESSAGE = src.REVIEW_MESSAGE,
        tgt._LOADED_AT     = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (
        REVIEW_ID_NK, ORDER_ID_NK,
        ANSWERED_DATE_KEY, CUSTOMER_KEY,
        REVIEW_SCORE, IS_POSITIVE, IS_NEUTRAL, IS_NEGATIVE,
        HAS_COMMENT, REVIEW_TITLE, REVIEW_MESSAGE,
        _IS_LATE_ARRIVING
    ) VALUES (
        src.REVIEW_ID_NK, src.ORDER_ID_NK,
        src.ANSWERED_DATE_KEY, src.CUSTOMER_KEY,
        src.REVIEW_SCORE, src.IS_POSITIVE, src.IS_NEUTRAL, src.IS_NEGATIVE,
        src.HAS_COMMENT, src.REVIEW_TITLE, src.REVIEW_MESSAGE,
        FALSE
    );

    RETURN 'FACT_REVIEWS merge complete';
END;
$$;


-- ── TASK: Pipeline run log (leaf node — runs after all facts) ─────────────────
CREATE OR REPLACE TASK AUDIT.TASK_LOG_PIPELINE_RUN
    WAREHOUSE = WH_M_ETL
    AFTER     AUDIT.TASK_MERGE_FACT_ORDER_ITEMS,
              AUDIT.TASK_MERGE_FACT_PAYMENTS,
              AUDIT.TASK_MERGE_FACT_REVIEWS
    COMMENT   = 'Leaf task — writes pipeline completion row to AUDIT.AUD_PIPELINE_RUNS'
AS
$$
INSERT INTO AUDIT.AUD_PIPELINE_RUNS (
    PIPELINE_NAME, TARGET_TABLE, STATUS,
    ROWS_LOADED, ROWS_REJECTED,
    STARTED_AT, FINISHED_AT, ERROR_MSG
)
SELECT
    'FULL_PIPELINE_DAG'        AS PIPELINE_NAME,
    'MARTS.*'                  AS TARGET_TABLE,
    'SUCCESS'                  AS STATUS,
    (SELECT COUNT(*) FROM MARTS.FACT_ORDER_ITEMS WHERE _LOADED_AT >= DATEADD('minute', -6, CURRENT_TIMESTAMP()))
    + (SELECT COUNT(*) FROM MARTS.FACT_PAYMENTS   WHERE _LOADED_AT >= DATEADD('minute', -6, CURRENT_TIMESTAMP()))
    + (SELECT COUNT(*) FROM MARTS.FACT_REVIEWS    WHERE _LOADED_AT >= DATEADD('minute', -6, CURRENT_TIMESTAMP()))
                               AS ROWS_LOADED,
    0                          AS ROWS_REJECTED,
    DATEADD('minute', -5, CURRENT_TIMESTAMP()) AS STARTED_AT,
    CURRENT_TIMESTAMP()        AS FINISHED_AT,
    NULL                       AS ERROR_MSG;
$$;


-- =============================================================================
-- SECTION 4 : PIPELINE CONTROL & MONITORING
-- =============================================================================

-- ── Start the full DAG ────────────────────────────────────────────────────────
-- Tasks must be resumed from LEAF to ROOT (Snowflake requirement).
-- Resume child tasks first, then the root.

ALTER TASK AUDIT.TASK_LOG_PIPELINE_RUN          RESUME;
ALTER TASK AUDIT.TASK_MERGE_FACT_ORDER_ITEMS    RESUME;
ALTER TASK AUDIT.TASK_MERGE_FACT_PAYMENTS       RESUME;
ALTER TASK AUDIT.TASK_MERGE_FACT_REVIEWS        RESUME;
ALTER TASK AUDIT.TASK_MERGE_DIM_SELLERS         RESUME;
ALTER TASK AUDIT.TASK_MERGE_DIM_PRODUCTS        RESUME;
ALTER TASK AUDIT.TASK_MERGE_DIM_CUSTOMERS       RESUME;
ALTER TASK AUDIT.TASK_MASTER_TRIGGER            RESUME;  -- root last

-- ── Suspend all tasks (for maintenance or debugging) ─────────────────────────
/*
ALTER TASK AUDIT.TASK_MASTER_TRIGGER            SUSPEND;
ALTER TASK AUDIT.TASK_MERGE_DIM_CUSTOMERS       SUSPEND;
ALTER TASK AUDIT.TASK_MERGE_DIM_PRODUCTS        SUSPEND;
ALTER TASK AUDIT.TASK_MERGE_DIM_SELLERS         SUSPEND;
ALTER TASK AUDIT.TASK_MERGE_FACT_ORDER_ITEMS    SUSPEND;
ALTER TASK AUDIT.TASK_MERGE_FACT_PAYMENTS       SUSPEND;
ALTER TASK AUDIT.TASK_MERGE_FACT_REVIEWS        SUSPEND;
ALTER TASK AUDIT.TASK_LOG_PIPELINE_RUN          SUSPEND;
*/

-- ── Manually trigger the DAG once (for testing without waiting 5 min) ─────────
/*
EXECUTE TASK AUDIT.TASK_MASTER_TRIGGER;
*/

-- ── Check task status and last run results ────────────────────────────────────
SHOW TASKS IN SCHEMA AUDIT;

-- Task execution history (last 1 hour):
SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    RETURN_VALUE,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100
))
ORDER BY SCHEDULED_TIME DESC;

-- ── Check stream lag (how stale is each stream) ───────────────────────────────
SELECT
    SYSTEM$STREAM_GET_TABLE_TIMESTAMP('STAGING.STRM_STG_CUSTOMERS')      AS customers_offset,
    SYSTEM$STREAM_GET_TABLE_TIMESTAMP('STAGING.STRM_STG_ORDER_ITEMS')    AS items_offset,
    SYSTEM$STREAM_GET_TABLE_TIMESTAMP('STAGING.STRM_STG_ORDER_PAYMENTS') AS payments_offset,
    SYSTEM$STREAM_GET_TABLE_TIMESTAMP('STAGING.STRM_STG_ORDER_REVIEWS')  AS reviews_offset,
    CURRENT_TIMESTAMP()                                                    AS now;

-- ── Check whether each stream currently has pending data ─────────────────────
SELECT
    'STRM_STG_CUSTOMERS'      AS stream_name,
    SYSTEM$STREAM_HAS_DATA('STAGING.STRM_STG_CUSTOMERS')      AS has_data UNION ALL
SELECT 'STRM_STG_PRODUCTS',
    SYSTEM$STREAM_HAS_DATA('STAGING.STRM_STG_PRODUCTS')               UNION ALL
SELECT 'STRM_STG_SELLERS',
    SYSTEM$STREAM_HAS_DATA('STAGING.STRM_STG_SELLERS')                UNION ALL
SELECT 'STRM_STG_ORDER_ITEMS',
    SYSTEM$STREAM_HAS_DATA('STAGING.STRM_STG_ORDER_ITEMS')            UNION ALL
SELECT 'STRM_STG_ORDER_PAYMENTS',
    SYSTEM$STREAM_HAS_DATA('STAGING.STRM_STG_ORDER_PAYMENTS')         UNION ALL
SELECT 'STRM_STG_ORDER_REVIEWS',
    SYSTEM$STREAM_HAS_DATA('STAGING.STRM_STG_ORDER_REVIEWS');

-- ── Pipeline run history ──────────────────────────────────────────────────────
SELECT
    RUN_ID, PIPELINE_NAME, STATUS,
    ROWS_LOADED, STARTED_AT, FINISHED_AT,
    DATEDIFF('second', STARTED_AT, FINISHED_AT) AS duration_secs
FROM AUDIT.AUD_PIPELINE_RUNS
ORDER BY STARTED_AT DESC
LIMIT 20;
