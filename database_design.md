# Database Design Document
## ShopSphere – Snowflake Data Warehouse

---

## 1. Naming Conventions

All names follow a single consistent rule set across every layer. Consistency removes ambiguity and makes SQL readable without documentation.

### 1.1 General Rules

| Rule | Detail |
|---|---|
| **Case** | All identifiers in `UPPER_SNAKE_CASE` (Snowflake stores unquoted identifiers in uppercase by default) |
| **Word separator** | Underscore `_` only — no hyphens, spaces, or camelCase |
| **Max length** | 40 characters (Snowflake limit is 255, but short names are faster to read and type) |
| **Abbreviations** | Avoid unless universally understood (e.g., `ID`, `TS`, `QTY`, `AMT`, `DT`) |
| **No reserved words** | Never use SQL reserved words as identifiers (`DATE`, `ORDER`, `VALUE`, `TYPE`) |
| **No leading numbers** | Identifiers must start with a letter or underscore |

---

### 1.2 Database Names

Pattern: `<PROJECT>_<PURPOSE>`

| Database | Purpose |
|---|---|
| `SHOPSPHERE_DW` | The single production data warehouse database |

---

### 1.3 Schema Names

Pattern: `<LAYER>` — one schema per architectural layer inside `SHOPSPHERE_DW`.

| Schema | Layer | Purpose |
|---|---|---|
| `RAW` | Ingestion / Landing | As-is data copied from source files. No transformations. Append-only. |
| `STAGING` | Transformation | Cleaned, deduplicated, validated data. Intermediate layer. |
| `MARTS` | Analytics / Presentation | Star-schema fact and dimension tables for BI reporting. |
| `COMMON` | Shared / Reference | Lookup tables and utility views shared across layers. |
| `AUDIT` | Monitoring | Pipeline run logs, data quality check results, row counts. |

Full reference pattern: `SHOPSPHERE_DW.<SCHEMA>.<TABLE>`

Example: `SHOPSPHERE_DW.MARTS.FACT_ORDER_ITEMS`

---

### 1.4 Table Names

Pattern: `<PREFIX>_<ENTITY>`

| Prefix | Schema | Meaning | Example |
|---|---|---|---|
| `RAW_` | `RAW` | Direct source copy | `RAW_ORDERS` |
| `STG_` | `STAGING` | Cleaned/transformed | `STG_CUSTOMERS` |
| `FACT_` | `MARTS` | Fact table (measures) | `FACT_ORDER_ITEMS` |
| `DIM_` | `MARTS` | Dimension table (context) | `DIM_CUSTOMERS` |
| `LKP_` | `COMMON` | Lookup / reference | `LKP_CATEGORY_NAMES` |
| `AUD_` | `AUDIT` | Audit / monitoring | `AUD_PIPELINE_RUNS` |

---

### 1.5 Column Names

| Pattern | Rule | Example |
|---|---|---|
| Primary key | `<TABLE_SINGULAR>_KEY` (surrogate) or `<ENTITY>_ID` (natural) | `ORDER_ITEM_KEY`, `ORDER_ID` |
| Foreign key | Same name as the referenced PK column | `CUSTOMER_KEY`, `PRODUCT_ID` |
| Timestamps | Suffix `_AT` for exact datetime, `_DT` for date-only | `PURCHASED_AT`, `ORDER_DT` |
| Boolean flags | Prefix `IS_` or `HAS_` | `IS_DELIVERED`, `HAS_REFUND` |
| Amounts / money | Suffix `_AMT` | `PAYMENT_AMT`, `FREIGHT_AMT` |
| Quantities | Suffix `_QTY` | `ITEM_QTY` |
| Counts | Suffix `_CNT` | `PHOTO_CNT` |
| Scores / ratings | Suffix `_SCORE` or `_RATING` | `REVIEW_SCORE` |
| Audit columns | `_LOADED_AT`, `_UPDATED_AT`, `_SOURCE_FILE` | present on every RAW table |

---

### 1.6 Other Object Names

| Object | Pattern | Example |
|---|---|---|
| View | `VW_<ENTITY>` | `VW_CUSTOMER_ORDERS` |
| Stage (internal) | `STG_<SOURCE>_STAGE` | `STG_OLIST_STAGE` |
| File format | `FF_<FORMAT>` | `FF_CSV_HEADER` |
| Stream | `STRM_<TABLE>` | `STRM_RAW_ORDERS` |
| Task | `TASK_<ACTION>_<TABLE>` | `TASK_LOAD_RAW_ORDERS` |
| Pipe | `PIPE_<TABLE>` | `PIPE_RAW_EVENTS` |
| Warehouse | `WH_<SIZE>_<PURPOSE>` | `WH_XS_ADHOC`, `WH_M_ETL` |
| Role | `ROLE_<TEAM>` | `ROLE_ANALYST`, `ROLE_ENGINEER` |

---

## 2. Database & Schema Structure

```
SHOPSPHERE_DW
├── RAW
│   ├── RAW_CUSTOMERS
│   ├── RAW_GEOLOCATION
│   ├── RAW_ORDERS
│   ├── RAW_ORDER_ITEMS
│   ├── RAW_ORDER_PAYMENTS
│   ├── RAW_ORDER_REVIEWS
│   ├── RAW_PRODUCTS
│   └── RAW_SELLERS
│
├── STAGING
│   ├── STG_CUSTOMERS
│   ├── STG_GEOLOCATION
│   ├── STG_ORDERS
│   ├── STG_ORDER_ITEMS
│   ├── STG_ORDER_PAYMENTS
│   ├── STG_ORDER_REVIEWS
│   ├── STG_PRODUCTS
│   └── STG_SELLERS
│
├── MARTS
│   ├── FACT_ORDER_ITEMS        ← primary fact (grain: order line item)
│   ├── FACT_PAYMENTS           ← payment fact (grain: payment entry)
│   ├── FACT_REVIEWS            ← review fact (grain: review)
│   ├── DIM_CUSTOMERS           ← SCD Type 2
│   ├── DIM_PRODUCTS            ← SCD Type 2
│   ├── DIM_SELLERS
│   ├── DIM_GEOGRAPHY
│   └── DIM_DATE
│
├── COMMON
│   └── LKP_CATEGORY_NAMES
│
└── AUDIT
    ├── AUD_PIPELINE_RUNS
    └── AUD_DQ_CHECKS
```

---

## 3. Table Definitions

---

### 3.1 RAW Schema — Landing Layer

> Rules: Columns typed as VARCHAR / VARIANT to accept any source value. No NOT NULL constraints. Every table adds three audit columns: `_LOADED_AT`, `_SOURCE_FILE`, `_ROW_HASH`.

---

#### `RAW.RAW_CUSTOMERS`
*Grain: one row per source record (customer_id)*

| Column | Data Type | Notes |
|---|---|---|
| `CUSTOMER_ID` | VARCHAR(50) | Natural key from source |
| `CUSTOMER_UNIQUE_ID` | VARCHAR(50) | True person-level identifier |
| `ZIP_CODE_PREFIX` | VARCHAR(10) | 5-digit ZIP prefix |
| `CITY` | VARCHAR(100) | |
| `STATE` | VARCHAR(10) | 2-letter state code |
| `_LOADED_AT` | TIMESTAMP_NTZ | Set by COPY INTO / Snowpipe |
| `_SOURCE_FILE` | VARCHAR(500) | Metadata column: `METADATA$FILENAME` |
| `_ROW_HASH` | VARCHAR(64) | SHA2 of all source columns for dedup |

---

#### `RAW.RAW_GEOLOCATION`
*Grain: one row per GPS coordinate sample for a ZIP prefix*

| Column | Data Type | Notes |
|---|---|---|
| `ZIP_CODE_PREFIX` | VARCHAR(10) | Non-unique; many rows per ZIP |
| `LAT` | FLOAT | Latitude |
| `LNG` | FLOAT | Longitude |
| `CITY` | VARCHAR(100) | |
| `STATE` | VARCHAR(10) | |
| `_LOADED_AT` | TIMESTAMP_NTZ | |
| `_SOURCE_FILE` | VARCHAR(500) | |
| `_ROW_HASH` | VARCHAR(64) | |

---

#### `RAW.RAW_ORDERS`
*Grain: one row per order*

| Column | Data Type | Notes |
|---|---|---|
| `ORDER_ID` | VARCHAR(50) | Natural key |
| `CUSTOMER_ID` | VARCHAR(50) | FK to RAW_CUSTOMERS |
| `ORDER_STATUS` | VARCHAR(30) | delivered / shipped / canceled / etc. |
| `PURCHASE_TS` | TIMESTAMP_NTZ | order_purchase_timestamp |
| `APPROVED_AT` | TIMESTAMP_NTZ | order_approved_at |
| `DELIVERED_CARRIER_AT` | TIMESTAMP_NTZ | order_delivered_carrier_date |
| `DELIVERED_CUSTOMER_AT` | TIMESTAMP_NTZ | order_delivered_customer_date |
| `ESTIMATED_DELIVERY_DT` | DATE | order_estimated_delivery_date |
| `_LOADED_AT` | TIMESTAMP_NTZ | |
| `_SOURCE_FILE` | VARCHAR(500) | |
| `_ROW_HASH` | VARCHAR(64) | |

---

#### `RAW.RAW_ORDER_ITEMS`
*Grain: one row per line item within an order*

| Column | Data Type | Notes |
|---|---|---|
| `ORDER_ID` | VARCHAR(50) | FK to RAW_ORDERS |
| `ORDER_ITEM_ID` | NUMBER(5,0) | Line sequence (1, 2, 3…) |
| `PRODUCT_ID` | VARCHAR(50) | FK to RAW_PRODUCTS |
| `SELLER_ID` | VARCHAR(50) | FK to RAW_SELLERS |
| `SHIPPING_LIMIT_AT` | TIMESTAMP_NTZ | |
| `PRICE_AMT` | NUMBER(12,2) | Item price excl. freight |
| `FREIGHT_AMT` | NUMBER(12,2) | Freight cost for this item |
| `_LOADED_AT` | TIMESTAMP_NTZ | |
| `_SOURCE_FILE` | VARCHAR(500) | |
| `_ROW_HASH` | VARCHAR(64) | |

---

#### `RAW.RAW_ORDER_PAYMENTS`
*Grain: one row per payment entry (order + payment_sequential)*

| Column | Data Type | Notes |
|---|---|---|
| `ORDER_ID` | VARCHAR(50) | FK to RAW_ORDERS |
| `PAYMENT_SEQUENTIAL` | NUMBER(5,0) | 1 = primary, 2 = secondary, etc. |
| `PAYMENT_TYPE` | VARCHAR(30) | credit_card / boleto / voucher / debit_card |
| `PAYMENT_INSTALLMENTS` | NUMBER(5,0) | Number of installments |
| `PAYMENT_AMT` | NUMBER(12,2) | Amount for this payment entry |
| `_LOADED_AT` | TIMESTAMP_NTZ | |
| `_SOURCE_FILE` | VARCHAR(500) | |
| `_ROW_HASH` | VARCHAR(64) | |

---

#### `RAW.RAW_ORDER_REVIEWS`
*Grain: one row per review submission*

| Column | Data Type | Notes |
|---|---|---|
| `REVIEW_ID` | VARCHAR(50) | Natural key |
| `ORDER_ID` | VARCHAR(50) | FK to RAW_ORDERS |
| `REVIEW_SCORE` | NUMBER(1,0) | 1–5 star rating |
| `REVIEW_TITLE` | VARCHAR(500) | Optional |
| `REVIEW_MESSAGE` | VARCHAR(5000) | Optional free text |
| `REVIEW_CREATED_DT` | DATE | Date the survey was sent |
| `REVIEW_ANSWERED_AT` | TIMESTAMP_NTZ | When the customer submitted |
| `_LOADED_AT` | TIMESTAMP_NTZ | |
| `_SOURCE_FILE` | VARCHAR(500) | |
| `_ROW_HASH` | VARCHAR(64) | |

---

#### `RAW.RAW_PRODUCTS`
*Grain: one row per product SKU*

| Column | Data Type | Notes |
|---|---|---|
| `PRODUCT_ID` | VARCHAR(50) | Natural key |
| `CATEGORY_NAME_PT` | VARCHAR(100) | Portuguese category name |
| `PRODUCT_NAME_LEN` | NUMBER(5,0) | Character count of name |
| `PRODUCT_DESC_LEN` | NUMBER(7,0) | Character count of description |
| `PHOTO_CNT` | NUMBER(5,0) | Number of product photos |
| `WEIGHT_G` | NUMBER(10,2) | Weight in grams |
| `LENGTH_CM` | NUMBER(8,2) | |
| `HEIGHT_CM` | NUMBER(8,2) | |
| `WIDTH_CM` | NUMBER(8,2) | |
| `_LOADED_AT` | TIMESTAMP_NTZ | |
| `_SOURCE_FILE` | VARCHAR(500) | |
| `_ROW_HASH` | VARCHAR(64) | |

---

#### `RAW.RAW_SELLERS`
*Grain: one row per seller*

| Column | Data Type | Notes |
|---|---|---|
| `SELLER_ID` | VARCHAR(50) | Natural key |
| `ZIP_CODE_PREFIX` | VARCHAR(10) | |
| `CITY` | VARCHAR(100) | |
| `STATE` | VARCHAR(10) | |
| `_LOADED_AT` | TIMESTAMP_NTZ | |
| `_SOURCE_FILE` | VARCHAR(500) | |
| `_ROW_HASH` | VARCHAR(64) | |

---

### 3.2 STAGING Schema — Transformation Layer

> Rules: Cleaned data. Deduplication applied. NOT NULL on key columns. Source natural keys preserved. No surrogate keys yet (assigned in MARTS). Audit columns track lineage.

---

#### `STAGING.STG_CUSTOMERS`
*Grain: one row per customer_id (order-scoped identity)*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `CUSTOMER_ID` | VARCHAR(50) | NOT NULL | Natural key from source |
| `CUSTOMER_UNIQUE_ID` | VARCHAR(50) | NOT NULL | True person key |
| `ZIP_CODE_PREFIX` | VARCHAR(10) | | |
| `CITY` | VARCHAR(100) | | Trimmed, lowercased |
| `STATE` | VARCHAR(10) | | Uppercased |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |
| `_UPDATED_AT` | TIMESTAMP_NTZ | NOT NULL | |
| `_IS_DUPLICATE` | BOOLEAN | NOT NULL | TRUE if duplicate row flagged |

---

#### `STAGING.STG_GEOLOCATION`
*Grain: one row per ZIP code prefix (aggregated centroid)*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `ZIP_CODE_PREFIX` | VARCHAR(10) | NOT NULL | Deduplicated; one row per ZIP |
| `AVG_LAT` | FLOAT | | Average latitude |
| `AVG_LNG` | FLOAT | | Average longitude |
| `CITY` | VARCHAR(100) | | Most-frequent city for this ZIP |
| `STATE` | VARCHAR(10) | | |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

#### `STAGING.STG_ORDERS`
*Grain: one row per order*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `ORDER_ID` | VARCHAR(50) | NOT NULL | |
| `CUSTOMER_ID` | VARCHAR(50) | NOT NULL | |
| `ORDER_STATUS` | VARCHAR(30) | NOT NULL | |
| `PURCHASE_TS` | TIMESTAMP_NTZ | NOT NULL | |
| `APPROVED_AT` | TIMESTAMP_NTZ | | Nullable (unpaid orders) |
| `DELIVERED_CARRIER_AT` | TIMESTAMP_NTZ | | |
| `DELIVERED_CUSTOMER_AT` | TIMESTAMP_NTZ | | |
| `ESTIMATED_DELIVERY_DT` | DATE | | |
| `IS_DELIVERED` | BOOLEAN | NOT NULL | Derived: STATUS = 'delivered' |
| `IS_CANCELED` | BOOLEAN | NOT NULL | Derived: STATUS = 'canceled' |
| `DELIVERY_DELAY_DAYS` | NUMBER(5,0) | | DELIVERED_CUSTOMER_AT - ESTIMATED_DELIVERY_DT |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |
| `_UPDATED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

#### `STAGING.STG_ORDER_ITEMS`
*Grain: one row per line item within an order*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `ORDER_ID` | VARCHAR(50) | NOT NULL | |
| `ORDER_ITEM_ID` | NUMBER(5,0) | NOT NULL | |
| `PRODUCT_ID` | VARCHAR(50) | NOT NULL | |
| `SELLER_ID` | VARCHAR(50) | NOT NULL | |
| `SHIPPING_LIMIT_AT` | TIMESTAMP_NTZ | | |
| `PRICE_AMT` | NUMBER(12,2) | NOT NULL | |
| `FREIGHT_AMT` | NUMBER(12,2) | NOT NULL | |
| `TOTAL_ITEM_AMT` | NUMBER(12,2) | NOT NULL | PRICE_AMT + FREIGHT_AMT |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

#### `STAGING.STG_ORDER_PAYMENTS`
*Grain: one row per payment entry (order + sequential)*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `ORDER_ID` | VARCHAR(50) | NOT NULL | |
| `PAYMENT_SEQUENTIAL` | NUMBER(5,0) | NOT NULL | |
| `PAYMENT_TYPE` | VARCHAR(30) | NOT NULL | |
| `PAYMENT_INSTALLMENTS` | NUMBER(5,0) | NOT NULL | |
| `PAYMENT_AMT` | NUMBER(12,2) | NOT NULL | |
| `IS_VOUCHER` | BOOLEAN | NOT NULL | Derived: TYPE = 'voucher' |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

#### `STAGING.STG_ORDER_REVIEWS`
*Grain: one row per review (latest review per order kept on dedup)*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `REVIEW_ID` | VARCHAR(50) | NOT NULL | |
| `ORDER_ID` | VARCHAR(50) | NOT NULL | |
| `REVIEW_SCORE` | NUMBER(1,0) | NOT NULL | |
| `REVIEW_TITLE` | VARCHAR(500) | | |
| `REVIEW_MESSAGE` | VARCHAR(5000) | | |
| `REVIEW_CREATED_DT` | DATE | NOT NULL | |
| `REVIEW_ANSWERED_AT` | TIMESTAMP_NTZ | | |
| `IS_POSITIVE` | BOOLEAN | NOT NULL | Derived: SCORE >= 4 |
| `IS_NEGATIVE` | BOOLEAN | NOT NULL | Derived: SCORE <= 2 |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

#### `STAGING.STG_PRODUCTS`
*Grain: one row per product SKU*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `PRODUCT_ID` | VARCHAR(50) | NOT NULL | |
| `CATEGORY_NAME_PT` | VARCHAR(100) | | |
| `CATEGORY_NAME_EN` | VARCHAR(100) | | Joined from LKP_CATEGORY_NAMES |
| `PHOTO_CNT` | NUMBER(5,0) | | |
| `WEIGHT_G` | NUMBER(10,2) | | |
| `LENGTH_CM` | NUMBER(8,2) | | |
| `HEIGHT_CM` | NUMBER(8,2) | | |
| `WIDTH_CM` | NUMBER(8,2) | | |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |
| `_UPDATED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

#### `STAGING.STG_SELLERS`
*Grain: one row per seller*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `SELLER_ID` | VARCHAR(50) | NOT NULL | |
| `ZIP_CODE_PREFIX` | VARCHAR(10) | | |
| `CITY` | VARCHAR(100) | | Trimmed, lowercased |
| `STATE` | VARCHAR(10) | | Uppercased |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

### 3.3 COMMON Schema — Reference / Lookup

---

#### `COMMON.LKP_CATEGORY_NAMES`
*Grain: one row per product category*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `CATEGORY_NAME_PT` | VARCHAR(100) | NOT NULL | PK — Portuguese name |
| `CATEGORY_NAME_EN` | VARCHAR(100) | NOT NULL | English translation |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

### 3.4 MARTS Schema — Analytics / Star Schema

> Rules: Surrogate keys (`NUMBER AUTOINCREMENT`) as PKs. Natural keys preserved as `_NK` columns. SCD Type 2 columns on `DIM_CUSTOMERS` and `DIM_PRODUCTS`. All foreign keys reference `_KEY` columns.

---

#### `MARTS.DIM_DATE`
*Grain: one row per calendar date*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `DATE_KEY` | NUMBER(8,0) | PK | Integer: YYYYMMDD |
| `FULL_DATE` | DATE | NOT NULL UNIQUE | |
| `DAY_OF_WEEK` | NUMBER(1,0) | NOT NULL | 1=Mon … 7=Sun |
| `DAY_NAME` | VARCHAR(10) | NOT NULL | Monday … Sunday |
| `DAY_OF_MONTH` | NUMBER(2,0) | NOT NULL | |
| `DAY_OF_YEAR` | NUMBER(3,0) | NOT NULL | |
| `WEEK_OF_YEAR` | NUMBER(2,0) | NOT NULL | |
| `MONTH_NUM` | NUMBER(2,0) | NOT NULL | |
| `MONTH_NAME` | VARCHAR(10) | NOT NULL | January … December |
| `QUARTER` | NUMBER(1,0) | NOT NULL | 1–4 |
| `YEAR` | NUMBER(4,0) | NOT NULL | |
| `IS_WEEKEND` | BOOLEAN | NOT NULL | |

---

#### `MARTS.DIM_CUSTOMERS`
*Grain: one row per customer version (SCD Type 2)*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `CUSTOMER_KEY` | NUMBER AUTOINCREMENT | PK | Surrogate key |
| `CUSTOMER_ID_NK` | VARCHAR(50) | NOT NULL | Natural key (order-scoped) |
| `CUSTOMER_UNIQUE_ID` | VARCHAR(50) | NOT NULL | True person identifier |
| `ZIP_CODE_PREFIX` | VARCHAR(10) | | |
| `CITY` | VARCHAR(100) | | |
| `STATE` | VARCHAR(10) | | |
| `SCD_START_DT` | DATE | NOT NULL | When this version became active |
| `SCD_END_DT` | DATE | | NULL = current version |
| `IS_CURRENT` | BOOLEAN | NOT NULL | TRUE = active version |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

#### `MARTS.DIM_PRODUCTS`
*Grain: one row per product version (SCD Type 2)*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `PRODUCT_KEY` | NUMBER AUTOINCREMENT | PK | Surrogate key |
| `PRODUCT_ID_NK` | VARCHAR(50) | NOT NULL | Natural key |
| `CATEGORY_NAME_PT` | VARCHAR(100) | | |
| `CATEGORY_NAME_EN` | VARCHAR(100) | | From LKP_CATEGORY_NAMES |
| `PHOTO_CNT` | NUMBER(5,0) | | |
| `WEIGHT_G` | NUMBER(10,2) | | |
| `LENGTH_CM` | NUMBER(8,2) | | |
| `HEIGHT_CM` | NUMBER(8,2) | | |
| `WIDTH_CM` | NUMBER(8,2) | | |
| `SCD_START_DT` | DATE | NOT NULL | |
| `SCD_END_DT` | DATE | | NULL = current version |
| `IS_CURRENT` | BOOLEAN | NOT NULL | |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

#### `MARTS.DIM_SELLERS`
*Grain: one row per seller*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `SELLER_KEY` | NUMBER AUTOINCREMENT | PK | |
| `SELLER_ID_NK` | VARCHAR(50) | NOT NULL | Natural key |
| `ZIP_CODE_PREFIX` | VARCHAR(10) | | |
| `CITY` | VARCHAR(100) | | |
| `STATE` | VARCHAR(10) | | |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

#### `MARTS.DIM_GEOGRAPHY`
*Grain: one row per ZIP code prefix (centroid)*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `GEOGRAPHY_KEY` | NUMBER AUTOINCREMENT | PK | |
| `ZIP_CODE_PREFIX` | VARCHAR(10) | NOT NULL UNIQUE | |
| `CITY` | VARCHAR(100) | | |
| `STATE` | VARCHAR(10) | | |
| `AVG_LAT` | FLOAT | | |
| `AVG_LNG` | FLOAT | | |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

#### `MARTS.FACT_ORDER_ITEMS`
*Grain: one row per line item within an order — primary fact table*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `ORDER_ITEM_KEY` | NUMBER AUTOINCREMENT | PK | Surrogate key |
| `ORDER_ID_NK` | VARCHAR(50) | NOT NULL | Natural order key |
| `ORDER_ITEM_ID` | NUMBER(5,0) | NOT NULL | Line sequence |
| `DATE_KEY` | NUMBER(8,0) | NOT NULL FK → DIM_DATE | Purchase date |
| `CUSTOMER_KEY` | NUMBER | NOT NULL FK → DIM_CUSTOMERS | |
| `PRODUCT_KEY` | NUMBER | NOT NULL FK → DIM_PRODUCTS | |
| `SELLER_KEY` | NUMBER | NOT NULL FK → DIM_SELLERS | |
| `ORDER_STATUS` | VARCHAR(30) | NOT NULL | Denormalized for performance |
| `PRICE_AMT` | NUMBER(12,2) | NOT NULL | Item price excl. freight |
| `FREIGHT_AMT` | NUMBER(12,2) | NOT NULL | Freight for this line |
| `TOTAL_ITEM_AMT` | NUMBER(12,2) | NOT NULL | PRICE_AMT + FREIGHT_AMT |
| `IS_DELIVERED` | BOOLEAN | NOT NULL | |
| `IS_CANCELED` | BOOLEAN | NOT NULL | |
| `DELIVERY_DELAY_DAYS` | NUMBER(5,0) | | Positive = late, negative = early |
| `PURCHASE_TS` | TIMESTAMP_NTZ | NOT NULL | Full timestamp for time-of-day analysis |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

#### `MARTS.FACT_PAYMENTS`
*Grain: one row per payment entry (order + payment_sequential)*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `PAYMENT_KEY` | NUMBER AUTOINCREMENT | PK | |
| `ORDER_ID_NK` | VARCHAR(50) | NOT NULL | |
| `PAYMENT_SEQUENTIAL` | NUMBER(5,0) | NOT NULL | |
| `DATE_KEY` | NUMBER(8,0) | NOT NULL FK → DIM_DATE | Payment date |
| `CUSTOMER_KEY` | NUMBER | NOT NULL FK → DIM_CUSTOMERS | |
| `PAYMENT_TYPE` | VARCHAR(30) | NOT NULL | |
| `PAYMENT_INSTALLMENTS` | NUMBER(5,0) | NOT NULL | |
| `PAYMENT_AMT` | NUMBER(12,2) | NOT NULL | |
| `IS_VOUCHER` | BOOLEAN | NOT NULL | |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

#### `MARTS.FACT_REVIEWS`
*Grain: one row per review submission*

| Column | Data Type | Constraints | Notes |
|---|---|---|---|
| `REVIEW_KEY` | NUMBER AUTOINCREMENT | PK | |
| `REVIEW_ID_NK` | VARCHAR(50) | NOT NULL | Natural key |
| `ORDER_ID_NK` | VARCHAR(50) | NOT NULL | |
| `DATE_KEY` | NUMBER(8,0) | NOT NULL FK → DIM_DATE | Review answered date |
| `CUSTOMER_KEY` | NUMBER | NOT NULL FK → DIM_CUSTOMERS | |
| `REVIEW_SCORE` | NUMBER(1,0) | NOT NULL | 1–5 |
| `REVIEW_TITLE` | VARCHAR(500) | | |
| `REVIEW_MESSAGE` | VARCHAR(5000) | | |
| `IS_POSITIVE` | BOOLEAN | NOT NULL | SCORE >= 4 |
| `IS_NEGATIVE` | BOOLEAN | NOT NULL | SCORE <= 2 |
| `_LOADED_AT` | TIMESTAMP_NTZ | NOT NULL | |

---

### 3.5 AUDIT Schema — Monitoring

---

#### `AUDIT.AUD_PIPELINE_RUNS`
*Grain: one row per pipeline execution*

| Column | Data Type | Notes |
|---|---|---|
| `RUN_ID` | NUMBER AUTOINCREMENT | PK |
| `PIPELINE_NAME` | VARCHAR(200) | e.g., `TASK_LOAD_RAW_ORDERS` |
| `TARGET_TABLE` | VARCHAR(200) | Fully qualified table name |
| `STATUS` | VARCHAR(20) | SUCCESS / FAILED / PARTIAL |
| `ROWS_LOADED` | NUMBER | |
| `ROWS_REJECTED` | NUMBER | |
| `STARTED_AT` | TIMESTAMP_NTZ | |
| `FINISHED_AT` | TIMESTAMP_NTZ | |
| `ERROR_MSG` | VARCHAR(5000) | NULL on success |

---

#### `AUDIT.AUD_DQ_CHECKS`
*Grain: one row per data quality check execution*

| Column | Data Type | Notes |
|---|---|---|
| `CHECK_ID` | NUMBER AUTOINCREMENT | PK |
| `CHECK_NAME` | VARCHAR(200) | e.g., `NULL_ORDER_ID_CHECK` |
| `TARGET_TABLE` | VARCHAR(200) | |
| `CHECK_TYPE` | VARCHAR(50) | NULL_CHECK / DUPLICATE / RANGE / REF_INTEGRITY |
| `RESULT` | VARCHAR(10) | PASS / FAIL |
| `FAIL_ROW_CNT` | NUMBER | 0 on PASS |
| `RUN_AT` | TIMESTAMP_NTZ | |
| `NOTES` | VARCHAR(2000) | |

---

## 4. Layer Flow Summary

```
CSV Files (S3 / Stage)
        │
        │  COPY INTO / Snowpipe
        ▼
  RAW Schema  ──────────────────────────────── append-only, no transforms
        │
        │  Snowflake Tasks + MERGE
        ▼
 STAGING Schema  ─────────────────────────── dedup, clean, enrich, flag
        │
        │  Snowflake Tasks + MERGE
        ▼
  MARTS Schema  ────────────────────────────── star schema, surrogate keys
  (FACT + DIM)
        │
        │  Secure Views / Roles
        ▼
  BI Tools (Tableau / Power BI / Preset)
```

---

## 5. Virtual Warehouse Strategy

| Warehouse | Size | Purpose | Auto-Suspend |
|---|---|---|---|
| `WH_XS_ADHOC` | X-Small | Ad-hoc analyst queries | 60 seconds |
| `WH_M_ETL` | Medium | Scheduled ETL Tasks | 120 seconds |
| `WH_S_REPORTING` | Small | Scheduled BI dashboard refreshes | 300 seconds |

---

## 6. Role & Access Design

| Role | Schema Access | Purpose |
|---|---|---|
| `ROLE_ENGINEER` | ALL schemas (read/write) | Build and maintain pipelines |
| `ROLE_ANALYST` | MARTS + COMMON (read only) | Business reporting and exploration |
| `ROLE_FINANCE` | MARTS.FACT_PAYMENTS (read only) + DIM_* | Payment and revenue analysis |
| `ROLE_MARKETING` | MARTS (read only, masked PII) | Customer and conversion analysis |
| `ROLE_MONITOR` | AUDIT (read only) | Pipeline and DQ monitoring |

---

*Document prepared for ShopSphere Snowflake Practice*
