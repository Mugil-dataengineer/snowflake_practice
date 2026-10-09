# ❄️ ShopSphere – Snowflake Data Warehouse Practice

> A complete, end-to-end Snowflake data warehouse project built on the **Olist Brazilian E-Commerce** dataset.  
> Covers every layer from raw ingestion to analytics-ready star schema, with automated pipelines, data quality checks, SCD Type 2, Dynamic Tables, and audit logging.

---

## 📋 Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Dataset](#3-dataset)
4. [Database Design](#4-database-design)
5. [Grain Definitions](#5-grain-definitions)
6. [SQL Scripts – Run Order](#6-sql-scripts--run-order)
7. [Pipeline Architecture](#7-pipeline-architecture)
8. [Business Analytics Enabled](#8-business-analytics-enabled)
9. [Repository Structure](#9-repository-structure)

---

## 1. Project Overview

ShopSphere is a rapidly growing Indian e-commerce company selling electronics, clothing, books, and household products. Data lives across **four isolated systems** — Customer Management, Order Management, Payment, and Website Activity — making it impossible to get a single consistent view.

**Goal:** Build a centralized cloud data warehouse on Snowflake that integrates all sources and delivers reliable, historical, analytics-ready data.

### Business Problems Solved

| Problem | Solution |
|---|---|
| Customer data separated from orders and payments | Unified star schema with `FACT_ORDER_ITEMS` joined to `DIM_CUSTOMERS` |
| No single purchase journey view | `CUSTOMER_UNIQUE_ID` tracked across all orders |
| Payment failures hard to analyse | `FACT_PAYMENTS` with type flags and reconciliation checks |
| Historical customer changes lost | SCD Type 2 on `DIM_CUSTOMERS` and `DIM_PRODUCTS` |
| Duplicate and inconsistent data | Deduplication via `QUALIFY ROW_NUMBER()` in Dynamic Tables |
| Manual reporting pipelines | Automated Task DAG running every 5 minutes |

---

## 2. Architecture

![Architecture Diagram](architecture.png)

### Layer Overview

```
CSV Files (Local / S3 / Stage)
        │
        │  COPY INTO / Snowpipe
        ▼
  RAW Schema  ──────────── as-is source data, no transforms, append-only
        │
        │  Dynamic Tables (auto-refresh, TARGET_LAG = 1 min)
        ▼
  STAGING.DT_* ─────────── cleaned, deduped, enriched, validated
        │
        │  Streams (DEFAULT / APPEND_ONLY) + Task DAG (every 5 min)
        ▼
  MARTS Schema ─────────── star schema: 3 facts + 5 dimensions
        │
        │  Secure Views / Roles
        ▼
  BI Tools (Tableau / Power BI / Preset)
        │
        │  Leaf Task → Stored Procedure
        ▼
  AUDIT Schema ─────────── pipeline runs + DQ check results
```

### Schemas

| Schema | Purpose |
|---|---|
| `RAW` | As-is landing zone — no transforms, `_ROW_HASH` for dedup |
| `STAGING` | Dynamic Tables (`DT_*`) — auto-refreshing clean layer |
| `MARTS` | Star schema — `FACT_*` and `DIM_*` tables for BI |
| `COMMON` | Lookup tables (e.g. `LKP_CATEGORY_NAMES`) |
| `AUDIT` | `AUD_PIPELINE_RUNS` and `AUD_DQ_CHECKS` |

---

## 3. Dataset

Source: **Olist Brazilian E-Commerce** public dataset — real transaction data from 2016–2018.

| File | Rows | Description |
|---|---|---|
| `olist_customers_dataset.csv` | 99,441 | Customer IDs, city, state, ZIP |
| `olist_geolocation_dataset.csv` | 1,000,163 | GPS lat/lng samples per ZIP prefix |
| `olist_orders_dataset.csv` | 99,441 | Order lifecycle — purchase → delivery |
| `olist_order_items_dataset.csv` | 112,650 | Line items — product, seller, price, freight |
| `olist_order_payments_dataset.csv` | 103,886 | Payment method, installments, amount |
| `olist_order_reviews_dataset.csv` | 104,164 | 1–5 star ratings + optional comments |
| `olist_products_dataset.csv` | 32,951 | Product dimensions and category (Portuguese) |
| `olist_sellers_dataset.csv` | 3,095 | Seller city, state, ZIP |
| `product_category_name_translation.csv` | 71 | Portuguese → English category names |

> All 9 files are **structured CSV** — loaded via `COPY INTO` with `FF_CSV_HEADER` file format.

---

## 4. Database Design

### Naming Conventions

| Identifier | Convention | Example |
|---|---|---|
| All identifiers | `UPPER_SNAKE_CASE` | `FACT_ORDER_ITEMS` |
| Primary keys (surrogate) | `<ENTITY>_KEY` | `CUSTOMER_KEY` |
| Natural keys | `<ENTITY>_ID` or `_NK` suffix | `ORDER_ID`, `PRODUCT_ID_NK` |
| Timestamps | `_AT` (datetime) / `_DT` (date only) | `PURCHASE_TS`, `ORDER_DT` |
| Boolean flags | `IS_` or `HAS_` prefix | `IS_DELIVERED`, `HAS_COMMENT` |
| Monetary amounts | `_AMT` suffix | `PRICE_AMT`, `FREIGHT_AMT` |
| Audit columns | `_LOADED_AT`, `_SOURCE_FILE`, `_ROW_HASH` | on every RAW table |

### Schema Structure

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
├── STAGING  (Dynamic Tables — DT_*)
│   ├── DT_CUSTOMERS        ← INITCAP city, LOWER IDs, QUALIFY dedup
│   ├── DT_GEOLOCATION      ← AVG lat/lng centroid per ZIP
│   ├── DT_ORDERS           ← UPPER status, IS_LATE, DELIVERY_DELAY_DAYS
│   ├── DT_ORDER_ITEMS      ← ROUND prices, TOTAL_ITEM_AMT derived
│   ├── DT_ORDER_PAYMENTS   ← UPPER type, boolean flags
│   ├── DT_ORDER_REVIEWS    ← NULLIF blanks, sentiment flags, dedup per order
│   ├── DT_PRODUCTS         ← LEFT JOIN English category name
│   └── DT_SELLERS          ← INITCAP city, UPPER state
│
├── MARTS  (Star Schema)
│   ├── FACT_ORDER_ITEMS    ← grain: order line item  (primary fact)
│   ├── FACT_PAYMENTS       ← grain: payment entry
│   ├── FACT_REVIEWS        ← grain: review submission
│   ├── DIM_CUSTOMERS       ← SCD Type 2 (tracks city/state/ZIP changes)
│   ├── DIM_PRODUCTS        ← SCD Type 2 (tracks category changes)
│   ├── DIM_SELLERS         ← Type 1 (latest values)
│   ├── DIM_GEOGRAPHY       ← ZIP centroid lookup
│   └── DIM_DATE            ← Calendar 2015–2030
│
├── COMMON
│   └── LKP_CATEGORY_NAMES
│
└── AUDIT
    ├── AUD_PIPELINE_RUNS
    └── AUD_DQ_CHECKS
```

---

## 5. Grain Definitions

The **grain** defines exactly what one row represents in each table.

| Table | Grain | Key |
|---|---|---|
| `RAW_CUSTOMERS` / `DT_CUSTOMERS` | One customer–order identity | `CUSTOMER_ID` |
| `DT_GEOLOCATION` | One GPS centroid per ZIP prefix | `ZIP_CODE_PREFIX` |
| `RAW_ORDERS` / `DT_ORDERS` | One order | `ORDER_ID` |
| `RAW_ORDER_ITEMS` / `DT_ORDER_ITEMS` | One line item within an order | `ORDER_ID + ORDER_ITEM_ID` |
| `RAW_ORDER_PAYMENTS` / `DT_ORDER_PAYMENTS` | One payment entry per order | `ORDER_ID + PAYMENT_SEQUENTIAL` |
| `RAW_ORDER_REVIEWS` / `DT_ORDER_REVIEWS` | One review per order (latest) | `REVIEW_ID` |
| `RAW_PRODUCTS` / `DT_PRODUCTS` | One product SKU | `PRODUCT_ID` |
| `RAW_SELLERS` / `DT_SELLERS` | One seller | `SELLER_ID` |
| `FACT_ORDER_ITEMS` | One order line item | `ORDER_ITEM_KEY` (surrogate) |
| `FACT_PAYMENTS` | One payment entry | `PAYMENT_KEY` (surrogate) |
| `FACT_REVIEWS` | One review | `REVIEW_KEY` (surrogate) |
| `DIM_CUSTOMERS` | One customer version (SCD2) | `CUSTOMER_KEY` (surrogate) |
| `DIM_PRODUCTS` | One product version (SCD2) | `PRODUCT_KEY` (surrogate) |

> ⚠️ `CUSTOMER_ID` is **not** a unique person identifier — one person can have multiple `CUSTOMER_ID` values across orders. Use `CUSTOMER_UNIQUE_ID` for person-level analysis.

---

## 6. SQL Scripts – Run Order

| # | File | Purpose |
|---|---|---|
| 1 | [`sql/01_setup_and_load.sql`](sql/01_setup_and_load.sql) | Database, schemas, warehouses, file format, internal stage, RAW table DDL, `COPY INTO` |
| 2 | [`sql/03_data_quality.sql`](sql/03_data_quality.sql) | Deduplication, reference integrity, validity flags, value validation, DQ results table |
| 3 | [`sql/04_marts_star_schema.sql`](sql/04_marts_star_schema.sql) | Dimension + fact DDL, initial full load, SCD Type 2 merge pattern, late-arriving data |
| 4 | [`sql/06_dynamic_tables_and_pipeline.sql`](sql/06_dynamic_tables_and_pipeline.sql) | Dynamic Tables on RAW, streams on DT_*, Task DAG, `SP_AUDIT_PIPELINE` stored procedure |

> **Note:** `02_staging_standardize.sql` and `05_streams_and_tasks.sql` are **superseded** by `06` — Dynamic Tables replace the manual staging inserts; the `06` Task DAG replaces the `05` stream/task setup.

### Running the pipeline

**Step 1 — Snowflake Worksheet** (Sections 1–4, 6 of `01`):
```sql
USE DATABASE SHOPSPHERE_DW;
-- Run 01_setup_and_load.sql sections 1–4 and 6
```

**Step 2 — SnowSQL CLI** (`PUT` commands — cannot run in browser worksheet):
```bash
snowsql -a <account> -u <username>
PUT file://C:/Users/.../snowflake_dataset/*.csv @SHOPSPHERE_DW.RAW.STG_OLIST_STAGE AUTO_COMPRESS=TRUE;
```

**Step 3 — Snowflake Worksheet** (COPY INTO + verify):
```sql
-- Run sections 7–8 of 01_setup_and_load.sql
```

**Step 4 — Run remaining scripts in order:**
```sql
-- 03_data_quality.sql   → DQ checks
-- 04_marts_star_schema.sql → initial MARTS load
-- 06_dynamic_tables_and_pipeline.sql → activate live pipeline
```

---

## 7. Pipeline Architecture

### Dynamic Tables (auto-refresh layer)

Dynamic Tables replace manual staging inserts. They are pure SQL `SELECT` definitions that Snowflake refreshes automatically within 1 minute of RAW changes.

| What they handle | What they don't handle |
|---|---|
| TRIM / INITCAP / UPPER / LOWER | SCD Type 2 expire + insert |
| ROUND / COALESCE for monetary values | Surrogate key resolution |
| Derived boolean flags | Conditional MERGE logic |
| QUALIFY dedup | Multi-step audit logging |
| LEFT JOIN enrichment | |
| AVG aggregation for geolocation | |

### Task DAG

```
TASK_DT_MASTER_TRIGGER  (every 5 min)
        │
   ┌────┼────────┐
   │    │        │
DIM_  DIM_     DIM_
CUST  PROD     SELL
   │    │        │
   └────┼────────┘
        │  (all dims complete first)
   ┌────┼────────┐
   │    │        │
FACT_ FACT_   FACT_
ITEMS PMTS    REVWS
   │    │        │
   └────┼────────┘
        │
 TASK_DT_AUDIT_LOG
 → CALL SP_AUDIT_PIPELINE(6)
```

### `SP_AUDIT_PIPELINE` — 4-step stored procedure

1. Count rows loaded across all 3 fact tables in the run window
2. Run DQ spot-checks (null surrogate keys, negative prices, bad review scores)
3. Write one row per check to `AUDIT.AUD_DQ_CHECKS`
4. Write one pipeline summary row to `AUDIT.AUD_PIPELINE_RUNS` (status: `SUCCESS` / `PARTIAL`)

### SCD Type 2 Pattern (DIM_CUSTOMERS & DIM_PRODUCTS)

When a customer moves city or a product changes category:

1. **Expire** old row: `SCD_END_DT = today - 1`, `IS_CURRENT = FALSE`
2. **Insert** new row: `SCD_START_DT = today`, `SCD_END_DT = NULL`, `IS_CURRENT = TRUE`

Historical fact rows keep pointing to the old surrogate key → **historical accuracy preserved**.

### Virtual Warehouses

| Warehouse | Size | Purpose | Auto-Suspend |
|---|---|---|---|
| `WH_XS_ADHOC` | X-Small | Ad-hoc analyst queries | 60 s |
| `WH_M_ETL` | Medium | ETL pipeline tasks | 120 s |
| `WH_S_REPORTING` | Small | Scheduled BI refreshes | 300 s |

---

## 8. Business Analytics Enabled

The MARTS layer directly answers the 12 business questions from the BRD:

| Question | Tables Used |
|---|---|
| Daily / monthly / yearly sales | `FACT_ORDER_ITEMS` + `DIM_DATE` |
| Highest revenue products & categories | `FACT_ORDER_ITEMS` + `DIM_PRODUCTS` |
| Top customers by revenue | `FACT_ORDER_ITEMS` + `DIM_CUSTOMERS` |
| Order cancellation & delivery rate | `FACT_ORDER_ITEMS`.`IS_CANCELED` / `IS_DELIVERED` |
| Payment method failure rate | `FACT_PAYMENTS`.`PAYMENT_TYPE` |
| Total refunds | `FACT_PAYMENTS` where `IS_VOUCHER = TRUE` |
| Product views without purchase | (future: website activity semi-structured layer) |
| View → cart → purchase conversion | (future: website activity semi-structured layer) |
| Revenue by city / region | `FACT_ORDER_ITEMS` + `DIM_CUSTOMERS`.`STATE` / `DIM_GEOGRAPHY` |
| Customer behaviour over time | `FACT_ORDER_ITEMS` + `DIM_DATE` + `DIM_CUSTOMERS` |
| Repeat vs one-time customers | `FACT_ORDER_ITEMS` GROUP BY `CUSTOMER_UNIQUE_ID` |
| Average Order Value (AOV) | `AVG(TOTAL_ITEM_AMT)` on `FACT_ORDER_ITEMS` |

### Role-based Access

| Role | Access |
|---|---|
| `ROLE_ENGINEER` | All schemas (read/write) |
| `ROLE_ANALYST` | `MARTS` + `COMMON` (read only) |
| `ROLE_FINANCE` | `FACT_PAYMENTS` + `DIM_*` (read only) |
| `ROLE_MARKETING` | `MARTS` (read only, PII masked) |
| `ROLE_MONITOR` | `AUDIT` (read only) |

---

## 9. Repository Structure

```
snowflake_practice/
│
├── README.md                          ← This file
├── business_requirement.md            ← BRD — objectives, problems, requirements
├── database_design.md                 ← Naming conventions, schema structure, DDL specs
├── grain_definition.md                ← What one row means in each table
├── architecture.png                   ← Architecture diagram (PNG)
│
├── snowflake_dataset/                 ← Olist CSV source files (9 files)
│   ├── olist_customers_dataset.csv
│   ├── olist_geolocation_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   └── product_category_name_translation.csv
│
└── sql/                               ← Snowflake SQL scripts (run in order)
    ├── 01_setup_and_load.sql          ← DB, schemas, stage, RAW tables, COPY INTO
    ├── 02_staging_standardize.sql     ← [Superseded by 06] Manual staging inserts
    ├── 03_data_quality.sql            ← Dedup, ref integrity, validity, DQ results table
    ├── 04_marts_star_schema.sql       ← Star schema DDL + initial load + SCD2 + late data
    ├── 05_streams_and_tasks.sql       ← [Superseded by 06] Streams on STG_* tables
    └── 06_dynamic_tables_and_pipeline.sql  ← Dynamic Tables + Streams + Task DAG + Audit SP
```

---

*ShopSphere Snowflake Practice — built with ❄️ Snowflake · 📊 Olist Dataset · 🔧 SQL*

### Phase 1 --- Foundations (Days 1--2)

**Task 1: Design the data platform** - Write a short business
requirements document. - Draw the source-to-report architecture. -
Define the grain of every table. - Prepare an initial entity
relationship diagram and data dictionary.

**Deliverable:** Requirements, architecture diagram, grain definitions,
and initial data model.

**Task 2: Create Snowflake infrastructure** - Create `SHOPSPHERE_DB`. -
Create `BRONZE`, `SILVER`, `GOLD`, and `AUDIT` schemas. - Create a
dedicated, appropriately sized warehouse. - Configure auto-suspend and
auto-resume. - Inspect database, schema, warehouse, and role objects.

Starter SQL:

``` sql
CREATE DATABASE IF NOT EXISTS SHOPSPHERE_DB;

CREATE SCHEMA IF NOT EXISTS SHOPSPHERE_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS SHOPSPHERE_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS SHOPSPHERE_DB.GOLD;
CREATE SCHEMA IF NOT EXISTS SHOPSPHERE_DB.AUDIT;

CREATE WAREHOUSE IF NOT EXISTS SHOPSPHERE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
```

Check syntax and account-specific availability in Snowsight. Use a size
appropriate to your workload and monitor credits.

### Phase 2 --- Data ingestion (Days 3--5)

**Task 3: Acquire and load source data** - Create or obtain all six
source CSVs. - Upload files to a named internal stage. - Create suitable
CSV file formats and raw landing tables. - Load using `COPY INTO`. -
Reconcile loaded row counts with source counts. - Review load history
and rejected records. - Test repeated loading and duplicate
prevention/detection.

**Deliverable:** Six raw Bronze tables, load evidence, and documented
load errors.

**Task 4: Practice semi-structured data** - Create JSON versions of
selected web events. - Load JSON into a `VARIANT` column. - Extract
event type, customer ID, product ID, and timestamp. - Handle missing
fields and unexpected structures. - Practice flattening a nested array.

**Deliverable:** Raw JSON event table and a clean relational event
table.

### Phase 3 --- Transformation and modelling (Days 6--10)

**Task 5: Build the Silver layer** - Standardize dates, timestamps,
status values, text, and amounts. - Deduplicate using business keys and
deterministic rules. - Identify missing customer/product references. -
Separate valid records from invalid records. - Validate quantities,
payments, refunds, and amounts. - Create reusable data-quality checks.

**Deliverable:** Clean Silver tables, rejected-record handling, and a
quality report.

**Task 6: Create dimensional models** - Build a sales fact table at
order-item grain. - Create customer, product, and date dimensions. -
Choose surrogate keys where appropriate. - Model fact-to-dimension
relationships. - Implement customer SCD Type 2 history. - Test a
customer city change and late-arriving data.

**Deliverable:** Documented star schema and proof that customer history
is preserved.

### Phase 4 --- Automation (Days 11--13)

**Task 7: Build an incremental pipeline using streams and tasks** -
Create a stream on a suitable source/staging table. - Create a task to
process changed records. - Use `MERGE` to insert or update target
rows. - Configure task dependencies if multiple steps are needed. - Test
inserts, updates, duplicates, and failed processing. - Inspect task
history and stream behavior.

**Deliverable:** Incremental processing that is safe to rerun and does
not double-count data.

**Task 8: Build a dynamic-table alternative** - Create a dynamic table
for a Silver transformation. - Create a second dynamic table for a join
or aggregation. - Set a suitable `TARGET_LAG`. - Inspect refresh history
and freshness. - Compare dynamic tables with the streams-and-tasks
approach. - Explain which approach suits each workload.

**Deliverable:** Working dynamic-table chain and written design
comparison.

### Phase 5 --- Analytics and security (Days 14--17)

**Task 9: Build Customer 360** Calculate: - Lifetime net revenue under a
clearly documented rule - Qualifying order count - Average order value -
Last purchase date - Days since purchase - Repeat/inactive/high-value
customer segment

Also create product performance and refund-adjusted sales outputs.
Define cart abandonment carefully, including which sessions and events
count.

**Deliverable:** Gold customer summary and sales mart reconciled to the
source data.

**Task 10: Implement access control and privacy** - Create analyst,
engineer, and restricted-data roles. - Grant only required privileges. -
Mask sensitive email/phone values. - Apply a row access policy for a
regional or departmental use case. - Test behavior using each role. -
Document how authentication, SSO/SAML, and network policies fit into
production.

**Deliverable:** Role and policy scripts plus tests proving access
restrictions work.

### Phase 6 --- Reliability and delivery (Days 18--21)

**Task 11: Add monitoring and recovery** - Log pipeline run status,
processed rows, rejected rows, and timestamps. - Detect stale data,
missing keys, duplicates, and invalid payments. - Use Time Travel to
investigate an intentional data mistake. - Demonstrate a zero-copy clone
for isolated testing. - Inspect query profiles and spilling. - Optimize
at least one query and document the result.

**Deliverable:** Audit trail, quality checks, recovery demonstration,
and before/after evidence.

**Task 12: Prepare the final submission** - Write a README with the
business problem and architecture. - Organize SQL scripts in dependency
order. - Add a data dictionary and source-to-target mapping. - Document
security, costs, limitations, and assumptions. - Capture screenshots of
architecture and results. - Demonstrate an end-to-end run from source
files to insights.

**Deliverable:** Reproducible repository and a project demonstration
suitable for an interview.

## 6. Intentionally introduce data-quality issues

Make the project realistic by adding controlled test cases: - Duplicate
customer and payment records - Missing customer IDs or unknown product
IDs - Inconsistent timestamp formats or time zones - Invalid email
addresses and null values - Cancelled and refunded orders - Failed
payment attempts and partial refunds - Duplicate web events and browsing
sessions with no purchase - A customer whose city changes during the
project

Document how each issue is detected, handled, quarantined, or corrected.
Avoid silently discarding records.

## 7. Important modelling and business rules

Before calculating metrics, define the rules clearly: - **Gross
merchandise value:** Define whether this is before discounts and whether
cancelled orders are excluded. - **Net merchandise sales:** Define
treatment of discounts, cancellations, and returns. - **Cash
collected:** Count only qualifying successful payments. - **Refunds:**
Account for partial and full refunds without subtracting them twice. -
**Average order value:** State which order statuses qualify and whether
the denominator is orders or customers. - **Cart abandonment:** Define
an eligible session, the time window, and what constitutes a purchase. -
**Customer lifetime value/revenue:** State the date range, order/payment
criteria, and refund treatment.

Do not blindly join payments directly to order-item rows and sum payment
amounts: one order may have multiple items and multiple payment
attempts, causing duplicated totals. Aggregate each dataset to the
intended grain before joining when necessary.

## 8. Core Snowflake features to demonstrate

-   Warehouses, auto-suspend, auto-resume, and query profiles
-   Internal stages, file formats, `COPY INTO`, and load history
-   Semi-structured data using `VARIANT` and `FLATTEN`
-   Streams, tasks, `MERGE`, and task dependencies
-   Dynamic tables and `TARGET_LAG`
-   Dimensional modelling, surrogate keys, and SCD Type 2
-   Roles, grants, masking policies, and row access policies
-   Time Travel and zero-copy cloning
-   Data quality, audit logging, and cost awareness

Optional extensions: 1. S3 plus Snowpipe for event-driven ingestion. 2.
Snowpark Python transformation compared with SQL. 3. External
functions/API integration for a non-sensitive product-classification or
sentiment use case. 4. Snowsight or BI dashboard over the Gold layer.

Optional cloud integrations may require cloud accounts, privileges, and
configuration. They are not prerequisites for the core project.

## 9. Suggested repository structure

``` text
snowflake-customer-360/
├── README.md
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── source_to_target_mapping.md
│   └── design_decisions.md
├── data/
│   └── sample_data_or_generation_script/
├── sql/
│   ├── 01_database_and_warehouse.sql
│   ├── 02_file_formats_and_stages.sql
│   ├── 03_bronze_tables.sql
│   ├── 04_load_data.sql
│   ├── 05_silver_transformations.sql
│   ├── 06_dimensional_model.sql
│   ├── 07_incremental_pipeline.sql
│   ├── 08_dynamic_tables.sql
│   ├── 09_gold_analytics.sql
│   ├── 10_security.sql
│   └── 11_data_quality_and_recovery.sql
├── tests/
│   └── validation_queries.sql
└── screenshots/
    └── architecture_and_results/
```

Never commit account passwords, private keys, tokens, or other
credentials. Use synthetic or appropriately licensed data.

## 10. Final acceptance tests

Before calling the project complete, prove that: 1. Revenue metrics
reconcile and differences between sales, payments, and refunds are
explained. 2. Re-loading source records does not incorrectly
double-count them. 3. New and updated records flow through incremental
processing. 4. SCD Type 2 preserves prior customer versions. 5. Invalid
payments and unknown product IDs are detected. 6. Roles, masking, and
row filtering behave as designed. 7. Time Travel or cloning supports a
controlled recovery demonstration. 8. Pipeline freshness,
task/dynamic-table history, and warehouse usage are inspected.