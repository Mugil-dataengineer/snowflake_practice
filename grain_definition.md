# Grain Definition Document
## Olist E-Commerce Dataset – ShopSphere Snowflake Practice

---

### What is "Grain"?

The **grain** of a table defines exactly **one row** — what a single record in that table represents. Defining the grain is the first and most critical step in data warehouse design. It determines what questions the table can answer and how it can be joined to other tables.

---

## Table-by-Table Grain Definitions

---

### 1. `olist_customers_dataset`

| Property | Detail |
|---|---|
| **File** | `olist_customers_dataset.csv` |
| **Row Count** | 99,441 |
| **Grain** | One row per **customer order identity** |
| **Grain Key(s)** | `customer_id` |
| **Natural Key** | `customer_unique_id` |

**Grain Explanation:**

Each row represents a **unique customer entry as seen in a specific order context**. The `customer_id` is generated per-order linkage — meaning the same physical person can appear multiple times under different `customer_id` values. The `customer_unique_id` is the true person-level identifier.

> ⚠️ **Important nuance:** This table is **NOT** one row per person. It is one row per `customer_id`, which is tied to an order. A single person who placed 3 orders may have up to 3 `customer_id` values but one `customer_unique_id`.

**Columns:**

| Column | Description |
|---|---|
| `customer_id` | Surrogate key linking to orders (order-scoped) |
| `customer_unique_id` | True unique identifier for the physical customer |
| `customer_zip_code_prefix` | 5-digit ZIP code prefix |
| `customer_city` | Customer's city |
| `customer_state` | Customer's state (2-letter code) |

---

### 2. `olist_geolocation_dataset`

| Property | Detail |
|---|---|
| **File** | `olist_geolocation_dataset.csv` |
| **Row Count** | 1,000,163 |
| **Grain** | One row per **geolocation coordinate sample for a ZIP code prefix** |
| **Grain Key(s)** | `geolocation_zip_code_prefix` + `geolocation_lat` + `geolocation_lng` (composite) |

**Grain Explanation:**

Each row represents **one GPS coordinate observation for a given ZIP code prefix**. The same ZIP code prefix can have many rows because GPS samples are collected at multiple points within a ZIP boundary. This is a **many-to-one** relationship from rows to ZIP codes — there is no single unique key per row.

> ⚠️ This table is **not deduplicated** by ZIP code. To use it as a lookup, aggregate by `geolocation_zip_code_prefix` (e.g., `AVG(lat)`, `AVG(lng)`).

**Columns:**

| Column | Description |
|---|---|
| `geolocation_zip_code_prefix` | 5-digit ZIP code prefix (non-unique per row) |
| `geolocation_lat` | Latitude of the GPS sample |
| `geolocation_lng` | Longitude of the GPS sample |
| `geolocation_city` | City name for this location sample |
| `geolocation_state` | State code for this location sample |

---

### 3. `olist_orders_dataset`

| Property | Detail |
|---|---|
| **File** | `olist_orders_dataset.csv` |
| **Row Count** | 99,441 |
| **Grain** | One row per **order** |
| **Grain Key(s)** | `order_id` |

**Grain Explanation:**

Each row represents a **single customer order** placed on the Olist marketplace. An order is a container — it can hold one or more items (see `olist_order_items_dataset`). This table tracks the lifecycle of the order from purchase to delivery.

**Columns:**

| Column | Description |
|---|---|
| `order_id` | Unique identifier for the order (PK) |
| `customer_id` | FK to `olist_customers_dataset` |
| `order_status` | Current status: `delivered`, `shipped`, `canceled`, `processing`, etc. |
| `order_purchase_timestamp` | Timestamp when the customer placed the order |
| `order_approved_at` | Timestamp when payment was approved |
| `order_delivered_carrier_date` | Timestamp when the order was handed to the carrier |
| `order_delivered_customer_date` | Timestamp when the customer received the order |
| `order_estimated_delivery_date` | Originally estimated delivery date |

---

### 4. `olist_order_items_dataset`

| Property | Detail |
|---|---|
| **File** | `olist_order_items_dataset.csv` |
| **Row Count** | 112,650 |
| **Grain** | One row per **line item within an order** |
| **Grain Key(s)** | `order_id` + `order_item_id` (composite) |

**Grain Explanation:**

Each row represents **one product line within an order**. A single order (identified by `order_id`) can contain multiple items, each with its own `order_item_id` (sequential integer starting at 1). This is the most granular transactional table — it captures exactly what was purchased, from which seller, at what price.

> 📌 This is the **primary fact source** for revenue and product analysis. Join to `olist_orders_dataset` on `order_id` to get order-level context.

**Columns:**

| Column | Description |
|---|---|
| `order_id` | FK to `olist_orders_dataset` |
| `order_item_id` | Line item sequence number within the order (1, 2, 3…) |
| `product_id` | FK to `olist_products_dataset` |
| `seller_id` | FK to `olist_sellers_dataset` |
| `shipping_limit_date` | Deadline for the seller to hand off to carrier |
| `price` | Price of this item (excluding freight) |
| `freight_value` | Freight cost for this item |

---

### 5. `olist_order_payments_dataset`

| Property | Detail |
|---|---|
| **File** | `olist_order_payments_dataset.csv` |
| **Row Count** | 103,886 |
| **Grain** | One row per **payment attempt / installment sequence for an order** |
| **Grain Key(s)** | `order_id` + `payment_sequential` (composite) |

**Grain Explanation:**

Each row represents **one payment transaction entry for an order**. A single order can have multiple payment rows because:
- A customer may split payment across **multiple payment methods** (e.g., voucher + credit card).
- Each method gets a separate `payment_sequential` number (1, 2, 3…).

> ⚠️ Do **not** sum `payment_value` by `order_id` without grouping — multiple rows per order are expected and intentional.

**Columns:**

| Column | Description |
|---|---|
| `order_id` | FK to `olist_orders_dataset` |
| `payment_sequential` | Sequence of payment method used (1 = primary, 2 = secondary…) |
| `payment_type` | Method: `credit_card`, `boleto`, `voucher`, `debit_card` |
| `payment_installments` | Number of installments chosen by the customer |
| `payment_value` | Amount paid in this payment entry |

---

### 6. `olist_order_reviews_dataset`

| Property | Detail |
|---|---|
| **File** | `olist_order_reviews_dataset.csv` |
| **Row Count** | 104,164 |
| **Grain** | One row per **review submitted for an order** |
| **Grain Key(s)** | `review_id` |

**Grain Explanation:**

Each row represents **one customer review for one order**. Typically one review per order, but due to system re-surveys, an order may occasionally have more than one `review_id`. The grain is the individual review submission event.

**Columns:**

| Column | Description |
|---|---|
| `review_id` | Unique identifier for the review (PK) |
| `order_id` | FK to `olist_orders_dataset` |
| `review_score` | Star rating from 1 (worst) to 5 (best) |
| `review_comment_title` | Optional short title of the review |
| `review_comment_message` | Optional free-text review body |
| `review_creation_date` | Date the review form was sent to the customer |
| `review_answer_timestamp` | Timestamp when the customer submitted the review |

---

### 7. `olist_products_dataset`

| Property | Detail |
|---|---|
| **File** | `olist_products_dataset.csv` |
| **Row Count** | 32,951 |
| **Grain** | One row per **product listed on the marketplace** |
| **Grain Key(s)** | `product_id` |

**Grain Explanation:**

Each row represents a **unique product SKU** available on Olist. This is a dimension/reference table. It contains physical attributes and category metadata for each product.

**Columns:**

| Column | Description |
|---|---|
| `product_id` | Unique identifier for the product (PK) |
| `product_category_name` | Category name in Portuguese |
| `product_name_lenght` | Character length of the product name |
| `product_description_lenght` | Character length of the product description |
| `product_photos_qty` | Number of photos published for the product |
| `product_weight_g` | Product weight in grams |
| `product_length_cm` | Product length in centimetres |
| `product_height_cm` | Product height in centimetres |
| `product_width_cm` | Product width in centimetres |

---

### 8. `olist_sellers_dataset`

| Property | Detail |
|---|---|
| **File** | `olist_sellers_dataset.csv` |
| **Row Count** | 3,095 |
| **Grain** | One row per **seller registered on the marketplace** |
| **Grain Key(s)** | `seller_id` |

**Grain Explanation:**

Each row represents a **unique third-party seller** operating on the Olist platform. This is a dimension/reference table. Sellers are linked to order items.

**Columns:**

| Column | Description |
|---|---|
| `seller_id` | Unique identifier for the seller (PK) |
| `seller_zip_code_prefix` | 5-digit ZIP code prefix of seller's location |
| `seller_city` | City where the seller is located |
| `seller_state` | State where the seller is located (2-letter code) |

---

### 9. `product_category_name_translation`

| Property | Detail |
|---|---|
| **File** | `product_category_name_translation.csv` |
| **Row Count** | 71 |
| **Grain** | One row per **product category** |
| **Grain Key(s)** | `product_category_name` |

**Grain Explanation:**

Each row represents **one product category**, providing a mapping from the original Portuguese category name to its English translation. This is a small lookup/reference table used to enrich the `olist_products_dataset`.

**Columns:**

| Column | Description |
|---|---|
| `product_category_name` | Category name in Portuguese (PK, FK to products table) |
| `product_category_name_english` | Translated English category name |

---

## Summary Table

| Table | Row Count | Grain (One row per…) | Grain Key(s) |
|---|---|---|---|
| `olist_customers_dataset` | 99,441 | Customer–order identity | `customer_id` |
| `olist_geolocation_dataset` | 1,000,163 | GPS coordinate sample for a ZIP prefix | `zip_prefix` + `lat` + `lng` |
| `olist_orders_dataset` | 99,441 | Order | `order_id` |
| `olist_order_items_dataset` | 112,650 | Line item within an order | `order_id` + `order_item_id` |
| `olist_order_payments_dataset` | 103,886 | Payment entry for an order | `order_id` + `payment_sequential` |
| `olist_order_reviews_dataset` | 104,164 | Review submitted for an order | `review_id` |
| `olist_products_dataset` | 32,951 | Product SKU | `product_id` |
| `olist_sellers_dataset` | 3,095 | Seller | `seller_id` |
| `product_category_name_translation` | 71 | Product category | `product_category_name` |

---

## Key Relationships

```
olist_customers_dataset
    └── customer_id ──────────────────────── olist_orders_dataset.customer_id
                                                      │
                              ┌───────────────────────┼──────────────────────────┐
                              │                        │                          │
              olist_order_items_dataset    olist_order_payments_dataset   olist_order_reviews_dataset
                  │       │
                  │       └── seller_id ─── olist_sellers_dataset.seller_id
                  │
                  └── product_id ─────────── olist_products_dataset.product_id
                                                        │
                                         product_category_name ──── product_category_name_translation

olist_geolocation_dataset  (joined on zip_code_prefix to customers or sellers)
```

---

*Document prepared for ShopSphere Snowflake Practice | Source: Olist Brazilian E-Commerce Dataset*
