# ShopSphere E-Commerce Data Warehouse

## Business Requirements Document (BRD)

### 1. Business Objective

ShopSphere is a rapidly growing Indian e-commerce company selling electronics, clothing, books, and household products. The company currently stores business data across separate systems, including Customer Management, Order Management, Payment, and Website Activity systems.

Because these systems are isolated, business teams find it difficult to obtain a **single, consistent view of customers, orders, payments, and customer behavior**.

The objective of this project is to build a centralized **cloud data warehouse using Snowflake** that integrates data from all these systems and provides reliable, historical, and analytics-ready data for business reporting and decision-making.

### 2. Business Problems

The current fragmented data environment creates the following problems:

* Customer information is separated from their orders and payment history.
* Business teams cannot easily determine a customer's complete purchase journey.
* Payment failures and refunds are difficult to analyze against orders.
* Website behavior such as product views, searches, and cart additions is not connected to purchases.
* Historical changes in customer information are difficult to track.
* Reports require manual data extraction and transformation.
* Different systems may contain inconsistent or duplicate data.
* Management lacks a centralized view of sales, customers, products, payments, and customer behavior.

### 3. Business Requirements

The data platform should integrate data from the following systems:

**Customer Management System**

* Customer ID
* Name and email
* Registration date
* City/state/location
* Customer status

**Order Management System**

* Order ID
* Customer ID
* Product ID
* Order date
* Quantity
* Price
* Discount
* Order status
* Cancellation and delivery information

**Payment System**

* Payment ID
* Order ID
* Payment method
* Payment amount
* Payment status
* Payment attempt timestamp
* Refund information

**Website Activity System**

* Customer/session ID
* Product views
* Searches
* Cart additions
* Purchase events
* Event timestamp

### 4. Required Analytics

The resulting data warehouse should enable business users to answer questions such as:

1. What are the company's daily, monthly, and yearly sales?
2. Which products and categories generate the highest revenue?
3. Which customers generate the most revenue?
4. What percentage of orders are cancelled or successfully delivered?
5. Which payment methods have the highest failure rate?
6. How much money has been refunded?
7. How many customers view a product but do not purchase it?
8. What is the conversion rate from **product view → cart → purchase**?
9. Which cities or regions generate the most revenue?
10. How does customer purchasing behavior change over time?
11. Which customers are repeat customers versus one-time customers?
12. What is the average order value (AOV)?

### 5. Data Platform Requirements

The solution should use **Snowflake as the centralized data warehouse** and should demonstrate appropriate Snowflake capabilities learned during the SnowPro Core course.

The platform should support:

* Initial and incremental data loading.
* Raw, transformed, and analytics-ready data layers.
* Data quality and validation checks.
* Historical tracking of customer and product changes.
* Deduplication and handling of late-arriving data.
* Automated data pipelines.
* Near-real-time or continuous ingestion for website activity where appropriate.
* Secure access based on business roles.
* Query performance and warehouse optimization.
* Monitoring of pipeline failures and data quality issues.

### 6. Expected Business Outcome

The final solution should provide ShopSphere with a **single source of truth** for e-commerce analytics.

Business teams should be able to use the warehouse to understand **sales performance, customer behavior, payment performance, product performance, and website conversion**, without manually combining data from multiple operational systems.

The solution should also be scalable so that it can support ShopSphere's future growth in customers, products, orders, and website traffic.
