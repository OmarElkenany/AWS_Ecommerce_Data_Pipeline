-- 03_aggregations_and_business_kpis.sql
-- Database: ecommerce_curated_db


-- 3.1 Overall sales performance
SELECT
    SUM(total_amount) AS total_sales,
    COUNT(DISTINCT orderid) AS total_orders,
    COUNT(DISTINCT customerid) AS purchasing_customers,
    SUM(quantity) AS total_units_sold,
    AVG(total_amount) AS average_order_line_value
FROM ecommerce_curated_db.fact_sales;


-- 3.2 Average Order Value (AOV)
-- Calculate each order total first
WITH order_totals AS (
    SELECT
        orderid,
        SUM(total_amount) AS order_total
    FROM ecommerce_curated_db.fact_sales
    GROUP BY orderid
)
SELECT
    AVG(order_total) AS average_order_value
FROM order_totals;


-- 3.3 Sales by customer
SELECT
    customerid,
    SUM(total_amount) AS total_sales,
    COUNT(DISTINCT orderid) AS total_orders,
    SUM(quantity) AS total_quantity
FROM ecommerce_curated_db.fact_sales
GROUP BY customerid
ORDER BY total_sales DESC;


-- 3.4 Top 10 customers by sales
SELECT
    customerid,
    SUM(total_amount) AS total_sales,
    COUNT(DISTINCT orderid) AS total_orders
FROM ecommerce_curated_db.fact_sales
GROUP BY customerid
ORDER BY total_sales DESC
LIMIT 10;


-- 3.5 Sales by product
SELECT
    productid,
    SUM(total_amount) AS total_sales,
    SUM(quantity) AS units_sold,
    AVG(unitprice) AS average_selling_price
FROM ecommerce_curated_db.fact_sales
GROUP BY productid
ORDER BY total_sales DESC;


-- 3.6 Top 10 products by sales
SELECT
    productid,
    SUM(total_amount) AS total_sales,
    SUM(quantity) AS units_sold
FROM ecommerce_curated_db.fact_sales
GROUP BY productid
ORDER BY total_sales DESC
LIMIT 10;


-- 3.7 Sales by category
SELECT
    categoryid,
    SUM(total_amount) AS total_sales,
    SUM(quantity) AS units_sold,
    COUNT(DISTINCT orderid) AS total_orders
FROM ecommerce_curated_db.fact_sales
GROUP BY categoryid
ORDER BY total_sales DESC;


-- 3.8 Sales by country
SELECT
    c.country,
    SUM(fs.total_amount) AS total_sales,
    COUNT(DISTINCT fs.orderid) AS total_orders,
    COUNT(DISTINCT fs.customerid) AS customers
FROM ecommerce_curated_db.fact_sales fs
INNER JOIN ecommerce_curated_db.customers c
    ON fs.customerid = c.customerid
GROUP BY c.country
ORDER BY total_sales DESC;


-- 3.9 Sales by city
SELECT
    c.country,
    c.city,
    SUM(fs.total_amount) AS total_sales,
    COUNT(DISTINCT fs.orderid) AS total_orders
FROM ecommerce_curated_db.fact_sales fs
INNER JOIN ecommerce_curated_db.customers c
    ON fs.customerid = c.customerid
GROUP BY c.country, c.city
ORDER BY total_sales DESC;


-- 3.10 Sales by order status
SELECT
    status,
    SUM(total_amount) AS total_sales,
    COUNT(DISTINCT orderid) AS total_orders
FROM ecommerce_curated_db.fact_sales
GROUP BY status
ORDER BY total_sales DESC;


-- 3.11 Average quantity per order
WITH order_quantities AS (
    SELECT
        orderid,
        SUM(quantity) AS total_quantity
    FROM ecommerce_curated_db.fact_sales
    GROUP BY orderid
)
SELECT
    AVG(total_quantity) AS average_quantity_per_order
FROM order_quantities;


-- 3.12 Sales by order value category
SELECT
    order_value_category,
    COUNT(*) AS order_line_count,
    SUM(total_amount) AS total_sales
FROM ecommerce_curated_db.order_details
GROUP BY order_value_category
ORDER BY total_sales DESC;


-- 3.13 Payment collection overview
SELECT
    COUNT(DISTINCT orderid) AS orders_with_payments,
    SUM(amount) AS total_payment_amount,
    AVG(amount) AS average_payment_amount
FROM ecommerce_curated_db.payments;


-- 3.14 Payment reconciliation by order
WITH order_totals AS (
    SELECT
        orderid,
        SUM(total_amount) AS calculated_total
    FROM ecommerce_curated_db.order_details
    GROUP BY orderid
),
payment_totals AS (
    SELECT
        orderid,
        SUM(amount) AS paid_total
    FROM ecommerce_curated_db.payments
    GROUP BY orderid
)
SELECT
    COALESCE(o.orderid, p.orderid) AS orderid,
    o.calculated_total,
    p.paid_total,
    COALESCE(p.paid_total, 0) - COALESCE(o.calculated_total, 0) AS payment_difference
FROM order_totals o
FULL OUTER JOIN payment_totals p
    ON o.orderid = p.orderid
WHERE ABS(
    COALESCE(p.paid_total, 0) - COALESCE(o.calculated_total, 0)
) > 0.01;