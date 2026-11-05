-- 05_reconciliation.sql
-- Database: ecommerce_curated_db


-- 5.1 Compare sales totals between fact_sales and order_details
SELECT
    'fact_sales' AS source_name,
    SUM(total_amount) AS total_sales
FROM ecommerce_curated_db.fact_sales

UNION ALL

SELECT
    'order_details' AS source_name,
    SUM(total_amount) AS total_sales
FROM ecommerce_curated_db.order_details;


-- 5.2 Compare total sales with customer_sales output
SELECT
    'fact_sales' AS source_name,
    SUM(total_amount) AS total_sales
FROM ecommerce_curated_db.fact_sales

UNION ALL

SELECT
    'customer_sales' AS source_name,
    SUM(total_sales) AS total_sales
FROM ecommerce_curated_db.customer_sales;


-- 5.3 Compare product-level aggregation
WITH calculated_product_sales AS (
    SELECT
        productid,
        SUM(total_amount) AS calculated_sales
    FROM ecommerce_curated_db.fact_sales
    GROUP BY productid
)
SELECT
    COALESCE(c.productid, p.productid) AS productid,
    c.calculated_sales,
    p.total_sales AS stored_sales,
    COALESCE(c.calculated_sales, 0)
        - COALESCE(p.total_sales, 0) AS difference
FROM calculated_product_sales c
FULL OUTER JOIN ecommerce_curated_db.product_sales p
    ON c.productid = p.productid
WHERE ABS(
    COALESCE(c.calculated_sales, 0)
        - COALESCE(p.total_sales, 0)
) > 0.01;


-- 5.4 Compare category-level aggregation
WITH calculated_category_sales AS (
    SELECT
        categoryid,
        SUM(total_amount) AS calculated_sales
    FROM ecommerce_curated_db.fact_sales
    GROUP BY categoryid
)
SELECT
    COALESCE(c.categoryid, p.categoryid) AS categoryid,
    c.calculated_sales,
    p.total_sales AS stored_sales,
    COALESCE(c.calculated_sales, 0)
        - COALESCE(p.total_sales, 0) AS difference
FROM calculated_category_sales c
FULL OUTER JOIN ecommerce_curated_db.category_sales p
    ON c.categoryid = p.categoryid
WHERE ABS(
    COALESCE(c.calculated_sales, 0)
        - COALESCE(p.total_sales, 0)
) > 0.01;


-- 5.5 Compare monthly aggregation
WITH calculated_monthly_sales AS (
    SELECT
        year(orderdate) AS sales_year,
        month(orderdate) AS sales_month,
        SUM(total_amount) AS calculated_sales
    FROM ecommerce_curated_db.fact_sales
    GROUP BY year(orderdate), month(orderdate)
)
SELECT
    COALESCE(c.sales_year, m.year) AS sales_year,
    COALESCE(c.sales_month, m.month) AS sales_month,
    c.calculated_sales,
    m.total_sales AS stored_sales,
    COALESCE(c.calculated_sales, 0)
        - COALESCE(m.total_sales, 0) AS difference
FROM calculated_monthly_sales c
FULL OUTER JOIN ecommerce_curated_db.monthly_sales m
    ON c.sales_year = m.year
   AND c.sales_month = m.month
WHERE ABS(
    COALESCE(c.calculated_sales, 0)
        - COALESCE(m.total_sales, 0)
) > 0.01;


-- 5.6 Payment reconciliation
WITH order_totals AS (
    SELECT
        orderid,
        SUM(total_amount) AS order_total
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
    o.orderid,
    o.order_total,
    p.paid_total,
    COALESCE(p.paid_total, 0) - o.order_total AS difference
FROM order_totals o
LEFT JOIN payment_totals p
    ON o.orderid = p.orderid
WHERE p.paid_total IS NULL
   OR ABS(p.paid_total - o.order_total) > 0.01;


-- 5.7 Compare order counts between orders and fact_sales
SELECT
    (SELECT COUNT(DISTINCT orderid)
     FROM ecommerce_curated_db.orders) AS source_order_count,

    (SELECT COUNT(DISTINCT orderid)
     FROM ecommerce_curated_db.fact_sales) AS fact_sales_order_count;