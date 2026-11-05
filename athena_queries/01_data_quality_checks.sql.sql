-- 01_data_quality_checks.sql
-- Database: ecommerce_curated_db

-- 1.1 Row counts for all processed datasets
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM ecommerce_curated_db.customers
UNION ALL
SELECT 'products', COUNT(*) FROM ecommerce_curated_db.products
UNION ALL
SELECT 'categories', COUNT(*) FROM ecommerce_curated_db.categories
UNION ALL
SELECT 'orders', COUNT(*) FROM ecommerce_curated_db.orders
UNION ALL
SELECT 'order_details', COUNT(*) FROM ecommerce_curated_db.order_details
UNION ALL
SELECT 'payments', COUNT(*) FROM ecommerce_curated_db.payments
UNION ALL
SELECT 'shipments', COUNT(*) FROM ecommerce_curated_db.shipments
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM ecommerce_curated_db.fact_sales;


-- 1.2 Check duplicate customer IDs
SELECT
    customerid,
    COUNT(*) AS duplicate_count
FROM ecommerce_curated_db.customers
GROUP BY customerid
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- 1.3 Check duplicate order IDs
SELECT
    orderid,
    COUNT(*) AS duplicate_count
FROM ecommerce_curated_db.orders
GROUP BY orderid
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- 1.4 Check NULL primary keys
SELECT
    COUNT_IF(customerid IS NULL) AS null_customer_ids
FROM ecommerce_curated_db.customers;

SELECT
    COUNT_IF(productid IS NULL) AS null_product_ids
FROM ecommerce_curated_db.products;

SELECT
    COUNT_IF(orderid IS NULL) AS null_order_ids
FROM ecommerce_curated_db.orders;

SELECT
    COUNT_IF(orderdetailid IS NULL) AS null_order_detail_ids
FROM ecommerce_curated_db.order_details;


-- 1.5 Check invalid customer emails
SELECT *
FROM ecommerce_curated_db.customers
WHERE email IS NULL
   OR NOT regexp_like(
       email,
       '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
   );


-- 1.6 Check invalid product prices
SELECT *
FROM ecommerce_curated_db.products
WHERE price IS NULL
   OR price <= 0;


-- 1.7 Check invalid order statuses
SELECT
    status,
    COUNT(*) AS order_count
FROM ecommerce_curated_db.orders
WHERE status NOT IN (
    'PENDING',
    'SHIPPED',
    'DELIVERED',
    'CANCELLED',
    'UNKNOWN'
)
   OR status IS NULL
GROUP BY status;


-- 1.8 Check negative quantities or prices
SELECT *
FROM ecommerce_curated_db.order_details
WHERE quantity < 0
   OR unitprice < 0
   OR total_amount < 0;


-- 1.9 Check orphan order details
SELECT COUNT(*) AS orphan_order_details
FROM ecommerce_curated_db.order_details od
LEFT JOIN ecommerce_curated_db.orders o
    ON od.orderid = o.orderid
WHERE o.orderid IS NULL;


-- 1.10 Check orphan order details with missing products
SELECT COUNT(*) AS details_without_products
FROM ecommerce_curated_db.order_details od
LEFT JOIN ecommerce_curated_db.products p
    ON od.productid = p.productid
WHERE p.productid IS NULL;


-- 1.11 Check shipments without matching orders
SELECT COUNT(*) AS shipments_without_orders
FROM ecommerce_curated_db.shipments s
LEFT JOIN ecommerce_curated_db.orders o
    ON s.orderid = o.orderid
WHERE o.orderid IS NULL;


-- 1.12 Check orders without shipments
SELECT COUNT(*) AS orders_without_shipments
FROM ecommerce_curated_db.orders o
LEFT JOIN ecommerce_curated_db.shipments s
    ON o.orderid = s.orderid
WHERE s.orderid IS NULL;