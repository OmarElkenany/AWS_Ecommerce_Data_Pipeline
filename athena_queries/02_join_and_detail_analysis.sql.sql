-- 02_join_and_detail_analysis.sql
-- Database: ecommerce_curated_db


-- 2.1 Orders with customer information
SELECT
    o.orderid,
    o.customerid,
    c.full_name,
    c.city,
    c.country,
    o.orderdate,
    o.status
FROM ecommerce_curated_db.orders o
INNER JOIN ecommerce_curated_db.customers c
    ON o.customerid = c.customerid
ORDER BY o.orderdate DESC;


-- 2.2 Order details with product and category IDs
SELECT
    od.orderdetailid,
    od.orderid,
    od.productid,
    p.categoryid,
    od.quantity,
    od.unitprice,
    od.total_amount
FROM ecommerce_curated_db.order_details od
INNER JOIN ecommerce_curated_db.products p
    ON od.productid = p.productid;


-- 2.3 Product and category relationship
SELECT
    p.productid,
    p.categoryid,
    p.price
FROM ecommerce_curated_db.products p
INNER JOIN ecommerce_curated_db.categories c
    ON p.categoryid = c.categoryid;


-- 2.4 Orders with payment information
-- Aggregate payments first to avoid multiplying order rows
SELECT
    o.orderid,
    o.customerid,
    o.orderdate,
    o.status,
    COALESCE(p.total_paid, 0) AS total_paid
FROM ecommerce_curated_db.orders o
LEFT JOIN (
    SELECT
        orderid,
        SUM(amount) AS total_paid
    FROM ecommerce_curated_db.payments
    GROUP BY orderid
) p
    ON o.orderid = p.orderid;


-- 2.5 Orders with shipment information
SELECT
    o.orderid,
    o.orderdate,
    o.status,
    s.shipmentid,
    s.shipdate,
    s.shipperid
FROM ecommerce_curated_db.orders o
LEFT JOIN ecommerce_curated_db.shipments s
    ON o.orderid = s.orderid
ORDER BY o.orderdate DESC;


-- 2.6 Customers who have never placed an order
SELECT
    c.customerid,
    c.full_name,
    c.email
FROM ecommerce_curated_db.customers c
LEFT JOIN ecommerce_curated_db.orders o
    ON c.customerid = o.customerid
WHERE o.orderid IS NULL;


-- 2.7 Products that have never been ordered
SELECT
    p.productid,
    p.categoryid,
    p.price
FROM ecommerce_curated_db.products p
LEFT JOIN ecommerce_curated_db.order_details od
    ON p.productid = od.productid
WHERE od.productid IS NULL;


-- 2.8 Orders with multiple shipments
SELECT
    orderid,
    COUNT(DISTINCT shipmentid) AS shipment_count
FROM ecommerce_curated_db.shipments
GROUP BY orderid
HAVING COUNT(DISTINCT shipmentid) > 1;