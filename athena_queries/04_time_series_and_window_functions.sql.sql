-- 04_time_series_and_window_functions.sql
-- Database: ecommerce_curated_db


-- 4.1 Daily sales
SELECT
    orderdate,
    SUM(total_amount) AS daily_sales,
    COUNT(DISTINCT orderid) AS daily_orders
FROM ecommerce_curated_db.fact_sales
GROUP BY orderdate
ORDER BY orderdate;


-- 4.2 Monthly sales
SELECT
    year(orderdate) AS sales_year,
    month(orderdate) AS sales_month,
    SUM(total_amount) AS monthly_sales,
    COUNT(DISTINCT orderid) AS monthly_orders
FROM ecommerce_curated_db.fact_sales
GROUP BY
    year(orderdate),
    month(orderdate)
ORDER BY sales_year, sales_month;


-- 4.3 Quarterly sales
SELECT
    year(orderdate) AS sales_year,
    quarter(orderdate) AS sales_quarter,
    SUM(total_amount) AS quarterly_sales
FROM ecommerce_curated_db.fact_sales
GROUP BY
    year(orderdate),
    quarter(orderdate)
ORDER BY sales_year, sales_quarter;


-- 4.4 Running total of monthly sales
WITH monthly_sales AS (
    SELECT
        date_trunc('month', orderdate) AS sales_month,
        SUM(total_amount) AS monthly_revenue
    FROM ecommerce_curated_db.fact_sales
    GROUP BY date_trunc('month', orderdate)
)
SELECT
    sales_month,
    monthly_revenue,
    SUM(monthly_revenue) OVER (
        ORDER BY sales_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue
FROM monthly_sales
ORDER BY sales_month;


-- 4.5 Month-over-month sales growth
WITH monthly_sales AS (
    SELECT
        date_trunc('month', orderdate) AS sales_month,
        SUM(total_amount) AS monthly_revenue
    FROM ecommerce_curated_db.fact_sales
    GROUP BY date_trunc('month', orderdate)
),
sales_with_previous_month AS (
    SELECT
        sales_month,
        monthly_revenue,
        LAG(monthly_revenue) OVER (
            ORDER BY sales_month
        ) AS previous_month_revenue
    FROM monthly_sales
)
SELECT
    sales_month,
    monthly_revenue,
    previous_month_revenue,
    monthly_revenue - previous_month_revenue AS revenue_change,
    ROUND(
        100.0 * (monthly_revenue - previous_month_revenue)
        / NULLIF(previous_month_revenue, 0),
        2
    ) AS mom_growth_percentage
FROM sales_with_previous_month
ORDER BY sales_month;


-- 4.6 Rank products by sales within each category
WITH product_category_sales AS (
    SELECT
        categoryid,
        productid,
        SUM(total_amount) AS total_sales
    FROM ecommerce_curated_db.fact_sales
    GROUP BY categoryid, productid
)
SELECT
    categoryid,
    productid,
    total_sales,
    RANK() OVER (
        PARTITION BY categoryid
        ORDER BY total_sales DESC
    ) AS product_rank
FROM product_category_sales
ORDER BY categoryid, product_rank;


-- 4.7 Top 3 products in each category
WITH product_category_sales AS (
    SELECT
        categoryid,
        productid,
        SUM(total_amount) AS total_sales
    FROM ecommerce_curated_db.fact_sales
    GROUP BY categoryid, productid
),
ranked_products AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY categoryid
            ORDER BY total_sales DESC, productid
        ) AS row_num
    FROM product_category_sales
)
SELECT
    categoryid,
    productid,
    total_sales,
    row_num
FROM ranked_products
WHERE row_num <= 3
ORDER BY categoryid, row_num;


-- 4.8 Rank customers by lifetime sales
WITH customer_sales AS (
    SELECT
        customerid,
        SUM(total_amount) AS lifetime_sales
    FROM ecommerce_curated_db.fact_sales
    GROUP BY customerid
)
SELECT
    customerid,
    lifetime_sales,
    DENSE_RANK() OVER (
        ORDER BY lifetime_sales DESC
    ) AS customer_rank
FROM customer_sales
ORDER BY customer_rank;


-- 4.9 First and latest order for each customer
WITH customer_orders AS (
    SELECT
        customerid,
        orderid,
        orderdate,
        ROW_NUMBER() OVER (
            PARTITION BY customerid
            ORDER BY orderdate ASC, orderid ASC
        ) AS first_order_rank,
        ROW_NUMBER() OVER (
            PARTITION BY customerid
            ORDER BY orderdate DESC, orderid DESC
        ) AS latest_order_rank
    FROM ecommerce_curated_db.orders
)
SELECT
    customerid,
    MAX(
        CASE WHEN first_order_rank = 1 THEN orderdate END
    ) AS first_order_date,
    MAX(
        CASE WHEN latest_order_rank = 1 THEN orderdate END
    ) AS latest_order_date
FROM customer_orders
GROUP BY customerid;


-- 4.10 Number of orders per customer over time
SELECT
    customerid,
    orderdate,
    orderid,
    ROW_NUMBER() OVER (
        PARTITION BY customerid
        ORDER BY orderdate, orderid
    ) AS customer_order_sequence
FROM ecommerce_curated_db.orders
ORDER BY customerid, customer_order_sequence;


-- 4.11 Compare each customer's sales with their previous order
WITH order_totals AS (
    SELECT
        customerid,
        orderid,
        orderdate,
        SUM(total_amount) AS order_total
    FROM ecommerce_curated_db.fact_sales
    GROUP BY customerid, orderid, orderdate
)
SELECT
    customerid,
    orderid,
    orderdate,
    order_total,
    LAG(order_total) OVER (
        PARTITION BY customerid
        ORDER BY orderdate, orderid
    ) AS previous_order_total
FROM order_totals
ORDER BY customerid, orderdate;