from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from functools import reduce

BUCKET = "samsung-aws-task"
RAW_PATH = f"s3://{BUCKET}/raw/ecommerce"
PROCESSED_PATH = f"s3://{BUCKET}/processed/ecommerce"

# Task 28: fill these with the instructor's thresholds if they were given separately.
VIP_MIN = None
PREMIUM_MIN = None
REGULAR_MIN = None

# Tasks 1-2: read all 12 CSVs and prepare them as Spark DataFrames.
table_names = [
    "customers", "categories", "products", "departments", "employees",
    "suppliers", "orders", "order_details", "payments",
    "product_suppliers", "shippers", "shipments"
]

dfs = {}
for name in table_names:
    dfs[name] = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{RAW_PATH}/{name}/{name}.csv")
    )

globals().update(dfs)

# Display every schema.
for name in table_names:
    print(f"\n===== {name} =====")
    dfs[name].printSchema()

# Tasks 3-4: display 10 records and row count for every table.
for name in table_names:
    print(f"\n===== {name}: first 10 rows =====")
    dfs[name].show(10, truncate=False)
    print(f"{name} row count = {dfs[name].count()}")

# Task 5: number of NULL values in every column of customers.
customer_nulls = customers.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in customers.columns
])
customer_nulls.show(truncate=False)

# Task 6: duplicate customer IDs.
customers.groupBy("CustomerID").count().filter(F.col("count") > 1).orderBy(F.desc("count")).show(truncate=False)

# Task 7: duplicate order IDs.
orders.groupBy("OrderID").count().filter(F.col("count") > 1).orderBy(F.desc("count")).show(truncate=False)

# Task 8: standardize every column name to lowercase snake_case.
def clean_column_names(df):
    for c in df.columns:
        new_name = (
            c.strip()
             .lower()
             .replace(" ", "_")
             .replace("-", "_")
        )
        df = df.withColumnRenamed(c, new_name)
    return df

for name in table_names:
    dfs[name] = clean_column_names(dfs[name])

globals().update(dfs)

def trim_string_columns(df):
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df

for name in table_names:
    dfs[name] = trim_string_columns(dfs[name])

globals().update(dfs)

# Task 10: lowercase customer emails.
customers = customers.withColumn("email", F.lower(F.col("email")))

# Task 11: uppercase order statuses.
orders = orders.withColumn("status", F.upper(F.col("status")))

# Task 12: remove duplicate customers based on customerid.
customers = customers.dropDuplicates(["customerid"])

# Task 13: remove duplicate orders based on orderid.
orders = orders.dropDuplicates(["orderid"])

# Task 14: remove records where the primary key is NULL.
# Primary keys used by the assignment.
pk_by_table = {
    "customers": "customerid",
    "categories": "categoryid",
    "products": "productid",
    "departments": "departmentid",
    "employees": "employeeid",
    "suppliers": "supplierid",
    "orders": "orderid",
    "order_details": "orderdetailid",
    "payments": "paymentid",
    "product_suppliers": "productsupplierid",
    "shippers": "shipperid",
    "shipments": "shipmentid",
}

for name, pk in pk_by_table.items():
    if pk in dfs[name].columns:
        dfs[name] = dfs[name].filter(F.col(pk).isNotNull())

globals().update(dfs)

# Task 15: find customers with invalid email formats.
invalid_emails = customers.filter(
    ~F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
    | F.col("email").isNull()
)
invalid_emails.show(truncate=False)

# Task 16: keep only the allowed order statuses; invalid values are marked UNKNOWN.
allowed_statuses = ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"]

orders = orders.withColumn(
    "status",
    F.when(F.col("status").isin(allowed_statuses), F.col("status"))
     .otherwise("UNKNOWN")
)

orders.filter(F.col("status") == "UNKNOWN").show(truncate=False)

# Task 17: find products with NULL prices.
products.filter(F.col("price").isNull()).show(truncate=False)

# Task 18: find products with price <= 0.
products.filter(F.col("price") <= 0).show(truncate=False)

for name in table_names:
    df = dfs[name]
    for c in df.columns:
        if c.endswith("id"):
            df = df.withColumn(c, F.col(c).cast("long"))
    dfs[name] = df

globals().update(dfs)

products = products.withColumn("price", F.col("price").cast(DecimalType(12, 2)))

# Task 21: convert orderdate to Spark date.
orders = orders.withColumn("orderdate", F.to_date("orderdate"))


payments = payments.withColumn("amount", F.col("amount").cast(DecimalType(12, 2)))


order_details = order_details.withColumn("quantity", F.col("quantity").cast("int"))

# Task 24: extract year, month, quarter, day, and day_of_week from orderdate.
orders = (
    orders
    .withColumn("year", F.year("orderdate"))
    .withColumn("month", F.month("orderdate"))
    .withColumn("quarter", F.quarter("orderdate"))
    .withColumn("day", F.dayofmonth("orderdate"))
    .withColumn("day_of_week", F.dayofweek("orderdate"))
)

# Task 25: order_details.total_amount = quantity * unitprice.
# Do validation/normalization before calculating the derived amount.
order_details = order_details.withColumn(
    "unitprice",
    F.when(F.col("unitprice") < 0, F.lit(0)).otherwise(F.col("unitprice"))
)
order_details = order_details.withColumn(
    "quantity",
    F.when(F.col("quantity") < 0, F.lit(0)).otherwise(F.col("quantity"))
)
order_details = order_details.withColumn(
    "total_amount",
    (F.col("quantity") * F.col("unitprice")).cast(DecimalType(18, 2))
)

# Task 26: create full_name in customers.
customers = customers.withColumn(
    "full_name",
    F.trim(F.concat(" ", F.col("firstname"), F.col("lastname")))
)

# Task 27: create order_value_category.
order_value_category_exp = (
    F.when(F.col("total_amount") >= 1000, "HIGH")
     .when(F.col("total_amount") >= 500, "MEDIUM")
     .otherwise("LOW")
)

order_details = order_details.withColumn(
    "order_value_category",
    order_value_category_exp
)

# Task 28: create customer_segment based on total customer sales.
# Because the PDF lists the four labels but does not display their numeric thresholds,
# fail explicitly instead of silently choosing arbitrary business thresholds.

if None in (VIP_MIN, PREMIUM_MIN, REGULAR_MIN):
    print("Set VIP_MIN, PREMIUM_MIN, and REGULAR_MIN to the instructor-provided thresholds before running this cell.")
else:
    customer_totals_for_segment = (
        orders.select("orderid", "customerid")
        .join(order_details.select("orderid", "total_amount"), "orderid", "inner")
        .groupBy("customerid")
        .agg(F.sum("total_amount").alias("total_customer_sales"))
    )

    customers = (
        customers.join(customer_totals_for_segment, "customerid", "left")
        .fillna({"total_customer_sales": 0})
        .withColumn(
            "customer_segment",
            F.when(F.col("total_customer_sales") >= VIP_MIN, "VIP")
             .when(F.col("total_customer_sales") >= PREMIUM_MIN, "PREMIUM")
             .when(F.col("total_customer_sales") >= REGULAR_MIN, "REGULAR")
             .otherwise("LOW_VALUE")
        )
    )

# Tasks 29-31: order-level total amount, total quantity, and average product price.
order_metrics = (
    order_details
    .groupBy("orderid")
    .agg(
        F.sum("total_amount").alias("total_amount"),
        F.sum("quantity").alias("total_quantity"),
        F.avg("unitprice").alias("average_product_price")
    )
)

order_metrics.show(20, truncate=False)

# Task 32: latest order for every customer using row_number().
customer_order_window = Window.partitionBy("customerid").orderBy(
    F.col("orderdate").desc_nulls_last(),
    F.col("orderid").desc()
)

latest_order_per_customer = (
    orders
    .withColumn("rn", F.row_number().over(customer_order_window))
    .filter(F.col("rn") == 1)
    .select("customerid", "orderid", "status", "orderdate")
)

latest_order_per_customer.show(truncate=False)

# Task 33: first order for every customer.
first_order_window = Window.partitionBy("customerid").orderBy(
    F.col("orderdate").asc_nulls_last(),
    F.col("orderid").asc()
)

first_order_per_customer = (
    orders
    .withColumn("rn", F.row_number().over(first_order_window))
    .filter(F.col("rn") == 1)
    .select("customerid", "orderid", "status", "orderdate")
)

first_order_per_customer.show(truncate=False)

# Task 34: rank customers by total sales.
customer_sales = (
    orders.select("orderid", "customerid")
    .join(order_details.select("orderid", "total_amount"), "orderid", "inner")
    .groupBy("customerid")
    .agg(F.sum("total_amount").alias("total_sales"))
)

sales_rank_window = Window.orderBy(F.col("total_sales").desc())
ranked_customers = customer_sales.withColumn("sales_rank", F.rank().over(sales_rank_window))
ranked_customers.show(20, truncate=False)

# Task 35: top 3 products in every category by sales amount.
product_sales_for_rank = (
    order_details
    .join(products.select("productid", "categoryid"), "productid", "inner")
    .groupBy("categoryid", "productid")
    .agg(F.sum("total_amount").alias("total_sales"))
)

product_category_window = Window.partitionBy("categoryid").orderBy(F.col("total_sales").desc())
top3_products_per_category = (
    product_sales_for_rank
    .withColumn("rank", F.row_number().over(product_category_window))
    .filter(F.col("rank") <= 3)
)

top3_products_per_category.show(50, truncate=False)

# Task 36: most expensive product in every category.
product_price_window = Window.partitionBy("categoryid").orderBy(
    F.col("price").desc_nulls_last(),
    F.col("productid")
)

most_expensive_product_per_category = (
    products
    .withColumn("rn", F.row_number().over(product_price_window))
    .filter(F.col("rn") == 1)
)

most_expensive_product_per_category.show(truncate=False)

# Task 37: latest shipment for every order.
shipment_window = Window.partitionBy("orderid").orderBy(
    F.col("shipdate").desc_nulls_last(),
    F.col("shipmentid").desc()
)

latest_shipment_per_order = (
    shipments
    .withColumn("rn", F.row_number().over(shipment_window))
    .filter(F.col("rn") == 1)
)

latest_shipment_per_order.show(truncate=False)

# Task 38: orders + customers.
orders_customers = (
    orders.alias("o")
    .join(customers.alias("c"), F.col("o.customerid") == F.col("c.customerid"), "inner")
    .select(
        F.col("o.orderid"),
        F.col("o.customerid"),
        F.col("c.full_name").alias("customer_name"),
        F.col("c.city"),
        F.col("c.country"),
        F.col("o.orderdate"),
        F.col("o.status")
    )
)

orders_customers.show(20, truncate=False)

# Task 39: orders + order_details + products => sales dataset.
sales = (
    orders.alias("o")
    .join(order_details.alias("od"), "orderid", "inner")
    .join(products.alias("p"), "productid", "inner")
    .select(
        F.col("o.orderid"),
        F.col("o.customerid"),
        F.col("o.orderdate"),
        F.col("o.status"),
        F.col("od.productid"),
        F.col("p.categoryid"),
        F.col("od.quantity"),
        F.col("od.unitprice"),
        F.col("od.total_amount")
    )
)

sales.show(20, truncate=False)

# Task 40: products + categories.
products_categories = (
    products.alias("p")
    .join(categories.alias("c"), "categoryid", "inner")
)

products_categories.show(20, truncate=False)

# Task 41: products + product_suppliers + suppliers.
products_suppliers = (
    products.alias("p")
    .join(product_suppliers.alias("ps"), "productid", "inner")
    .join(suppliers.alias("s"), "supplierid", "inner")
)

products_suppliers.show(20, truncate=False)

# Task 42: orders + payments.
orders_payments = (
    orders.alias("o")
    .join(payments.alias("p"), "orderid", "left")
)

orders_payments.show(20, truncate=False)

# Task 43: orders + shipments + shippers.
orders_shipments_shippers = (
    orders.alias("o")
    .join(shipments.alias("s"), "orderid", "left")
    .join(shippers.alias("sh"), "shipperid", "left")
)

orders_shipments_shippers.show(20, truncate=False)

# Common order-level sales view.
order_sales = (
    orders.select("orderid", "customerid", "orderdate", "status", "year", "month", "quarter", "day", "day_of_week")
    .join(order_details.select("orderid", "quantity", "unitprice", "total_amount"), "orderid", "inner")
)

# Tasks 44-47
total_sales = order_sales.agg(F.sum("total_amount").alias("total_sales"))
total_orders = orders.agg(F.countDistinct("orderid").alias("total_orders"))
total_quantity_sold = order_details.agg(F.sum("quantity").alias("total_quantity_sold"))
average_order_value = (
    order_sales.groupBy("orderid")
    .agg(F.sum("total_amount").alias("order_total"))
    .agg(F.avg("order_total").alias("average_order_value"))
)

total_sales.show()
total_orders.show()
total_quantity_sold.show()
average_order_value.show()

# Task 48: total sales by customer.
customer_sales = (
    order_sales
    .groupBy("customerid")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy(F.desc("total_sales"))
)
customer_sales.show(50, truncate=False)

# Task 49: total sales by product.
product_sales = (
    sales
    .groupBy("productid")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy(F.desc("total_sales"))
)
product_sales.show(50, truncate=False)

# Task 50: total sales by category.
category_sales = (
    sales
    .groupBy("categoryid")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy(F.desc("total_sales"))
)
category_sales.show(50, truncate=False)

# Task 51: total sales by country.
country_sales = (
    orders_customers
    .join(order_details.select("orderid", "total_amount"), "orderid", "inner")
    .groupBy("country")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy(F.desc("total_sales"))
)
country_sales.show(50, truncate=False)

# Task 52: total sales by city.
city_sales = (
    orders_customers
    .join(order_details.select("orderid", "total_amount"), "orderid", "inner")
    .groupBy("city")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy(F.desc("total_sales"))
)
city_sales.show(50, truncate=False)

# Task 53: monthly sales.
monthly_sales = (
    order_sales
    .groupBy("year", "month")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy("year", "month")
)
monthly_sales.show(100, truncate=False)

# Task 54: yearly sales.
yearly_sales = (
    order_sales
    .groupBy("year")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy("year")
)
yearly_sales.show(50, truncate=False)

# Task 55: sales by order status.
status_sales = (
    order_sales
    .groupBy("status")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy(F.desc("total_sales"))
)
status_sales.show(truncate=False)

# Task 56: number of orders per customer.
orders_per_customer = (
    orders
    .groupBy("customerid")
    .agg(F.countDistinct("orderid").alias("order_count"))
    .orderBy(F.desc("order_count"))
)
orders_per_customer.show(50, truncate=False)

# Task 57: top 10 customers by sales.
top10_customers = customer_sales.orderBy(F.desc("total_sales")).limit(10)
top10_customers.show(truncate=False)

# Task 58: top 10 products by sales.
top10_products = product_sales.orderBy(F.desc("total_sales")).limit(10)
top10_products.show(truncate=False)

# Task 59: top 5 categories by sales.
top5_categories = category_sales.orderBy(F.desc("total_sales")).limit(5)
top5_categories.show(truncate=False)

# Task 60: customers who have never placed an order.
customers_no_orders = (
    customers.alias("c")
    .join(orders.select("customerid").distinct().alias("o"), "customerid", "left_anti")
)
customers_no_orders.show(50, truncate=False)

# Task 61: products that have never been ordered.
products_never_ordered = (
    products.alias("p")
    .join(order_details.select("productid").distinct().alias("od"), "productid", "left_anti")
)
products_never_ordered.show(50, truncate=False)

# Task 62: customers with more than 10 orders.
customers_more_than_10_orders = (
    orders.groupBy("customerid")
    .agg(F.countDistinct("orderid").alias("order_count"))
    .filter(F.col("order_count") > 10)
    .orderBy(F.desc("order_count"))
)
customers_more_than_10_orders.show(50, truncate=False)

# Task 63: orders where payment amount does not match calculated order total.
calculated_order_totals = (
    order_details
    .groupBy("orderid")
    .agg(F.sum("total_amount").alias("calculated_order_total"))
)

payment_totals = (
    payments
    .groupBy("orderid")
    .agg(F.sum("amount").alias("paid_amount"))
)

payment_mismatch_orders = (
    calculated_order_totals
    .join(payment_totals, "orderid", "left")
    .filter(
        F.col("paid_amount").isNull()
        | (F.abs(F.col("paid_amount") - F.col("calculated_order_total")) > F.lit(0.01))
    )
)

payment_mismatch_orders.show(50, truncate=False)

# Task 64: orders that do not have a shipment.
orders_without_shipment = (
    orders.select("orderid")
    .join(shipments.select("orderid").distinct(), "orderid", "left_anti")
)
orders_without_shipment.show(50, truncate=False)

# Task 65: shipments that do not have a matching order.
shipments_without_matching_order = (
    shipments
    .join(orders.select("orderid").distinct(), "orderid", "left_anti")
)
shipments_without_matching_order.show(50, truncate=False)

# Task 66: daily sales.
daily_sales = (
    order_sales
    .groupBy("orderdate")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy("orderdate")
)
daily_sales.show(100, truncate=False)

# Task 67: monthly sales.
monthly_sales = (
    order_sales
    .groupBy("year", "month")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy("year", "month")
)
monthly_sales.show(100, truncate=False)

# Task 68: quarterly sales.
quarterly_sales = (
    order_sales
    .groupBy("year", "quarter")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy("year", "quarter")
)
quarterly_sales.show(100, truncate=False)

# Task 69: yearly sales.
yearly_sales = (
    order_sales
    .groupBy("year")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy("year")
)
yearly_sales.show(50, truncate=False)

# Task 70: month with the highest sales.
highest_sales_month = (
    monthly_sales
    .orderBy(F.col("total_sales").desc())
    .limit(1)
)
highest_sales_month.show(truncate=False)

# Task 71: day with the highest number of orders.
highest_order_day = (
    orders
    .groupBy("orderdate")
    .agg(F.countDistinct("orderid").alias("order_count"))
    .orderBy(F.col("order_count").desc(), F.col("orderdate"))
    .limit(1)
)
highest_order_day.show(truncate=False)

# Task 72: average number of orders per month.
monthly_order_counts = (
    orders
    .groupBy("year", "month")
    .agg(F.countDistinct("orderid").alias("order_count"))
)

average_orders_per_month = monthly_order_counts.agg(
    F.avg("order_count").alias("average_orders_per_month")
)

average_orders_per_month.show()

# Write helper.
def write_parquet(df, name, partition_cols=None):
    writer = (
        df.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(f"{PROCESSED_PATH}/{name}/")

# Required processed datasets from the assignment.
write_parquet(customers, "customers")
write_parquet(products, "products")
write_parquet(categories, "categories")
write_parquet(orders, "orders", partition_cols=["year", "month"])
write_parquet(order_details, "order_details")
write_parquet(payments, "payments")
write_parquet(shipments, "shipments")

write_parquet(sales, "fact_sales")
write_parquet(customer_sales, "customer_sales")
write_parquet(product_sales, "product_sales")
write_parquet(category_sales, "category_sales")
write_parquet(monthly_sales, "monthly_sales")

print(f"Processed data written under: {PROCESSED_PATH}")

# Final fact_sales output aligned with the requested model.
fact_sales = sales.select(
    "orderid",
    "customerid",
    "productid",
    "categoryid",
    "orderdate",
    "quantity",
    "unitprice",
    "total_amount",
    "status"
)

fact_sales.show(20, truncate=False)

# Optional final verification.
output_datasets = [
    "customers", "products", "categories", "orders",
    "order_details", "payments", "shipments",
    "fact_sales", "customer_sales", "product_sales",
    "category_sales", "monthly_sales"
]

for dataset in output_datasets:
    print(f"{dataset}: {PROCESSED_PATH}/{dataset}/")

job.commit()