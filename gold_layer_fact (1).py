# Databricks notebook source
# MAGIC %md
# MAGIC ##Import libraries and configure tables

# COMMAND ----------

from pyspark.sql import functions as F

# Database names
CATALOG = "capretailnew"

SILVER = CATALOG + ".silver"
GOLD = CATALOG + ".gold"

# Create Gold schema if it does not exist
spark.sql("CREATE SCHEMA IF NOT EXISTS " + GOLD)

print("Gold schema is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load Silver tables and Gold dimensions

# COMMAND ----------

# Silver source tables
sales = spark.table(SILVER + ".silver_sales_order_lines")

returns = spark.table(SILVER + ".silver_sales_returns")

inventory = spark.table(SILVER + ".silver_inventory_movements")


# Gold dimension tables
product_dim = spark.table(GOLD + ".gold_dim_product")

store_dim = spark.table(GOLD + ".gold_dim_store")


print("Sales records:", sales.count())
print("Returns records:", returns.count())
print("Inventory records:", inventory.count())

print("Product dimension:", product_dim.count())
print("Store dimension:", store_dim.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Analyse sales order statuses

# COMMAND ----------

status_summary = (
    sales
    .groupBy("order_status")
    .agg(
        F.count("*").alias("total_lines"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity")
    )
    .orderBy("order_status")
)

display(status_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calculate sales measures

# COMMAND ----------

gold_fact_sales = (
    sales

    # Calculate gross sales
    .withColumn(
        "gross_sales",
        (
            F.col("quantity") * F.col("unit_price")
        ).cast("decimal(18,2)")
    )

    # Calculate discount amount
    .withColumn(
        "discount_amount",
        (
            F.col("gross_sales") *
            F.col("discount_pct") / 100
        ).cast("decimal(18,2)")
    )

    # Calculate sales after discount, before tax
    .withColumn(
        "sales_after_discount_before_tax",
        (
            F.col("gross_sales") -
            F.col("discount_amount")
        ).cast("decimal(18,2)")
    )

    # Calculate sales including tax, before refunds
    .withColumn(
        "sales_including_tax_before_refunds",
        (
            F.col("sales_after_discount_before_tax") +
            F.col("tax_amount")
        ).cast("decimal(18,2)")
    )
)

display(gold_fact_sales.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Join product dimension

# COMMAND ----------

product_lookup = product_dim.select(
    "product_id",
    "product_name",
    "category",
    "subcategory"
)

gold_fact_sales = gold_fact_sales.join(
    product_lookup,
    on="product_id",
    how="left"
)

display(gold_fact_sales.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Join store dimension and save Sales fact

# COMMAND ----------

store_lookup = store_dim.select(
    "store_id",
    "store_name",
    "city",
    "region"
)

gold_fact_sales = gold_fact_sales.join(
    store_lookup,
    on="store_id",
    how="left"
)

# Save Gold sales fact
gold_fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(GOLD + ".gold_fact_sales")

print("Gold Fact Sales created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Preview and validate Sales fact

# COMMAND ----------

gold_fact_sales = spark.table(GOLD + ".gold_fact_sales")

print("Silver sales count:", sales.count())
print("Gold sales count:", gold_fact_sales.count())

display(
    gold_fact_sales.select(
        "order_id",
        "order_line_id",
        "product_id",
        "product_name",
        "store_id",
        "store_name",
        "quantity",
        "gross_sales",
        "discount_amount",
        "sales_after_discount_before_tax",
        "tax_amount",
        "sales_including_tax_before_refunds"
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate sales totals

# COMMAND ----------

display(
    gold_fact_sales.agg(
        F.sum("gross_sales").alias("total_gross_sales"),
        F.sum("discount_amount").alias("total_discount"),
        F.sum("sales_after_discount_before_tax").alias(
            "total_sales_after_discount"
        ),
        F.sum("tax_amount").alias("total_tax"),
        F.sum("sales_including_tax_before_refunds").alias(
            "total_sales_including_tax"
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Gold Fact Returns

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select original sales information

# COMMAND ----------

# Select original sales information
sales_for_returns = sales.select(
    "order_line_id",
    F.col("order_id").alias("original_order_id"),
    F.col("order_ts").alias("original_order_ts"),
    F.col("quantity").alias("original_quantity"),
    F.col("order_status").alias("original_order_status")
)

# Join returns with original sales
gold_fact_returns = returns.join(
    sales_for_returns,
    on="order_line_id",
    how="left"
)

print("Returns joined with original sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Join returns with original sales

# COMMAND ----------

return_product_lookup = product_dim.select(
    "product_id",
    "product_name",
    "category"
)

gold_fact_returns = gold_fact_returns.join(
    return_product_lookup,
    on="product_id",
    how="left"
)

print("Product details added")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add product details

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add store details and save Returns fact

# COMMAND ----------

return_store_lookup = store_dim.select(
    "store_id",
    "store_name",
    F.col("region").alias("store_region")
)

gold_fact_returns = gold_fact_returns.join(
    return_store_lookup,
    on="store_id",
    how="left"
)

# Check column names before saving
print(gold_fact_returns.columns)

# Save the final table
gold_fact_returns.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(GOLD + ".gold_fact_returns")

print("Gold Fact Returns created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate Returns Fact

# COMMAND ----------

gold_fact_returns = spark.table(
    GOLD + ".gold_fact_returns"
)

print("Silver Returns:", returns.count())
print("Gold Returns:", gold_fact_returns.count())

print(
    "Duplicate Return IDs:",
    gold_fact_returns
    .groupBy("return_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Check sales link status
display(
    gold_fact_returns
    .groupBy("sales_link_status")
    .count()
)

# Check approved returns
display(
    gold_fact_returns
    .groupBy("approved_flag")
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Gold Fact Inventory Movements

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load the Silver inventory table

# COMMAND ----------

inventory = spark.table(
    SILVER + ".silver_inventory_movements"
)

print("Total Inventory Records:", inventory.count())

display(inventory.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Join product dimension

# COMMAND ----------

inventory_product_lookup = product_dim.select(
    "product_id",
    "product_name",
    "category",
    "unit_of_measure"
)

gold_fact_inventory = inventory.join(
    inventory_product_lookup,
    on="product_id",
    how="left"
)

display(gold_fact_inventory.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Join store dimension

# COMMAND ----------

inventory_store_lookup = store_dim.select(
    "store_id",
    "store_name",
    "region",
    F.col("warehouse_id").alias("store_warehouse_id")
)

gold_fact_inventory = gold_fact_inventory.join(
    inventory_store_lookup,
    on="store_id",
    how="left"
)

display(gold_fact_inventory.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create movement date

# COMMAND ----------

gold_fact_inventory = gold_fact_inventory.withColumn(
    "movement_date",
    F.to_date("movement_ts")
)

display(
    gold_fact_inventory.select(
        "movement_id",
        "movement_ts",
        "movement_date",
        "movement_type",
        "product_id",
        "product_name",
        "quantity_change",
        "unit_of_measure",
        "store_id",
        "store_name"
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save Gold inventory fact

# COMMAND ----------

gold_fact_inventory.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        GOLD + ".gold_fact_inventory_movements"
    )

print("Gold Fact Inventory Movements created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate the table

# COMMAND ----------

gold_fact_inventory = spark.table(
    GOLD + ".gold_fact_inventory_movements"
)

print("Silver Inventory:", inventory.count())

print("Gold Inventory:", gold_fact_inventory.count())

print(
    "Duplicate Movement IDs:",
    gold_fact_inventory
    .groupBy("movement_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Analyse movement types

# COMMAND ----------

display(
    gold_fact_inventory
    .groupBy("movement_type")
    .agg(
        F.count("*").alias("total_movements"),
        F.sum("quantity_change").alias("total_quantity_change")
    )
    .orderBy("movement_type")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check units of measure and missing references

# COMMAND ----------

# Check product UOM
display(
    gold_fact_inventory
    .groupBy("unit_of_measure")
    .count()
)

# Check missing product details
print(
    "Missing Product Names:",
    gold_fact_inventory.filter(
        F.col("product_name").isNull()
    ).count()
)

# Check missing store details
print(
    "Missing Store Names:",
    gold_fact_inventory.filter(
        F.col("store_name").isNull()
    ).count()
)