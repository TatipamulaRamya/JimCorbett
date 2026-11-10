# Databricks notebook source
# MAGIC %md
# MAGIC ##Import libraries and configure tables

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "capretailnew"

SILVER = CATALOG + ".silver"
GOLD = CATALOG + ".gold"

print("Business notebook started")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load all required tables

# COMMAND ----------

# Silver source tables
sales = spark.table(SILVER + ".silver_sales_order_lines")

returns = spark.table(SILVER + ".silver_sales_returns")

inventory = spark.table(SILVER + ".silver_inventory_movements")

products = spark.table(SILVER + ".silver_product_master")


# Gold fact tables
gold_fact_sales = spark.table(GOLD + ".gold_fact_sales")

gold_fact_returns = spark.table(GOLD + ".gold_fact_returns")

gold_fact_inventory = spark.table(
    GOLD + ".gold_fact_inventory_movements"
)


# Gold dimension tables
product_dim = spark.table(GOLD + ".gold_dim_product")

store_dim = spark.table(GOLD + ".gold_dim_store")


print("All required tables loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Analyse inventory movement types

# COMMAND ----------

movement_summary = (
    gold_fact_inventory
    .groupBy("movement_type")
    .agg(
        F.count("*").alias("record_count"),
        F.sum("quantity_change").alias("total_quantity_change"),
        F.min("quantity_change").alias("minimum_quantity"),
        F.max("quantity_change").alias("maximum_quantity")
    )
    .orderBy("movement_type")
)

display(movement_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inspect receipt and transfer records

# COMMAND ----------

display(
    gold_fact_inventory
    .filter(
        F.col("movement_type").isin("RECEIPT", "TRANSFER")
    )
    .select(
        "movement_id",
        "movement_type",
        "product_id",
        "store_id",
        "movement_ts",
        "quantity_change",
        "unit_cost"
    )
    .orderBy("movement_type", "movement_ts")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create inventory movement ledger

# COMMAND ----------

# Define the order of inventory movements
inventory_window = (
    Window
    .partitionBy("product_id", "store_id")
    .orderBy("movement_ts", "movement_id")
)

# Calculate cumulative quantity change
gold_inventory_ledger = (
    gold_fact_inventory

    # Assign sequence number to each movement
    .withColumn(
        "movement_sequence",
        F.row_number().over(inventory_window)
    )

    # Calculate cumulative quantity change
    .withColumn(
        "cumulative_quantity_change",
        F.sum("quantity_change").over(
            inventory_window.rowsBetween(
                Window.unboundedPreceding,
                Window.currentRow
            )
        )
    )
)

display(gold_inventory_ledger.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save inventory movement ledger

# COMMAND ----------

gold_inventory_ledger.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        GOLD + ".gold_inventory_movement_ledger"
    )

print("Inventory Movement Ledger created")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create daily sales summary

# COMMAND ----------

gold_daily_sales_summary = (
    gold_fact_sales

    # Extract date from order timestamp
    .withColumn(
        "sales_date",
        F.to_date("order_ts")
    )

    # Group sales by date, store and category
    .groupBy(
        "sales_date",
        "store_id",
        "store_name",
        "category"
    )

    # Calculate daily sales measures
    .agg(
        F.countDistinct("order_id").alias("total_orders"),

        F.count("order_line_id").alias("total_order_lines"),

        F.sum("quantity").alias("total_quantity_sold"),

        F.sum("gross_sales").alias("gross_sales"),

        F.sum("discount_amount").alias("total_discount"),

        F.sum(
            "sales_after_discount_before_tax"
        ).alias("sales_after_discount_before_tax")
    )
)

display(gold_daily_sales_summary.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save daily sales summary

# COMMAND ----------

gold_daily_sales_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        GOLD + ".gold_daily_sales_summary"
    )

print("Daily Sales Summary created")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Store Sales KPIs

# COMMAND ----------

gold_store_sales_kpis = (
    gold_fact_sales

    # Group sales by store
    .groupBy(
        "store_id",
        "store_name",
        "city",
        "region"
    )

    # Calculate store-level measures
    .agg(
        F.countDistinct("order_id").alias("total_orders"),

        F.count("order_line_id").alias("total_order_lines"),

        F.sum("quantity").alias("total_quantity_sold"),

        F.sum("gross_sales").alias("gross_sales"),

        F.sum("discount_amount").alias("total_discount"),

        F.sum(
            "sales_after_discount_before_tax"
        ).alias("total_sales_after_discount")
    )
)

display(gold_store_sales_kpis.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save Store Sales KPIs

# COMMAND ----------

gold_store_sales_kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        GOLD + ".gold_store_sales_kpis"
    )

print("Store Sales KPIs created")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check inventory quantities by product and UOM

# COMMAND ----------

gold_uom_check = (
    gold_fact_inventory
    .groupBy(
        "product_id",
        "product_name",
        "unit_of_measure"
    )
    .agg(
        F.count("*").alias("total_movements"),
        F.sum("quantity_change").alias("total_quantity_change")
    )
)

display(gold_uom_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check missing UOM in inventory

# COMMAND ----------

missing_inventory_uom = gold_fact_inventory.filter(
    F.col("unit_of_measure").isNull()
    | (F.trim(F.col("unit_of_measure")) == "")
)

print(
    "Inventory records with missing UOM:",
    missing_inventory_uom.count()
)

display(missing_inventory_uom)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check UOM in product master

# COMMAND ----------

display(
    products
    .groupBy("unit_of_measure")
    .agg(
        F.countDistinct("product_id").alias("product_count")
    )
)

# COMMAND ----------

missing_product_uom = products.filter(
    F.col("unit_of_measure").isNull()
    | (F.trim(F.col("unit_of_measure")) == "")
)

print(
    "Products with missing UOM:",
    missing_product_uom.count()
)

display(missing_product_uom)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate fact-to-dimension references
# MAGIC ##Prepare dimension keys

# COMMAND ----------

product_keys = product_dim.select("product_id").distinct()

store_keys = store_dim.select("store_id").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check Sales references

# COMMAND ----------

sales_missing_products = sales.join(
    product_keys,
    on="product_id",
    how="left_anti"
)

sales_missing_stores = sales.join(
    store_keys,
    on="store_id",
    how="left_anti"
)

print(
    "Sales without product:",
    sales_missing_products.count()
)

print(
    "Sales without store:",
    sales_missing_stores.count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check Inventory and Returns references

# COMMAND ----------

# Inventory checks
inventory_missing_products = inventory.join(
    product_keys,
    on="product_id",
    how="left_anti"
)

inventory_missing_stores = inventory.join(
    store_keys,
    on="store_id",
    how="left_anti"
)


# Returns checks
returns_missing_products = returns.join(
    product_keys,
    on="product_id",
    how="left_anti"
)

returns_missing_stores = returns.join(
    store_keys,
    on="store_id",
    how="left_anti"
)


print("Inventory without product:", inventory_missing_products.count())
print("Inventory without store:", inventory_missing_stores.count())

print("Returns without product:", returns_missing_products.count())
print("Returns without store:", returns_missing_stores.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sales fact validation
# MAGIC ##Compare counts before and after dimension joins

# COMMAND ----------

sales_before_join = sales.count()

sales_after_join = gold_fact_sales.count()

print("Sales before dimension joins:", sales_before_join)
print("Sales after dimension joins:", sales_after_join)

if sales_before_join == sales_after_join:
    print("Join validation PASSED")
else:
    print("Join validation FAILED")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reconcile Silver and Gold sales measures

# COMMAND ----------

silver_sales_totals = sales.agg(
    F.sum(
        F.col("quantity") * F.col("unit_price")
    ).alias("gross_sales"),

    F.sum(
        F.col("quantity") *
        F.col("unit_price") *
        F.col("discount_pct") / 100
    ).alias("discount_amount"),

    F.sum("tax_amount").alias("tax_amount")
)

gold_sales_totals = gold_fact_sales.agg(
    F.sum("gross_sales").alias("gross_sales"),

    F.sum("discount_amount").alias("discount_amount"),

    F.sum(
        "sales_after_discount_before_tax"
    ).alias("sales_after_discount_before_tax"),

    F.sum("tax_amount").alias("tax_amount")
)

print("Silver sales totals")
display(silver_sales_totals)

print("Gold sales totals")
display(gold_sales_totals)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reconcile daily sales summary

# COMMAND ----------

fact_total = gold_fact_sales.agg(
    F.sum("gross_sales").alias("fact_gross"),
    F.sum("discount_amount").alias("fact_discount"),
    F.sum(
        "sales_after_discount_before_tax"
    ).alias("fact_sales_after_discount")
)

daily_total = gold_daily_sales_summary.agg(
    F.sum("gross_sales").alias("daily_gross"),
    F.sum("total_discount").alias("daily_discount"),
    F.sum(
        "sales_after_discount_before_tax"
    ).alias("daily_sales_after_discount")
)

display(fact_total.crossJoin(daily_total))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sales measure quality checks

# COMMAND ----------

invalid_sales = gold_fact_sales.filter(
    F.col("gross_sales").isNull()
    | F.col("discount_amount").isNull()
    | F.col("sales_after_discount_before_tax").isNull()
    | F.col("discount_pct").isNull()
    | (F.col("discount_pct") < 0)
    | (F.col("discount_pct") > 100)
    | (F.col("quantity") <= 0)
    | (F.col("unit_price") < 0)
)

print("Invalid sales records:", invalid_sales.count())

display(
    invalid_sales.select(
        "order_line_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "gross_sales",
        "discount_amount",
        "sales_after_discount_before_tax"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final Gold table validation

# COMMAND ----------

gold_tables = [
    "gold_dim_product",
    "gold_dim_store",
    "gold_dim_supplier",
    "gold_fact_sales",
    "gold_fact_returns",
    "gold_fact_inventory_movements",
    "gold_inventory_movement_ledger",
    "gold_daily_sales_summary",
    "gold_store_sales_kpis"
]

for table_name in gold_tables:

    table_data = spark.table(GOLD + "." + table_name)

    print(
        table_name,
        "| Records:",
        table_data.count()
    )