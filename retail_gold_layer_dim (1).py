# Databricks notebook source
# MAGIC %md
# MAGIC ##Configure the Gold layer

# COMMAND ----------

# MAGIC %md
# MAGIC # GOLD LAYER - DIMENSION TABLES

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "capretailnew"

SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

print("Gold schema created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load Silver master tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Master Tables

# COMMAND ----------

products = spark.table(
    f"{SILVER}.silver_product_master"
)

stores = spark.table(
    f"{SILVER}.silver_store_master"
)

suppliers = spark.table(
    f"{SILVER}.silver_supplier_master"
)

print("Products:", products.count())
print("Stores:", stores.count())
print("Suppliers:", suppliers.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inspect master schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Silver Master Schemas

# COMMAND ----------

print("PRODUCT MASTER")
products.printSchema()

print("STORE MASTER")
stores.printSchema()

print("SUPPLIER MASTER")
suppliers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Product Dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Product Dimension

# COMMAND ----------

gold_dim_product = products.select(
    "product_id",
    "sku",
    "product_name",
    "category",
    "subcategory",
    "unit_of_measure",
    "list_price",
    "cost_price",
    "supplier_id",
    "active_flag"
)

print("Product dimension prepared")

display(gold_dim_product.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Store Dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Store Dimension

# COMMAND ----------

gold_dim_store = stores.select(
    "store_id",
    "store_name",
    "city",
    "region",
    "store_type",
    "opening_date",
    "active_flag",
    "warehouse_id",
    "manager_code",
    "country"
)

print("Store dimension prepared")

display(gold_dim_store.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Supplier Dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Supplier Dimension

# COMMAND ----------

gold_dim_supplier = suppliers.select(
    "supplier_id",
    "supplier_name",
    "country",
    "lead_time_days",
    "payment_terms",
    "active_flag",
    "supplier_tier",
    "city",
    "category",
    "contact_code"
)

print("Supplier dimension prepared")

display(gold_dim_supplier.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate dimension primary keys

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Dimension Primary Keys

# COMMAND ----------

dimension_checks = [
    ("Product", gold_dim_product, "product_id"),
    ("Store", gold_dim_store, "store_id"),
    ("Supplier", gold_dim_supplier, "supplier_id")
]

for name, df, key in dimension_checks:

    total_records = df.count()

    null_keys = df.filter(
        F.col(key).isNull() |
        (F.trim(F.col(key)) == "")
    ).count()

    duplicate_keys = (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"\n{name} Dimension")
    print("Total Records:", total_records)
    print("Null Keys:", null_keys)
    print("Duplicate Key Groups:", duplicate_keys)

    if null_keys > 0 or duplicate_keys > 0:
        raise ValueError(
            f"{name} dimension validation failed"
        )

print("\nAll dimension primary keys are valid.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate Product-to-Supplier relationship

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Product-Supplier Relationship

# COMMAND ----------

supplier_keys = gold_dim_supplier.select(
    "supplier_id"
).distinct()

unmatched_products = gold_dim_product.join(
    supplier_keys,
    on="supplier_id",
    how="left_anti"
)

print(
    "Products without matching suppliers:",
    unmatched_products.count()
)

display(unmatched_products)

if unmatched_products.count() > 0:
    raise ValueError(
        "Product-Supplier relationship validation failed"
    )

print("Product-Supplier relationship validated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save all Gold dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Gold Dimension Tables

# COMMAND ----------

(
    gold_dim_product.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD}.gold_dim_product")
)

(
    gold_dim_store.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD}.gold_dim_store")
)

(
    gold_dim_supplier.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD}.gold_dim_supplier")
)

print("All Gold dimension tables saved successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dimension validation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Gold Dimension Validation

# COMMAND ----------

gold_dimension_tables = [
    "gold_dim_product",
    "gold_dim_store",
    "gold_dim_supplier"
]

for table_name in gold_dimension_tables:

    df = spark.table(f"{GOLD}.{table_name}")
    print("TABLE:", table_name)
    print("RECORD COUNT:", df.count())
    print("COLUMNS:", df.columns)

    display(df.limit(5))