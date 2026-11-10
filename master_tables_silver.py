# Databricks notebook source
# MAGIC %md
# MAGIC ##Read the three Bronze tables

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, expr, lit
from pyspark.sql import functions as F

product_bronze = spark.table(
    "capretailnew.bronze.bronze_product_master"
)

store_bronze = spark.table(
    "capretailnew.bronze.bronze_store_master"
)

supplier_bronze = spark.table(
    "capretailnew.bronze.bronze_supplier_master"
)

print("Product:", product_bronze.count())
print("Store:", store_bronze.count())
print("Supplier:", supplier_bronze.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transform Product Master

# COMMAND ----------

product_silver = (
    product_bronze
    .select(
        trim(col("product_id")).alias("product_id"),
        trim(col("sku")).alias("sku"),
        trim(col("product_name")).alias("product_name"),
        trim(col("category")).alias("category"),
        trim(col("subcategory")).alias("subcategory"),
        trim(col("unit_of_measure")).alias("unit_of_measure"),
        expr("try_cast(trim(list_price) AS DECIMAL(18,2))").alias("list_price"),
        expr("try_cast(trim(cost_price) AS DECIMAL(18,2))").alias("cost_price"),
        trim(col("supplier_id")).alias("supplier_id"),
        upper(trim(col("active_flag"))).alias("active_flag")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transform Store Master

# COMMAND ----------

store_silver = (
    store_bronze
    .select(
        trim(col("store_id")).alias("store_id"),
        trim(col("store_name")).alias("store_name"),
        trim(col("city")).alias("city"),
        trim(col("region")).alias("region"),
        trim(col("store_type")).alias("store_type"),
        expr("try_cast(trim(opening_date) AS DATE)").alias("opening_date"),
        upper(trim(col("active_flag"))).alias("active_flag"),
        trim(col("warehouse_id")).alias("warehouse_id"),
        trim(col("manager_code")).alias("manager_code"),
        trim(col("country")).alias("country")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transform Supplier Master

# COMMAND ----------

supplier_silver = (
    supplier_bronze
    .select(
        trim(col("supplier_id")).alias("supplier_id"),
        trim(col("supplier_name")).alias("supplier_name"),
        trim(col("country")).alias("country"),
        expr("try_cast(trim(lead_time_days) AS INT)").alias("lead_time_days"),
        trim(col("payment_terms")).alias("payment_terms"),
        upper(trim(col("active_flag"))).alias("active_flag"),
        trim(col("supplier_tier")).alias("supplier_tier"),
        trim(col("city")).alias("city"),
        trim(col("category")).alias("category"),
        trim(col("contact_code")).alias("contact_code")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate casting and primary keys

# COMMAND ----------

def validate_master(df, key, cast_columns):
    total = df.count()

    missing_keys = df.filter(
        col(key).isNull() | (trim(col(key)) == "")
    ).count()

    duplicate_keys = (
        df.groupBy(key)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print("\nTable:", key)
    print("Total records:", total)
    print("Missing keys:", missing_keys)
    print("Duplicate key groups:", duplicate_keys)

    for column_name in cast_columns:
        invalid = df.filter(col(column_name).isNull()).count()
        print("NULL / failed cast in", column_name, ":", invalid)


validate_master(
    product_silver,
    "product_id",
    ["list_price", "cost_price"]
)

validate_master(
    store_silver,
    "store_id",
    ["opening_date"]
)

validate_master(
    supplier_silver,
    "supplier_id",
    ["lead_time_days"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate product-to-supplier relationship

# COMMAND ----------

supplier_ids = (
    supplier_silver
    .select("supplier_id")
    .distinct()
    .withColumn("supplier_exists", lit(True))
)

product_supplier_validation = product_silver.join(
    supplier_ids,
    on="supplier_id",
    how="left"
)

display(
    product_supplier_validation
    .groupBy("supplier_exists")
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save the three Silver master tables
# MAGIC

# COMMAND ----------

(product_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("capretailnew.silver.silver_product_master")
)

(store_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("capretailnew.silver.silver_store_master")
)

(supplier_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("capretailnew.silver.silver_supplier_master")
)

print("All three Silver master tables created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Layer Validation

# COMMAND ----------

sales = spark.table("capretailnew.silver.silver_sales_order_lines")
inventory = spark.table("capretailnew.silver.silver_inventory_movements")
returns = spark.table("capretailnew.silver.silver_sales_returns")

products = spark.table("capretailnew.silver.silver_product_master")
stores = spark.table("capretailnew.silver.silver_store_master")
suppliers = spark.table("capretailnew.silver.silver_supplier_master")

print("All six Silver tables loaded.")
table_counts = [
    ("Sales Silver", sales.count(), 11320),
    ("Sales Quarantine", spark.table(
        "capretailnew.silver.quarantine_sales_order_lines"
    ).count(), 53),
    ("Sales Duplicate Audit", spark.table(
        "capretailnew.silver.sales_order_lines_duplicate_audit"
    ).count(), 39),
    ("Inventory Silver", inventory.count(), 634),
    ("Inventory Quarantine", spark.table(
        "capretailnew.silver.quarantine_inventory_movements"
    ).count(), 0),
    ("Returns Silver", returns.count(), 316),
    ("Returns Quarantine", spark.table(
        "capretailnew.silver.quarantine_sales_returns"
    ).count(), 0),
    ("Product Silver", products.count(), 139),
    ("Store Silver", stores.count(), 148),
    ("Supplier Silver", suppliers.count(), 157)
]

for name, actual, expected in table_counts:
    print(
        name,
        "| Actual:", actual,
        "| Expected:", expected,
        "| Status:", "PASS" if actual == expected else "CHECK"
    )
    primary_keys = [
    (sales, "order_line_id", "Sales"),
    (inventory, "movement_id", "Inventory"),
    (returns, "return_id", "Returns"),
    (products, "product_id", "Product"),
    (stores, "store_id", "Store"),
    (suppliers, "supplier_id", "Supplier")
]

for df, key, name in primary_keys:
    duplicate_count = (
        df.groupBy(key)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    missing_count = df.filter(
        col(key).isNull() | (trim(col(key)) == "")
    ).count()

    print(
        name,
        "| Duplicate key groups:", duplicate_count,
        "| Missing keys:", missing_count
    )
    

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate foreign-key relationships

# COMMAND ----------

def check_orphans(source_df, source_column, target_df, target_column, label):
    target_keys = (
        target_df.select(col(target_column).alias("_target_key"))
        .distinct()
    )

    orphan_count = (
        source_df
        .join(
            target_keys,
            source_df[source_column] == target_keys["_target_key"],
            "left_anti"
        )
        .filter(
            col(source_column).isNotNull() &
            (trim(col(source_column)) != "")
        )
        .count()
    )

    print(label, "| Orphan records:", orphan_count)


# Sales relationships
check_orphans(sales, "product_id", products, "product_id", "Sales -> Product")
check_orphans(sales, "store_id", stores, "store_id", "Sales -> Store")

# Inventory relationships
check_orphans(inventory, "product_id", products, "product_id", "Inventory -> Product")
check_orphans(inventory, "store_id", stores, "store_id", "Inventory -> Store")

# Returns relationships
check_orphans(returns, "product_id", products, "product_id", "Returns -> Product")
check_orphans(returns, "store_id", stores, "store_id", "Returns -> Store")

# Product master relationship
check_orphans(products, "supplier_id", suppliers, "supplier_id", "Product -> Supplier")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate Returns-to-Sales relationship

# COMMAND ----------

sales_quarantine = spark.table(
    "capretailnew.silver.quarantine_sales_order_lines"
)

clean_sales_ids = sales.select("order_line_id").distinct()
quarantined_sales_ids = sales_quarantine.select("order_line_id").distinct()

return_clean_match = returns.join(
    clean_sales_ids,
    on="order_line_id",
    how="left_semi"
).count()

return_quarantine_match = returns.join(
    quarantined_sales_ids,
    on="order_line_id",
    how="left_semi"
).count()

print("Returns linked to clean Sales:", return_clean_match)
print("Returns linked to quarantined Sales:", return_quarantine_match)
print("Total accounted returns:", return_clean_match + return_quarantine_match)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Review chronology flags

# COMMAND ----------

display(
    returns.groupBy(
        "sales_link_status",
        "return_before_order"
    )
    .count()
    .orderBy("sales_link_status", "return_before_order")
)

# COMMAND ----------


from pyspark.sql.functions import (
    col, count, countDistinct, sum, when, trim,
    lower, to_timestamp, lit
)

CATALOG = "capretailnew"

# Load Silver tables
sales = spark.table(CATALOG + ".silver.silver_sales_order_lines")
inventory = spark.table(CATALOG + ".silver.silver_inventory_movements")
returns = spark.table(CATALOG + ".silver.silver_sales_returns")
products = spark.table(CATALOG + ".silver.silver_product_master")
stores = spark.table(CATALOG + ".silver.silver_store_master")
suppliers = spark.table(CATALOG + ".silver.silver_supplier_master")

results = []

def report(name, total, issue_count, description):
    results.append((name, total, issue_count, description))
    print(
        name,
        "| Total:", total,
        "| Issues:", issue_count,
        "|", description
    )

print("=" * 90)
print("FINAL RETAIL PROJECT DATA QUALITY INVESTIGATION")
print("=" * 90)

# --------------------------------------------------
# 1. PRIMARY KEY VALIDATION
# --------------------------------------------------

print("\n1. PRIMARY KEY VALIDATION")

primary_keys = [
    (sales, "order_line_id", "Sales"),
    (inventory, "movement_id", "Inventory"),
    (returns, "return_id", "Returns"),
    (products, "product_id", "Products"),
    (stores, "store_id", "Stores"),
    (suppliers, "supplier_id", "Suppliers")
]

for df, key, name in primary_keys:
    total = df.count()

    missing = df.filter(
        col(key).isNull() | (trim(col(key)) == "")
    ).count()

    duplicates = (
        df.groupBy(key)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    report(name + " missing " + key, total, missing, "Missing keys")
    report(name + " duplicate " + key, total, duplicates, "Duplicate key groups")


# --------------------------------------------------
# 2. FOREIGN KEY VALIDATION
# --------------------------------------------------

print("\n2. FOREIGN KEY VALIDATION")

relationships = [
    (sales, "product_id", products, "product_id", "Sales -> Products"),
    (sales, "store_id", stores, "store_id", "Sales -> Stores"),
    (inventory, "product_id", products, "product_id", "Inventory -> Products"),
    (inventory, "store_id", stores, "store_id", "Inventory -> Stores"),
    (returns, "product_id", products, "product_id", "Returns -> Products"),
    (returns, "store_id", stores, "store_id", "Returns -> Stores"),
    (products, "supplier_id", suppliers, "supplier_id", "Products -> Suppliers")
]

for child, child_key, parent, parent_key, name in relationships:

    parent_ids = parent.select(
        col(parent_key).alias("parent_id")
    ).distinct()

    unmatched = (
        child.filter(col(child_key).isNotNull())
        .join(
            parent_ids,
            col(child_key) == col("parent_id"),
            "left_anti"
        )
        .count()
    )

    report(name, child.count(), unmatched, "Unmatched foreign keys")


# --------------------------------------------------
# 3. WAREHOUSE ID CONSISTENCY
# --------------------------------------------------

print("\n3. WAREHOUSE ID CONSISTENCY")

store_warehouse = stores.select(
    "store_id",
    col("warehouse_id").alias("master_warehouse_id")
)

warehouse_check = (
    inventory
    .join(store_warehouse, "store_id", "left")
)

missing_store = warehouse_check.filter(
    col("master_warehouse_id").isNull()
).count()

warehouse_mismatch = warehouse_check.filter(
    col("warehouse_id").isNotNull() &
    col("master_warehouse_id").isNotNull() &
    (col("warehouse_id") != col("master_warehouse_id"))
).count()

missing_inventory_warehouse = warehouse_check.filter(
    col("warehouse_id").isNull()
).count()

print("Inventory rows:", inventory.count())
print("Store warehouse missing:", missing_store)
print("Inventory warehouse missing:", missing_inventory_warehouse)
print("Warehouse assignment disagreements:", warehouse_mismatch)

display(
    warehouse_check.filter(
        col("master_warehouse_id").isNull() |
        col("warehouse_id").isNull() |
        (col("warehouse_id") != col("master_warehouse_id"))
    ).select(
        "movement_id",
        "store_id",
        "warehouse_id",
        "master_warehouse_id",
        "movement_type",
        "reference_id"
    )
)


# --------------------------------------------------
# 4. RETURNS TO SALES ID VALIDATION
# --------------------------------------------------

print("\n4. RETURNS TO SALES VALIDATION")

sales_parent = sales.select(
    "order_line_id",
    col("order_id").alias("parent_order_id"),
    col("product_id").alias("sales_product_id"),
    col("store_id").alias("sales_store_id"),
    col("order_ts")
)

return_check = returns.join(
    sales_parent,
    "order_line_id",
    "left"
)

missing_parent = return_check.filter(
    col("parent_order_id").isNull()
).count()

product_mismatch = return_check.filter(
    col("parent_order_id").isNotNull() &
    (col("product_id") != col("sales_product_id"))
).count()

store_mismatch = return_check.filter(
    col("parent_order_id").isNotNull() &
    (col("store_id") != col("sales_store_id"))
).count()

return_before_order = return_check.filter(
    col("order_ts").isNotNull() &
    col("return_ts").isNotNull() &
    (col("return_ts") < col("order_ts"))
).count()

print("Total returns:", returns.count())
print("Missing clean Sales parent:", missing_parent)
print("Product ID mismatches:", product_mismatch)
print("Store ID mismatches:", store_mismatch)
print("Return before order:", return_before_order)

display(
    return_check.filter(
        col("parent_order_id").isNull() |
        (col("product_id") != col("sales_product_id")) |
        (col("store_id") != col("sales_store_id")) |
        (col("return_ts") < col("order_ts"))
    ).select(
        "return_id",
        "order_line_id",
        "parent_order_id",
        "product_id",
        "sales_product_id",
        "store_id",
        "sales_store_id",
        "return_ts",
        "order_ts",
        "sales_link_status"
    )
)


# --------------------------------------------------
# 5. INVENTORY REFERENCE ID INVESTIGATION
# --------------------------------------------------

print("\n5. INVENTORY REFERENCE ID INVESTIGATION")

reference_checks = [
    (sales, "order_id", "Sales order_id"),
    (sales, "order_line_id", "Sales order_line_id"),
    (returns, "return_id", "Returns return_id"),
    (inventory, "movement_id", "Inventory movement_id")
]

reference_ids = (
    inventory
    .filter(col("reference_id").isNotNull())
    .select("reference_id")
    .distinct()
)

for df, key, name in reference_checks:

    target_ids = (
        df.select(col(key).alias("target_id"))
        .filter(col(key).isNotNull())
        .distinct()
    )

    matched = reference_ids.join(
        target_ids,
        reference_ids.reference_id == target_ids.target_id,
        "inner"
    ).count()

    print("Reference matches with", name, ":", matched)


# --------------------------------------------------
# 6. TIMESTAMP VALIDATION
# --------------------------------------------------

print("\n6. TIMESTAMP VALIDATION")

timestamp_checks = [
    (sales, "order_ts", "Sales order_ts"),
    (inventory, "movement_ts", "Inventory movement_ts"),
    (inventory, "recorded_ts", "Inventory recorded_ts"),
    (returns, "return_ts", "Returns return_ts")
]

for df, field, name in timestamp_checks:

    missing = df.filter(col(field).isNull()).count()

    placeholder = df.filter(
        lower(trim(col(field).cast("string"))).isin(
            "1900-01-01",
            "1970-01-01",
            "0000-00-00",
            "9999-12-31",
            "null",
            "n/a",
            "unknown"
        )
    ).count()

    print(
        name,
        "| Total:", df.count(),
        "| Missing:", missing,
        "| Placeholder candidates:", placeholder
    )


# --------------------------------------------------
# 7. FINAL INVENTORY AND RETURNS COUNTS
# --------------------------------------------------

print("\n7. FINAL SOURCE COUNTS")

for df, name in [
    (sales, "Sales"),
    (inventory, "Inventory"),
    (returns, "Returns"),
    (products, "Products"),
    (stores, "Stores"),
    (suppliers, "Suppliers")
]:
    print(name, ":", df.count())

print("\n" + "=" * 90)
print("INVESTIGATION COMPLETED - NO SOURCE TABLES MODIFIED")
print("=" * 90)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(
# MAGIC         CASE WHEN dq_missing_order_status = true
# MAGIC         THEN 1 ELSE 0 END
# MAGIC     ) AS missing_status_records
# MAGIC FROM capretailnew.silver.silver_sales_order_lines;