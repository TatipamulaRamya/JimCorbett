# Databricks notebook source
# MAGIC %md
# MAGIC ##Configure all Bronze and Silver table names

# COMMAND ----------


CATALOG = "capretailnew"

BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# Bronze source tables
BRONZE_SALES = f"{BRONZE}.bronze_sales_order_lines"
BRONZE_INVENTORY = f"{BRONZE}.bronze_inventory_movements"
BRONZE_RETURNS = f"{BRONZE}.bronze_sales_returns"

BRONZE_PRODUCT = f"{BRONZE}.bronze_product_master"
BRONZE_STORE = f"{BRONZE}.bronze_store_master"
BRONZE_SUPPLIER = f"{BRONZE}.bronze_supplier_master"

# Silver output tables
SILVER_SALES = f"{SILVER}.silver_sales_order_lines"
SILVER_INVENTORY = f"{SILVER}.silver_inventory_movements"
SILVER_RETURNS = f"{SILVER}.silver_sales_returns"

SILVER_PRODUCT = f"{SILVER}.silver_product_master"
SILVER_STORE = f"{SILVER}.silver_store_master"
SILVER_SUPPLIER = f"{SILVER}.silver_supplier_master"

print("Bronze:", BRONZE)
print("Silver:", SILVER)
print("Configuration completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Verify all six Bronze tables

# COMMAND ----------

bronze_tables = [
    BRONZE_SALES,
    BRONZE_INVENTORY,
    BRONZE_RETURNS,
    BRONZE_PRODUCT,
    BRONZE_STORE,
    BRONZE_SUPPLIER
]

for table_name in bronze_tables:

    print("\n===================================")
    print("TABLE:", table_name)

    df = spark.table(table_name)

    print("Record Count:", df.count())
    print("Column Count:", len(df.columns))
    print("Columns:", df.columns)

    df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Bronze source profiling

# COMMAND ----------

# ============================================================
# DAY 2 - STEP 2: CORRECTED BRONZE DATA PROFILING
# ============================================================

from pyspark.sql.functions import col, count, when, trim
from pyspark.sql.types import StringType

source_tables = {
    "SALES": (
        "capretailnew.bronze.bronze_sales_order_lines",
        "order_line_id"
    ),
    "INVENTORY": (
        "capretailnew.bronze.bronze_inventory_movements",
        "movement_id"
    ),
    "RETURNS": (
        "capretailnew.bronze.bronze_sales_returns",
        "return_id"
    ),
    "PRODUCT": (
        "capretailnew.bronze.bronze_product_master",
        "product_id"
    ),
    "STORE": (
        "capretailnew.bronze.bronze_store_master",
        "store_id"
    ),
    "SUPPLIER": (
        "capretailnew.bronze.bronze_supplier_master",
        "supplier_id"
    )
}

for name, (table_name, business_key) in source_tables.items():

    df = spark.table(table_name)

    total_records = df.count()

    distinct_keys = (
        df.select(business_key)
        .distinct()
        .count()
    )

    duplicate_key_groups = (
        df.groupBy(business_key)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    exact_duplicate_excess = (
        total_records - df.dropDuplicates().count()
    )

    print("\n" + "=" * 65)
    print("DATASET:", name)
    print("TABLE:", table_name)
    print("Total records:", total_records)
    print("Distinct business keys:", distinct_keys)
    print("Duplicate key groups:", duplicate_key_groups)
    print("Exact duplicate excess rows:", exact_duplicate_excess)

    # NULL and empty-string profiling
    null_expressions = []

    for field in df.schema.fields:

        column_name = field.name

        if isinstance(field.dataType, StringType):

            condition = (
                col(column_name).isNull() |
                (trim(col(column_name)) == "")
            )

        else:
            condition = col(column_name).isNull()

        null_expressions.append(
            count(
                when(condition, 1)
            ).alias(column_name)
        )

    print("\nNULL / EMPTY VALUE COUNTS:")

    display(df.select(*null_expressions))

    # Rescued records
    if "_rescued_data" in df.columns:

        rescued_count = (
            df.filter(col("_rescued_data").isNotNull())
            .count()
        )

        print("Rescued records:", rescued_count)

    # Duplicate business keys
    print("\nDUPLICATE BUSINESS KEY GROUPS:")

    display(
        df.groupBy(business_key)
        .count()
        .filter(col("count") > 1)
        .orderBy(col("count").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##INVESTIGATE DUPLICATE SALES KEYS

# COMMAND ----------



from pyspark.sql.functions import col

sales_df = spark.table(
    "capretailnew.bronze.bronze_sales_order_lines"
)

duplicate_ids = (
    sales_df.groupBy("order_line_id")
    .count()
    .filter(col("count") > 1)
    .select("order_line_id")
)

duplicate_records = (
    sales_df.join(
        duplicate_ids,
        on="order_line_id",
        how="inner"
    )
    .orderBy("order_line_id", "source_file")
)

print("Total duplicate-key records:", duplicate_records.count())

display(duplicate_records)

# COMMAND ----------

# ============================================================
# DAY 2 - STEP 4: COMPARE DUPLICATE BUSINESS KEYS
# ============================================================

display(
    duplicate_records.select(
        "order_line_id",
        "order_id",
        "order_ts",
        "store_id",
        "product_id",
        "customer_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "sales_channel",
        "order_status",
        "tax_amount",
        "source_file",
        "ingestion_timestamp"
    )
)

# COMMAND ----------

# ============================================================
# DAY 2 - BUSINESS LEVEL DUPLICATE VALIDATION
# ============================================================

from pyspark.sql.functions import col, count

sales_df = spark.table(
    "capretailnew.bronze.bronze_sales_order_lines"
)

# Exclude ingestion metadata from business comparison
business_columns = [
    "order_line_id",
    "order_id",
    "order_ts",
    "store_id",
    "product_id",
    "customer_id",
    "quantity",
    "unit_price",
    "discount_pct",
    "sales_channel",
    "order_status",
    "tax_amount"
]

business_duplicate_count = (
    sales_df.count()
    - sales_df.select(*business_columns).distinct().count()
)

print("Total Bronze records:", sales_df.count())
print("Business-level duplicate excess:", business_duplicate_count)
print(
    "Distinct business records:",
    sales_df.select(*business_columns).distinct().count()
)
# Check duplicate records by source file

duplicate_business = (
    sales_df.groupBy(*business_columns)
    .count()
    .filter(col("count") > 1)
)

print("Duplicate business groups:", duplicate_business.count())

duplicate_details = (
    sales_df.join(
        duplicate_business.select(*business_columns),
        on=business_columns,
        how="inner"
    )
)

display(
    duplicate_details.select(
        "order_line_id",
        "source_file",
        "ingestion_timestamp"
    ).orderBy("order_line_id", "source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate the duplicate pattern

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, collect_set, sum, when, row_number
)
from pyspark.sql.window import Window

business_columns = [
    "order_line_id",
    "order_id",
    "order_ts",
    "store_id",
    "product_id",
    "customer_id",
    "quantity",
    "unit_price",
    "discount_pct",
    "sales_channel",
    "order_status",
    "tax_amount"
]

duplicate_groups = (
    sales_df
    .groupBy(*business_columns)
    .agg(
        count("*").alias("record_count"),
        collect_set("source_file").alias("source_files"),
        sum(
            when(
                col("source_file").contains("batch_010.csv"), 1
            ).otherwise(0)
        ).alias("batch_010_count")
    )
    .filter(col("record_count") > 1)
)

display(duplicate_groups)

# COMMAND ----------

from pyspark.sql.functions import size
invalid_duplicate_groups = duplicate_groups.filter(
    (col("record_count") != 2) |
    (col("batch_010_count") != 1) |
    (size(col("source_files")) != 2)
)

print("Total duplicate groups:", duplicate_groups.count())
print("Invalid duplicate groups:", invalid_duplicate_groups.count())

display(invalid_duplicate_groups)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Deduplicate sales records

# COMMAND ----------

from pyspark.sql.functions import col, when, row_number
from pyspark.sql.window import Window

# Define deterministic ranking:
# Original batches are preferred over batch_010
dedup_window = (
    Window
    .partitionBy(*business_columns)
    .orderBy(
        when(
            col("source_file").contains("batch_010.csv"), 1
        ).otherwise(0).asc(),
        col("source_file").asc()
    )
)

# Assign a rank to each business-level record
sales_ranked = sales_df.withColumn(
    "_duplicate_rank",
    row_number().over(dedup_window)
)

# Keep one original record per business record
sales_business_dedup = (
    sales_ranked
    .filter(col("_duplicate_rank") == 1)
    .drop("_duplicate_rank")
)

# Preserve replayed duplicate records separately
sales_dedup_audit = (
    sales_ranked
    .filter(col("_duplicate_rank") > 1)
    .drop("_duplicate_rank")
)

# Display reconciliation
print("Original Bronze records:", sales_df.count())
print("Clean sales records:", sales_business_dedup.count())
print("Duplicate records in audit:", sales_dedup_audit.count())

print(
    "Remaining duplicate business groups:",
    sales_business_dedup
    .groupBy(*business_columns)
    .count()
    .filter(col("count") > 1)
    .count()
)

display(sales_dedup_audit.select(
    "order_line_id",
    "source_file",
    "ingestion_timestamp"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inspect sales data quality

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, when, count, min, max,
    expr, sum, countDistinct
)

# 1. Safely test conversions without changing the source DataFrame
sales_checked = (
    sales_business_dedup
    .withColumn(
        "quantity_check",
        expr("try_cast(trim(quantity) AS INT)")
    )
    .withColumn(
        "unit_price_check",
        expr("try_cast(trim(unit_price) AS DECIMAL(18,2))")
    )
    .withColumn(
        "discount_pct_check",
        expr("try_cast(trim(discount_pct) AS DECIMAL(5,2))")
    )
    .withColumn(
        "tax_amount_check",
        expr("try_cast(trim(tax_amount) AS DECIMAL(18,2))")
    )
    .withColumn(
        "order_ts_check",
        expr("try_cast(order_ts AS TIMESTAMP)")
    )
)

# 2. Check missing and invalid values
sales_quality_summary = sales_checked.agg(
    count("*").alias("total_records"),

    sum(when(
        col("order_line_id").isNull() |
        (trim(col("order_line_id")) == ""), 1
    ).otherwise(0)).alias("missing_order_line_id"),

    sum(when(
        col("order_id").isNull() |
        (trim(col("order_id")) == ""), 1
    ).otherwise(0)).alias("missing_order_id"),

    sum(when(
        col("customer_id").isNull() |
        (trim(col("customer_id")) == ""), 1
    ).otherwise(0)).alias("missing_customer_id"),

    sum(when(col("quantity_check").isNull(), 1)
        .otherwise(0)).alias("invalid_quantity"),

    sum(when(col("unit_price_check").isNull(), 1)
        .otherwise(0)).alias("invalid_unit_price"),

    sum(when(col("discount_pct_check").isNull(), 1)
        .otherwise(0)).alias("invalid_discount_pct"),

    sum(when(col("tax_amount_check").isNull(), 1)
        .otherwise(0)).alias("invalid_tax_amount"),

    sum(when(col("order_ts_check").isNull(), 1)
        .otherwise(0)).alias("invalid_order_timestamp")
)

display(sales_quality_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inspect numeric ranges

# COMMAND ----------

display(
    sales_checked.select(
        min("quantity_check").alias("min_quantity"),
        max("quantity_check").alias("max_quantity"),

        min("unit_price_check").alias("min_unit_price"),
        max("unit_price_check").alias("max_unit_price"),

        min("discount_pct_check").alias("min_discount_pct"),
        max("discount_pct_check").alias("max_discount_pct"),

        min("tax_amount_check").alias("min_tax_amount"),
        max("tax_amount_check").alias("max_tax_amount")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##nspect category values

# COMMAND ----------

for column_name in [
    "sales_channel",
    "order_status"
]:
    print("Distinct values for:", column_name)

    display(
        sales_checked
        .groupBy(column_name)
        .count()
        .orderBy(col("count").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inspect suspicious numeric records

# COMMAND ----------

display(
    sales_checked.filter(
        (col("quantity_check") <= 0) |
        (col("unit_price_check") <= 0) |
        (col("discount_pct_check") < 0) |
        (col("discount_pct_check") > 100) |
        (col("tax_amount_check") < 0)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inspect the 27 failed numeric conversions

# COMMAND ----------

from pyspark.sql.functions import col, trim

print("Invalid quantity values")
display(
    sales_checked
    .filter(
        col("quantity").isNull() |
        (trim(col("quantity")) == "") |
        col("quantity_check").isNull()
    )
    .select("order_line_id", "quantity", "source_file")
)

print("Invalid unit price values")
display(
    sales_checked
    .filter(
        col("unit_price").isNull() |
        (trim(col("unit_price")) == "") |
        col("unit_price_check").isNull()
    )
    .select("order_line_id", "unit_price", "source_file")
)

print("Invalid discount values")
display(
    sales_checked
    .filter(
        col("discount_pct").isNull() |
        (trim(col("discount_pct")) == "") |
        col("discount_pct_check").isNull()
    )
    .select("order_line_id", "discount_pct", "source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inspect missing business attributes

# COMMAND ----------

print("Missing order IDs")
display(
    sales_checked
    .filter(
        col("order_id").isNull() |
        (trim(col("order_id")) == "")
    )
    .select("order_line_id", "order_id", "customer_id")
)

print("Missing customer IDs")
display(
    sales_checked
    .filter(
        col("customer_id").isNull() |
        (trim(col("customer_id")) == "")
    )
    .select("order_line_id", "order_id", "customer_id")
)

print("Missing sales channels")
display(
    sales_checked
    .filter(
        col("sales_channel").isNull() |
        (trim(col("sales_channel")) == "")
    )
    .select("order_line_id", "sales_channel")
)

print("Missing order statuses")
display(
    sales_checked
    .filter(
        col("order_status").isNull() |
        (trim(col("order_status")) == "")
    )
    .select("order_line_id", "order_status")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Investigate unusually high unit prices

# COMMAND ----------

display(
    sales_checked
    .filter(col("unit_price_check") >= 100000)
    .select(
        "order_line_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "tax_amount",
        "order_status"
    )
    .orderBy(col("unit_price_check").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate high sales prices against product master

# COMMAND ----------

from pyspark.sql.functions import col, expr

product_check = (
    spark.table("capretailnew.bronze.bronze_product_master")
    .select(
        "product_id",
        "product_name",
        expr("try_cast(list_price AS DECIMAL(18,2))").alias("list_price"),
        expr("try_cast(cost_price AS DECIMAL(18,2))").alias("cost_price")
    )
)

high_price_check = (
    sales_checked
    .filter(col("unit_price_check") >= 100000)
    .join(product_check, "product_id", "left")
    .select(
        "order_line_id",
        "product_id",
        "product_name",
        "unit_price_check",
        "list_price",
        "cost_price",
        "quantity",
        "discount_pct_check"
    )
    .orderBy(col("unit_price_check").desc())
)

display(high_price_check)

# COMMAND ----------

display(
    product_check.selectExpr(
        "MIN(list_price) AS min_list_price",
        "MAX(list_price) AS max_list_price",
        "MIN(cost_price) AS min_cost_price",
        "MAX(cost_price) AS max_cost_price",
        "COUNT(*) AS total_products"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build row-level DQ flags

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, when, lit, concat_ws, array,
    size, filter as array_filter
)

sales_dq_tagged = (
    sales_checked

    # Quantity validation
    .withColumn(
        "dq_invalid_quantity",
        col("quantity_check").isNull() |
        (col("quantity_check") <= 0)
    )

    # Unit price validation
    .withColumn(
        "dq_invalid_unit_price",
        col("unit_price_check").isNull() |
        (col("unit_price_check") == 999999.99) |
        (col("unit_price_check") <= 0)
    )

    # Discount validation
    .withColumn(
        "dq_invalid_discount",
        col("discount_pct_check").isNull() |
        (col("discount_pct_check") < 0) |
        (col("discount_pct_check") > 100)
    )

    # Required relationship candidate
    .withColumn(
        "dq_missing_order_id",
        col("order_id").isNull() |
        (trim(col("order_id")) == "")
    )

    # Missing attributes to review
    .withColumn(
        "dq_missing_customer",
        col("customer_id").isNull() |
        (trim(col("customer_id")) == "")
    )
    .withColumn(
        "dq_missing_sales_channel",
        col("sales_channel").isNull() |
        (trim(col("sales_channel")) == "")
    )
    .withColumn(
        "dq_missing_order_status",
        col("order_status").isNull() |
        (trim(col("order_status")) == "")
    )
)

# Build a readable list of DQ reasons for each row
sales_dq_tagged = sales_dq_tagged.withColumn(
    "dq_reasons",
    concat_ws(
        ", ",
        when(col("dq_invalid_quantity"), lit("INVALID_QUANTITY")),
        when(col("dq_invalid_unit_price"), lit("INVALID_UNIT_PRICE")),
        when(col("dq_invalid_discount"), lit("INVALID_DISCOUNT")),
        when(col("dq_missing_order_id"), lit("MISSING_ORDER_ID")),
        when(col("dq_missing_customer"), lit("MISSING_CUSTOMER")),
        when(col("dq_missing_sales_channel"), lit("MISSING_SALES_CHANNEL")),
        when(col("dq_missing_order_status"), lit("MISSING_ORDER_STATUS"))
    )
)

# Records with no DQ flags
sales_dq_tagged = sales_dq_tagged.withColumn(
    "has_dq_issue",
    col("dq_reasons") != ""
)

print("Total records:", sales_dq_tagged.count())
print(
    "Records with at least one DQ issue:",
    sales_dq_tagged.filter(col("has_dq_issue")).count()
)
print(
    "Records without these DQ issues:",
    sales_dq_tagged.filter(~col("has_dq_issue")).count()
)

# COMMAND ----------

dq_columns = [
    "dq_invalid_quantity",
    "dq_invalid_unit_price",
    "dq_invalid_discount",
    "dq_missing_order_id",
    "dq_missing_customer",
    "dq_missing_sales_channel",
    "dq_missing_order_status"
]

for dq_column in dq_columns:
    print(
        dq_column,
        sales_dq_tagged.filter(col(dq_column)).count()
    )

# COMMAND ----------

display(
    sales_dq_tagged
    .filter(col("has_dq_issue"))
    .select(
        "order_line_id",
        "order_id",
        "customer_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "sales_channel",
        "order_status",
        "dq_reasons",
        "source_file"
    )
    .orderBy("order_line_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Separate valid and invalid sales records

# COMMAND ----------

from pyspark.sql.functions import col, trim

# Hard data-quality failures
sales_hard_invalid = (
    col("dq_invalid_quantity") |
    col("dq_invalid_unit_price") |
    col("dq_invalid_discount") |
    col("dq_missing_order_id")
)

# Valid records: hard-invalid records are excluded
sales_silver = (
    sales_dq_tagged
    .filter(~sales_hard_invalid)
    .select(
        col("order_line_id"),
        col("order_id"),
        col("order_ts_check").alias("order_ts"),
        col("store_id"),
        col("product_id"),
        col("customer_id"),
        col("quantity_check").alias("quantity"),
        col("unit_price_check").alias("unit_price"),
        col("discount_pct_check").alias("discount_pct"),
        col("sales_channel"),
        col("order_status"),

        # Persist the missing status quality flag
        col("dq_missing_order_status"),

        col("tax_amount_check").alias("tax_amount"),
        col("source_file"),
        col("ingestion_timestamp")
    )
)

# Quarantine hard-invalid records with their reasons
sales_quarantine = (
    sales_dq_tagged
    .filter(sales_hard_invalid)
    .withColumn("quarantine_reason", col("dq_reasons"))
)

# Keep duplicate replay records separately for audit
sales_duplicate_audit = sales_dedup_audit

print("Silver sales records:", sales_silver.count())
print("Quarantined sales records:", sales_quarantine.count())
print("Duplicate audit records:", sales_duplicate_audit.count())

print(
    "Missing order status records:",
    sales_silver.filter(
        col("dq_missing_order_status") == True
    ).count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write the three Delta tables

# COMMAND ----------

# Clean Silver sales table
(sales_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("capretailnew.silver.silver_sales_order_lines")
)

# Quarantine table
(sales_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("capretailnew.silver.quarantine_sales_order_lines")
)

# Duplicate replay audit table
(sales_duplicate_audit.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("capretailnew.silver.sales_order_lines_duplicate_audit")
)

print("All three sales tables written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate the saved tables

# COMMAND ----------

silver_sales = spark.table(
    "capretailnew.silver.silver_sales_order_lines"
)

quarantine_sales = spark.table(
    "capretailnew.silver.quarantine_sales_order_lines"
)

duplicate_audit = spark.table(
    "capretailnew.silver.sales_order_lines_duplicate_audit"
)

print("Silver count:", silver_sales.count())
print("Quarantine count:", quarantine_sales.count())
print("Duplicate audit count:", duplicate_audit.count())

print(
    "Duplicate business keys remaining:",
    silver_sales.groupBy("order_line_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Silver schema:")
silver_sales.printSchema()