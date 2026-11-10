# Databricks notebook source
# MAGIC %md
# MAGIC ##Load and inspect Bronze inventory

# COMMAND ----------

from pyspark.sql.functions import col, trim, count, min, max, sum
from pyspark.sql import functions as F

BRONZE_INVENTORY = "capretailnew.bronze.bronze_inventory_movements"

inventory_df = spark.table(BRONZE_INVENTORY)

print("Total records:", inventory_df.count())
print("Distinct movement IDs:", inventory_df.select("movement_id").distinct().count())

inventory_df.printSchema()

display(inventory_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import expr

inventory_checked = (
    inventory_df
    .withColumn("movement_ts_check", expr("try_cast(movement_ts AS TIMESTAMP)"))
    .withColumn("recorded_ts_check", expr("try_cast(recorded_ts AS TIMESTAMP)"))
    .withColumn("quantity_change_check", expr("try_cast(quantity_change AS DECIMAL(18,2))"))
    .withColumn("unit_cost_check", expr("try_cast(unit_cost AS DECIMAL(18,2))"))
)

print("Movement type distribution:")
display(
    inventory_checked.groupBy("movement_type")
    .count()
    .orderBy("movement_type")
)

print("Quantity change distribution:")
display(
    inventory_checked.groupBy("movement_type")
    .agg(
        count("*").alias("records"),
        min("quantity_change_check").alias("minimum_quantity_change"),
        max("quantity_change_check").alias("maximum_quantity_change")
    )
    .orderBy("movement_type")
)

print("Null or failed-cast checks:")
display(
    inventory_checked.select(
        F.count("*").alias("total_records"),
        F.sum(F.when(col("movement_ts_check").isNull(), 1).otherwise(0)).alias("invalid_movement_ts"),
        F.sum(F.when(col("recorded_ts_check").isNull(), 1).otherwise(0)).alias("invalid_recorded_ts"),
        F.sum(F.when(col("quantity_change_check").isNull(), 1).otherwise(0)).alias("invalid_quantity_change"),
        F.sum(F.when(col("unit_cost_check").isNull(), 1).otherwise(0)).alias("invalid_unit_cost")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check inventory business rules

# COMMAND ----------

from pyspark.sql.functions import col, trim, when, sum as spark_sum

valid_movement_types = [
    "SALE",
    "TRANSFER",
    "ADJUSTMENT",
    "RECEIPT"
]

inventory_dq = (
    inventory_checked
    .withColumn(
        "dq_missing_required",
        col("movement_id").isNull() | (trim(col("movement_id")) == "") |
        col("product_id").isNull() | (trim(col("product_id")) == "") |
        col("store_id").isNull() | (trim(col("store_id")) == "") |
        col("movement_type").isNull() | (trim(col("movement_type")) == "") |
        col("reference_id").isNull() | (trim(col("reference_id")) == "") |
        col("warehouse_id").isNull() | (trim(col("warehouse_id")) == "")
    )
    .withColumn(
        "dq_invalid_movement_type",
        ~col("movement_type").isin(valid_movement_types)
    )
    .withColumn(
        "dq_invalid_quantity",
        col("quantity_change_check").isNull()
    )
    .withColumn(
        "dq_invalid_unit_cost",
        col("unit_cost_check").isNull() | (col("unit_cost_check") < 0)
    )
    .withColumn(
        "dq_invalid_timestamp",
        col("movement_ts_check").isNull() |
        col("recorded_ts_check").isNull()
    )
    .withColumn(
        "dq_recorded_before_movement",
        col("recorded_ts_check") < col("movement_ts_check")
    )
)

inventory_dq = inventory_dq.withColumn(
    "has_dq_issue",
    col("dq_missing_required") |
    col("dq_invalid_movement_type") |
    col("dq_invalid_quantity") |
    col("dq_invalid_unit_cost") |
    col("dq_invalid_timestamp") |
    col("dq_recorded_before_movement")
)

display(
    inventory_dq.agg(
        spark_sum(when(col("dq_missing_required"), 1).otherwise(0)).alias("missing_required"),
        spark_sum(when(col("dq_invalid_movement_type"), 1).otherwise(0)).alias("invalid_movement_type"),
        spark_sum(when(col("dq_invalid_quantity"), 1).otherwise(0)).alias("invalid_quantity"),
        spark_sum(when(col("dq_invalid_unit_cost"), 1).otherwise(0)).alias("invalid_unit_cost"),
        spark_sum(when(col("dq_invalid_timestamp"), 1).otherwise(0)).alias("invalid_timestamp"),
        spark_sum(when(col("dq_recorded_before_movement"), 1).otherwise(0)).alias("recorded_before_movement"),
        spark_sum(when(col("has_dq_issue"), 1).otherwise(0)).alias("total_dq_records")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create and save Inventory Silver

# COMMAND ----------

inventory_silver = (
    inventory_dq
    .filter(~col("has_dq_issue"))
    .select(
        col("movement_id"),
        col("product_id"),
        col("store_id"),
        col("movement_ts_check").alias("movement_ts"),
        col("movement_type"),
        col("quantity_change_check").alias("quantity_change"),
        col("reference_id"),
        col("unit_cost_check").alias("unit_cost"),
        col("warehouse_id"),
        col("recorded_ts_check").alias("recorded_ts"),
        col("source_file"),
        col("ingestion_timestamp")
    )
)

inventory_quarantine = (
    inventory_dq
    .filter(col("has_dq_issue"))
    .withColumn(
        "quarantine_reason",
        when(col("dq_missing_required"), "MISSING_REQUIRED_FIELD")
        .when(col("dq_invalid_movement_type"), "INVALID_MOVEMENT_TYPE")
        .when(col("dq_invalid_quantity"), "INVALID_QUANTITY")
        .when(col("dq_invalid_unit_cost"), "INVALID_UNIT_COST")
        .when(col("dq_invalid_timestamp"), "INVALID_TIMESTAMP")
        .when(col("dq_recorded_before_movement"), "RECORDED_BEFORE_MOVEMENT")
    )
)

(inventory_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("capretailnew.silver.silver_inventory_movements")
)

(inventory_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("capretailnew.silver.quarantine_inventory_movements")
)

print("Silver inventory:", inventory_silver.count())
print("Quarantined inventory:", inventory_quarantine.count())

# COMMAND ----------

saved_inventory = spark.table(
    "capretailnew.silver.silver_inventory_movements"
)

print("Saved Silver records:", saved_inventory.count())

print(
    "Duplicate movement IDs:",
    saved_inventory.groupBy("movement_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

display(
    saved_inventory.groupBy("movement_type")
    .count()
    .orderBy("movement_type")
)

saved_inventory.printSchema()