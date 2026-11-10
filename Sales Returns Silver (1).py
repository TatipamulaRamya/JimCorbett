# Databricks notebook source
# MAGIC %md
# MAGIC ##Load and inspect Bronze returns

# COMMAND ----------

from pyspark.sql.functions import col, trim, count, min, max, sum as spark_sum
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

BRONZE_RETURNS = "capretailnew.bronze.bronze_sales_returns"

returns_df = spark.table(BRONZE_RETURNS)

print("Total records:", returns_df.count())
print("Distinct return IDs:", returns_df.select("return_id").distinct().count())

returns_df.printSchema()

display(returns_df.limit(10))

# COMMAND ----------

returns_checked = (
    returns_df
    .withColumn("return_ts_check", expr("try_cast(return_ts AS TIMESTAMP)"))
    .withColumn("return_quantity_check", expr("try_cast(return_quantity AS DECIMAL(18,2))"))
    .withColumn("refund_amount_check", expr("try_cast(refund_amount AS DECIMAL(18,2))"))
)

print("Return reason distribution:")
display(
    returns_checked.groupBy("return_reason")
    .count()
    .orderBy("return_reason")
)

print("Inspection status distribution:")
display(
    returns_checked.groupBy("inspection_status")
    .count()
    .orderBy("inspection_status")
)

print("Approval flag distribution:")
display(
    returns_checked.groupBy("approved_flag")
    .count()
    .orderBy("approved_flag")
)

print("Numeric ranges:")
display(
    returns_checked.agg(
        min("return_quantity_check").alias("min_return_quantity"),
        max("return_quantity_check").alias("max_return_quantity"),
        min("refund_amount_check").alias("min_refund_amount"),
        max("refund_amount_check").alias("max_refund_amount")
    )
)
from pyspark.sql.functions import when
print("Cast validation:")
display(
    returns_checked.agg(
        F.count("*").alias("total_records"),
        spark_sum(when(col("return_ts_check").isNull(), 1).otherwise(0)).alias("invalid_return_timestamp"),
        spark_sum(when(col("return_quantity_check").isNull(), 1).otherwise(0)).alias("invalid_return_quantity"),
        spark_sum(when(col("refund_amount_check").isNull(), 1).otherwise(0)).alias("invalid_refund_amount")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check return links and chronology

# COMMAND ----------

sales_for_returns = spark.table(
    "capretailnew.silver.silver_sales_order_lines"
).select(
    "order_line_id",
    "order_ts"
)

returns_linked = returns_checked.join(
    sales_for_returns,
    on="order_line_id",
    how="left"
)

display(
    returns_linked.agg(
        spark_sum(
            when(col("order_ts").isNull(), 1).otherwise(0)
        ).alias("missing_sales_order_line"),
        spark_sum(
            when(
                col("order_ts").isNotNull() &
                (col("return_ts_check") < col("order_ts")),
                1
            ).otherwise(0)
        ).alias("return_before_order")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Investigate the timestamp anomaly

# COMMAND ----------

# Show examples where the return appears earlier than the order
display(
    returns_linked
    .filter(
        col("order_ts").isNotNull() &
        (col("return_ts_check") < col("order_ts"))
    )
    .select(
        "return_id",
        "order_line_id",
        "return_ts",
        "return_ts_check",
        "order_ts"
    )
    .orderBy("return_id")
    .limit(15)
)
display(
    returns_linked.agg(
        F.min("return_ts_check").alias("earliest_return"),
        F.max("return_ts_check").alias("latest_return"),
        F.min("order_ts").alias("earliest_order"),
        F.max("order_ts").alias("latest_order")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##

# COMMAND ----------

# Read saved tables from the Sales Silver notebook
sales_silver_saved = spark.table(
    "capretailnew.silver.silver_sales_order_lines"
)

sales_quarantine_saved = spark.table(
    "capretailnew.silver.quarantine_sales_order_lines"
)

# Check returns against clean Sales Silver
sales_keys = sales_silver_saved.select(
    "order_line_id",
    "order_ts"
)

returns_linked = returns_checked.join(
    sales_keys,
    on="order_line_id",
    how="left"
)

print(
    "Returns matching clean Sales Silver:",
    returns_linked.filter(col("order_ts").isNotNull()).count()
)

print(
    "Returns not matching clean Sales Silver:",
    returns_linked.filter(col("order_ts").isNull()).count()
)

# Check whether unmatched returns refer to quarantined sales records
quarantine_keys = sales_quarantine_saved.select(
    "order_line_id"
).distinct().withColumn(
    "matched_to_sales_quarantine",
    F.lit(True)
)

returns_reconciled = returns_linked.join(
    quarantine_keys,
    on="order_line_id",
    how="left"
)

display(
    returns_reconciled
    .filter(col("order_ts").isNull())
    .select(
        "return_id",
        "order_line_id",
        "matched_to_sales_quarantine"
    )
)

# COMMAND ----------

display(
    returns_linked
    .withColumn(
        "return_before_order",
        col("order_ts").isNotNull() &
        (col("return_ts_check") < col("order_ts"))
    )
    .groupBy("return_before_order")
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add quality and relationship flags

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, when, lit, sum as spark_sum
)

valid_return_reasons = [
    "CUSTOMER_REQUEST",
    "DAMAGED",
    "WRONG_ITEM"
]

valid_inspection_statuses = [
    "ACTIVE",
    "INACTIVE",
    "PENDING"
]

returns_dq = (
    returns_reconciled
    .withColumn(
        "dq_missing_required",
        col("return_id").isNull() | (trim(col("return_id")) == "") |
        col("order_line_id").isNull() | (trim(col("order_line_id")) == "") |
        col("product_id").isNull() | (trim(col("product_id")) == "") |
        col("store_id").isNull() | (trim(col("store_id")) == "")
    )
    .withColumn(
        "dq_invalid_return_reason",
        ~col("return_reason").isin(valid_return_reasons)
    )
    .withColumn(
        "dq_invalid_inspection_status",
        ~col("inspection_status").isin(valid_inspection_statuses)
    )
    .withColumn(
        "dq_invalid_approval_flag",
        ~col("approved_flag").isin("Y", "N")
    )
    .withColumn(
        "dq_invalid_return_quantity",
        col("return_quantity_check").isNull() |
        (col("return_quantity_check") <= 0)
    )
    .withColumn(
        "dq_invalid_refund_amount",
        col("refund_amount_check").isNull() |
        (col("refund_amount_check") < 0)
    )
    .withColumn(
        "dq_invalid_return_timestamp",
        col("return_ts_check").isNull()
    )
)

returns_dq = (
    returns_dq
    .withColumn(
        "has_dq_issue",
        col("dq_missing_required") |
        col("dq_invalid_return_reason") |
        col("dq_invalid_inspection_status") |
        col("dq_invalid_approval_flag") |
        col("dq_invalid_return_quantity") |
        col("dq_invalid_refund_amount") |
        col("dq_invalid_return_timestamp")
    )
    .withColumn(
        "sales_link_status",
        when(
            col("matched_to_sales_quarantine") == True,
            "PARENT_SALES_QUARANTINED"
        )
        .when(
            col("order_ts").isNotNull(),
            "MATCHED_TO_CLEAN_SALES"
        )
        .otherwise("UNMATCHED_SALES")
    )
    .withColumn(
        "return_before_order",
        when(
            col("order_ts").isNotNull(),
            col("return_ts_check") < col("order_ts")
        ).otherwise(lit(None).cast("boolean"))
    )
)

display(
    returns_dq.groupBy("sales_link_status", "return_before_order")
    .count()
    .orderBy("sales_link_status", "return_before_order")
)

display(
    returns_dq.agg(
        spark_sum(when(col("has_dq_issue"), 1).otherwise(0))
        .alias("intrinsic_dq_records")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Split and save Returns Silver

# COMMAND ----------

returns_silver = (
    returns_dq
    .filter(~col("has_dq_issue"))
    .select(
        "return_id",
        "order_line_id",
        col("return_ts_check").alias("return_ts"),
        col("return_quantity_check").alias("return_quantity"),
        col("refund_amount_check").alias("refund_amount"),
        "return_reason",
        "inspection_status",
        "store_id",
        "product_id",
        "approved_flag",
        "sales_link_status",
        "return_before_order",
        "source_file",
        "ingestion_timestamp"
    )
)

returns_quarantine = (
    returns_dq
    .filter(col("has_dq_issue"))
    .withColumn(
        "quarantine_reason",
        when(col("dq_missing_required"), "MISSING_REQUIRED_FIELD")
        .when(col("dq_invalid_return_reason"), "INVALID_RETURN_REASON")
        .when(col("dq_invalid_inspection_status"), "INVALID_INSPECTION_STATUS")
        .when(col("dq_invalid_approval_flag"), "INVALID_APPROVAL_FLAG")
        .when(col("dq_invalid_return_quantity"), "INVALID_RETURN_QUANTITY")
        .when(col("dq_invalid_refund_amount"), "INVALID_REFUND_AMOUNT")
        .when(col("dq_invalid_return_timestamp"), "INVALID_RETURN_TIMESTAMP")
    )
)

(returns_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("capretailnew.silver.silver_sales_returns")
)

(returns_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("capretailnew.silver.quarantine_sales_returns")
)

print("Returns Silver:", returns_silver.count())
print("Returns Quarantine:", returns_quarantine.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate saved Returns Silver

# COMMAND ----------

saved_returns = spark.table(
    "capretailnew.silver.silver_sales_returns"
)

print("Saved Returns Silver:", saved_returns.count())

print(
    "Duplicate return IDs:",
    saved_returns.groupBy("return_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

display(
    saved_returns.groupBy("sales_link_status", "return_before_order")
    .count()
    .orderBy("sales_link_status", "return_before_order")
)

saved_returns.printSchema()