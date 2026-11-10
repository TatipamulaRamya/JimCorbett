# Databricks notebook source
# Databricks notebook source
from pyspark.sql.functions import (
    col,
    current_timestamp
)

# DBTITLE 1,2. Configure Catalog and Schema
CATALOG = "capretailnew"
SCHEMA = "bronze"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")




# COMMAND ----------


# DBTITLE 1,3. Configure Source Paths

ROOT_PATH = "/Volumes/capretailnew/retail/ext_retail_vol"

SALES_PATH = ROOT_PATH + "/sales_order_lines"
INVENTORY_PATH = ROOT_PATH + "/inventory_movements"
RETURNS_PATH = ROOT_PATH + "/sales_returns"

CHECKPOINT_ROOT = ROOT_PATH + "/checkpoints"
SCHEMA_ROOT = ROOT_PATH + "/schema_tracking"




# COMMAND ----------


# DBTITLE 1,4. Create Auto Loader Directories

dbutils.fs.mkdirs(CHECKPOINT_ROOT)
dbutils.fs.mkdirs(SCHEMA_ROOT)

print("Checkpoint and schema directories are ready")




# COMMAND ----------



# DBTITLE 1,5. Create Auto Loader Ingestion Function

def ingest_with_autoloader(
    source_path,
    table_name,
    checkpoint_name,
    schema_name
):

    checkpoint_path = CHECKPOINT_ROOT + "/" + checkpoint_name
    schema_path = SCHEMA_ROOT + "/" + schema_name

    print("Starting ingestion:", table_name)

    source_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .load(source_path)
    )

    bronze_df = (
        source_df
        .withColumn(
            "source_file",
            col("_metadata.file_path")
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
    )

    query = (
        bronze_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(CATALOG + "." + SCHEMA + "." + table_name)
    )

    query.awaitTermination()

    print("Completed ingestion:", table_name)


# COMMAND ----------

# DBTITLE 1,6. Ingest Sales Data
ingest_with_autoloader(
    SALES_PATH,
    "bronze_sales_order_lines",
    "sales",
    "sales"
)

# DBTITLE 1,7. Ingest Inventory Data

ingest_with_autoloader(
    INVENTORY_PATH,
    "bronze_inventory_movements",
    "inventory",
    "inventory"
)


# DBTITLE 1,8. Ingest Returns Data

ingest_with_autoloader(
    RETURNS_PATH,
    "bronze_sales_returns",
    "returns",
    "returns"
)

# DBTITLE 1,9. Display Bronze Sales Table

display(
    spark.table(
        CATALOG + "." + SCHEMA + ".bronze_sales_order_lines"
    )
)

# DBTITLE 1,10. Display Bronze Inventory Table

display(
    spark.table(
        CATALOG + "." + SCHEMA + ".bronze_inventory_movements"
    )
)

# DBTITLE 1,11. Display Bronze Returns Table

display(
    spark.table(
        CATALOG + "." + SCHEMA + ".bronze_sales_returns"
    )
)


# DBTITLE 1,12. Bronze Ingestion Summary

print(
    "Sales Bronze Count:",
    spark.table(
        CATALOG + "." + SCHEMA + ".bronze_sales_order_lines"
    ).count()
)

print(
    "Inventory Bronze Count:",
    spark.table(
        CATALOG + "." + SCHEMA + ".bronze_inventory_movements"
    ).count()
)

print(
    "Returns Bronze Count:",
    spark.table(
        CATALOG + "." + SCHEMA + ".bronze_sales_returns"
    ).count()
)

print("Bronze Ingestion Completed")