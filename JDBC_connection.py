# Databricks notebook source
jdbc_url = (
    "jdbc:sqlserver://capstone4dbserver.database.windows.net:1433;""database=capstone4_retail;"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)
try:
    product_master_df = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dbo.product_master")
        .option("user", "ltmadmin@capstone4dbserver")
        .option("password", "P@55word")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load())
    display(product_master_df)
except Exception as e:
    print(str(e)[:2000]) 

# COMMAND ----------

try:
    store_master_df = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dbo.store_master")
        .option("user", "ltmadmin@capstone4dbserver")
        .option("password", "P@55word")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load())
    display(store_master_df)
except Exception as e:
    print(str(e)[:2000]) 

try:
    supplier_master_df = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dbo.supplier_master")
        .option("user", "ltmadmin@capstone4dbserver")
        .option("password", "P@55word")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load())
    display(supplier_master_df)
except Exception as e:
    print(str(e)[:2000]) 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists capretailnew.bronze.bronze_product_master

# COMMAND ----------

product_master_df.write.format("delta").mode("overwrite").saveAsTable("capretailnew.bronze.bronze_product_master")   
store_master_df.write.format("delta").mode("overwrite").saveAsTable("capretailnew.bronze.bronze_store_master") 
supplier_master_df.write.format("delta").mode("overwrite").saveAsTable("capretailnew.bronze.bronze_supplier_master") 


# COMMAND ----------

# MAGIC %md
# MAGIC Checking master data

# COMMAND ----------

from pyspark.sql.functions import *
print(product_master_df.count())
print(store_master_df.count())
print(supplier_master_df.count())

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

print(product_master_df.filter(coalesce(*[col(c) for c in product_master_df.columns]).isNull()).count())
print(store_master_df.filter(coalesce(*[col(c) for c in store_master_df.columns]).isNull()).count())
print(supplier_master_df.filter(coalesce(*[col(c) for c in supplier_master_df.columns]).isNull()).count())

# COMMAND ----------

total_product=product_master_df.count()
distinct_product=product_master_df.distinct().count()
total_store=store_master_df.count()
distinct_store=store_master_df.distinct().count()
total_supplier=supplier_master_df.count()
distinct_supplier=supplier_master_df.distinct().count()
print(total_product-distinct_product)
print(total_store-distinct_store)
print(total_supplier-distinct_supplier)


# COMMAND ----------

product_master_df.printSchema()
store_master_df.printSchema()
supplier_master_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from capretailnew.bronze.bronze_product_master;