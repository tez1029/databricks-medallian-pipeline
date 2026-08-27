# Databricks notebook source
SOURCE_TABLE = 'silver.sales.orders'
TARGET_TABLE = 'gold.sales.orders_category'

# COMMAND ----------

df = spark.table(SOURCE_TABLE)
df.show()

# COMMAND ----------

from pyspark.sql.functions import sum, count

# COMMAND ----------

c_df = df.groupBy("category").agg(count("order_id").alias("total_orders"), sum("net_amount").alias("total_sales"), sum("quantity").alias("total_quantity"))
c_df.show()

# COMMAND ----------

c_df.write.mode("overwrite").saveAsTable(TARGET_TABLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.sales.orders_category;

# COMMAND ----------

