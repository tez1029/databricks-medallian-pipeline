# Databricks notebook source
SOURCE_TABLE_NAME = 'source_system.sales.orders'
TARGET_TABLE_NAME = 'silver.sales.orders'

# COMMAND ----------

raw_df = spark.table(SOURCE_TABLE_NAME)

# COMMAND ----------

raw_df.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df = raw_df.filter(col('status') == 'COMPLETED').filter(col('quantity') > 0).filter(col('unit_price') > 0)

# COMMAND ----------

df.show()

# COMMAND ----------

amount_df = df.withColumn("gross_column", col("unit_price") * col("quantity")).withColumn("net_amount", col("unit_price") * col("quantity") - col('discount'))
amount_df.show()

# COMMAND ----------

from pyspark.sql.functions import year, month

# COMMAND ----------

final_df = amount_df.withColumn('order_year', year(col('order_date'))).withColumn('order_month',month(col('order_date')))
final_df.show()


# COMMAND ----------

final_df.write.mode('overwrite').saveAsTable(TARGET_TABLE_NAME)