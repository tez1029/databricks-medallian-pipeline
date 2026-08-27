# Databricks notebook source
# MAGIC %md
# MAGIC 1. upload the given sample .csv file and notebooks to workspace.
# MAGIC 2. run first setup notebook
# MAGIC 3. change the path in read csv cell
# MAGIC 4. run bronze, silver, gold notebooks.
# MAGIC 5. observe the data at each layer
# MAGIC 6. create job

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog source_system;
# MAGIC create catalog raw;
# MAGIC create catalog silver;
# MAGIC create catalog gold;
# MAGIC create schema source_system.sales;
# MAGIC create schema raw.sales;
# MAGIC create schema silver.sales;
# MAGIC create schema gold.sales;

# COMMAND ----------

# DBTITLE 1,read csv
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Workspace/Users/<userid>/orders_10000.csv")

# COMMAND ----------

df.show()

# COMMAND ----------


cdf.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("SOURCE_SYSTEM.SALES.ORDERS")

# COMMAND ----------

