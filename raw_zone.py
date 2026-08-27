# Databricks notebook source
# MAGIC %md
# MAGIC 1. Create git account
# MAGIC 2. install git if not exist
# MAGIC 3. git clone
# MAGIC 3. create feature branch
# MAGIC 4. add below code after testing
# MAGIC 5. create PR
# MAGIC 6. add the missing case(table exists with zero records) and create the required tables and test the flow

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SOURCE_SYSTEM.SALES.ORDERS

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(unit_price) as min_quant, max(unit_price) as max_quant from SOURCE_SYSTEM.SALES.ORDERS

# COMMAND ----------

from pyspark.sql.functions import current_date, date_sub, col

# COMMAND ----------

source_table = "SOURCE_SYSTEM.SALES.ORDERS"
target_table = "RAW.SALES.ORDERS"

# COMMAND ----------

# DBTITLE 1,table exists check
# Check whether target table exists
target_exists = spark.catalog.tableExists(target_table)
target_exists

# COMMAND ----------

count = 0
try:
    count = spark.sql(f"select count(*) from {target_table}").collect()[0][0]
    print(f"Target table has {count} records")
except Exception as e:
    print(f"Target table does not exist")

# COMMAND ----------

a = 0
b = 1
if a or b:
    print("Both a and b are having non zero elements")
else:
    print("Either a or b is false")

# COMMAND ----------

if not target_exists or not count:
    # -----------------------------
    # FIRST RUN - FULL LOAD
    # -----------------------------
    print("Performing FULL LOAD.")

    df = spark.table(source_table)

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)
else:
    print("Target table exists. Performing INCREMENTAL LOAD.")

    df = spark.table(source_table)

    previous_day_df = df.filter(
        col("order_date") == date_sub(current_date(), 1)
    )

    previous_day_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(target_table)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.SALES.ORDERS

# COMMAND ----------

