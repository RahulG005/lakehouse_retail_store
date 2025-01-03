# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists silver; 
# MAGIC create table IF NOT EXISTS silver.products(
# MAGIC   code varchar(200),
# MAGIC   name varchar(200),
# MAGIC   category varchar(200),
# MAGIC   updated_at timestamp
# MAGIC )

# COMMAND ----------

# %sql
# insert into bronze.control_tbl values ('products',0);

# COMMAND ----------

srcDelta = spark.sql("select * from bronze.control_tbl where tablename='products' ")

# COMMAND ----------

last_version=srcDelta.collect()[0][1]
print(last_version)

# COMMAND ----------

from delta.tables import *
srcDeltaTable = DeltaTable.forName(spark, "bronze.products")

# COMMAND ----------

latest_version=srcDeltaTable.history(1).collect()[0][0]
print(latest_version)

# COMMAND ----------

df=spark.sql(f"select * from bronze.products@v{latest_version} except all select * from bronze.products@v{last_version} ")

# COMMAND ----------

df.createOrReplaceTempView("delta_diff")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silver.products
# MAGIC USING delta_diff 
# MAGIC ON delta_diff.code = silver.products.code
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

spark.sql("update bronze.control_tbl set version="+str(latest_version)+" where tablename='products'")