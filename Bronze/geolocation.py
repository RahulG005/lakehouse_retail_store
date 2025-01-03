# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists bronze; 
# MAGIC create table IF NOT EXISTS bronze.geolocation(
# MAGIC   ip_address string,
# MAGIC   city string,
# MAGIC   country_code string,
# MAGIC   country string
# MAGIC )

# COMMAND ----------

df = spark.read.format("delta").load("/user/hive/warehouse/geolocation")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("bronze.geolocation")