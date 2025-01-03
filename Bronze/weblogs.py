# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists bronze; 
# MAGIC create table IF NOT EXISTS bronze.weblogs(
# MAGIC   agent varchar(255),
# MAGIC   remote_ip varchar(255),
# MAGIC   request varchar(255),
# MAGIC   response bigint,
# MAGIC   time string
# MAGIC )

# COMMAND ----------

df = spark.read.json("/FileStore/tables/*.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("bronze.weblogs")