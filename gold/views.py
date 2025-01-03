# Databricks notebook source
# %sql

# create schema if not exists gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Entity 1 Customers
# MAGIC ### -----> silver.customers  (Offline & Store Customers)
# MAGIC ### -----> silver.eventsonline (Online Customers)

# COMMAND ----------

# MAGIC %sql
# MAGIC 	CREATE  VIEW if not exists gold.customers AS
# MAGIC   SELECT  id,name,address,city,postalcode,country,phone,email
# MAGIC   FROM silver.customers
# MAGIC   UNION ALL
# MAGIC   SELECT  id,customer_name,address,city,postalcode,country,phone,email
# MAGIC   FROM silver.eventsonline

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from gold.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Entity 2 Orders
# MAGIC ### -----> silver.orders  (Offline & Store Orders)
# MAGIC ### -----> silver.eventsonline (Online Orders)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create view if not exists  gold.orders as 
# MAGIC SELECT order_number, email,p.name,order_date,units,sale_price,order_mode,sale_price_usd
# MAGIC           FROM silver.orders o
# MAGIC 					 JOIN gold.customers c ON c.id = o.customer_id
# MAGIC 					 JOIN silver.products p ON p.code = o.product_id
# MAGIC UNION ALL
# MAGIC            SELECT order_number, email, product_name, order_date, 1 AS units, sale_price,order_mode,sale_price_usd
# MAGIC 					  FROM silver.eventsonline     

# COMMAND ----------

# MAGIC %md
# MAGIC ### Entity 3 Products
# MAGIC ### -----> silver.products

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create view if not exists gold.products as select * from silver.products

# COMMAND ----------

# MAGIC %md
# MAGIC ### Entity 4 Weblogs
# MAGIC ### &nbsp; &nbsp;&nbsp;    silver.weblogs

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create view if not exists  gold.weblogs as select * from silver.weblogs;