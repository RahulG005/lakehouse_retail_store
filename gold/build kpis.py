# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### KPI - Get unique customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists gold.unique_customers as
# MAGIC select count(distinct id) as total from gold.customers

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### KPI - Get unique customers in each country

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists  gold.country_customers as
# MAGIC select country,count(*) as total from gold.customers group by country

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### KPI - Get total website hits

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists  gold.total_website_hits as
# MAGIC SELECT count(*) AS hits FROM gold.weblogs

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### KPI - Get website hits for each country

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists  gold.total_country_website_hits as
# MAGIC SELECT country,count(*) AS hits FROM gold.weblogs group by country

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### KPI - Get product,yearly & Unit quantity 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists  gold.product_yearly_units as
# MAGIC SELECT name, YEAR(order_date) AS year, count(*) AS units_sold FROM gold.orders
# MAGIC GROUP BY name, YEAR(order_date)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### KPI - Get yearly sales 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists gold.yearly_sales as
# MAGIC SELECT YEAR(order_date) AS year,round(sum(sale_price_usd),2) as aggregated_sales_price
# MAGIC 						   FROM gold.orders 
# MAGIC 					   GROUP BY YEAR(order_date)