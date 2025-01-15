# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists bronze; 
# MAGIC create table IF NOT EXISTS bronze.products(
# MAGIC   code varchar(200),
# MAGIC   name varchar(200),
# MAGIC   category varchar(200),
# MAGIC   updated_at timestamp
# MAGIC )

# COMMAND ----------

sqlservername = "storeserver.database.windows.net"
sqldb = "storedb"
sqlport = 1433
User = "Rahul"
password = "*****"
sqlserverurl = "jdbc:sqlserver://{0}:{1};database={2}".format(sqlservername, sqlport, sqldb)
connectionProperties = {
    "user": User,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

c_control_df = spark.read.format("jdbc")\
    .option("url", sqlserverurl)\
    .option("query","select * from [dbo].[control_tbl] where table_name = 'products'")\
    .option("user",User)\
    .option("password",password)\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .load()

# COMMAND ----------

display(c_control_df)

# COMMAND ----------

last_modified = c_control_df.collect()[0][1]
print(last_modified)

# COMMAND ----------

products_df = spark.read.format("jdbc")\
    .option("url", sqlserverurl)\
    .option("query","select * from [dbo].[products]")\
    .option("user",User)\
    .option("password",password)\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .load()

# COMMAND ----------

display(products_df)

# COMMAND ----------

products_df = products_df.filter(products_df['updated_at']>last_modified)

# COMMAND ----------

display(products_df)

# COMMAND ----------

products_df.createOrReplaceTempView("product_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into bronze.products
# MAGIC using product_temp_view
# MAGIC on products.code = product_temp_view.code
# MAGIC when matched then update set *
# MAGIC when not matched then insert *

# COMMAND ----------

from pyspark.sql.functions import max
products_df_last_modified_date = products_df.agg(max(products_df["updated_at"])).collect()[0][0]
print(products_df_last_modified_date)

# COMMAND ----------

import pymssql
conn = pymssql.connect("storeserver.database.windows.net", "Rahul@storeserver", "*****", "storedb")
cursor = conn.cursor(as_dict=True)

# COMMAND ----------

products_df_last_modified_date = str(products_df_last_modified_date).split(".")[0]
print(products_df_last_modified_date)

# COMMAND ----------

sqlstring = "update [dbo].[control_tbl] set watermark_value = '" + products_df_last_modified_date + "' where table_name = 'products'"
print(sqlstring)

# COMMAND ----------

cursor.execute(sqlstring)
conn.commit()
conn.close()
