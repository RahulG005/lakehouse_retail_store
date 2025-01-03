# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists bronze; 
# MAGIC create table IF NOT EXISTS bronze.orders(
# MAGIC   order_number integer,
# MAGIC   customer_id integer,
# MAGIC   product_id integer,
# MAGIC   order_date date,
# MAGIC   units varchar(200),
# MAGIC   sale_price double,
# MAGIC   currency varchar(200),
# MAGIC   order_mode varchar(200),
# MAGIC   updated_at timestamp
# MAGIC )

# COMMAND ----------

sqlservername = "storeserver.database.windows.net"
sqldb = "storedb"
sqlport = 1433
User = "Rahul"
password = "Data@123"
sqlserverurl = "jdbc:sqlserver://{0}:{1};database={2}".format(sqlservername, sqlport, sqldb)
connectionProperties = {
    "user": User,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

c_control_df = spark.read.format("jdbc")\
    .option("url", sqlserverurl)\
    .option("query","select * from [dbo].[control_tbl] where table_name = 'orders'")\
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

orders_df = spark.read.format("jdbc")\
    .option("url", sqlserverurl)\
    .option("query","select * from [dbo].[orders]")\
    .option("user",User)\
    .option("password",password)\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .load()

# COMMAND ----------

orders_df.count()

# COMMAND ----------

orders_df = orders_df.filter(orders_df['updated_at']>last_modified)

# COMMAND ----------

display(orders_df)

# COMMAND ----------

orders_df.createOrReplaceTempView("orders_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into bronze.orders
# MAGIC using orders_temp_view
# MAGIC on orders.order_number = orders_temp_view.order_number
# MAGIC when matched then update set *
# MAGIC when not matched then insert *

# COMMAND ----------

from pyspark.sql.functions import max
orders_df_last_modified_date = orders_df.agg(max(orders_df["updated_at"])).collect()[0][0]
print(orders_df_last_modified_date)

# COMMAND ----------

import pymssql
conn = pymssql.connect("storeserver.database.windows.net", "Rahul@storeserver", "Data@123", "storedb")
cursor = conn.cursor(as_dict=True)

# COMMAND ----------

orders_df_last_modified_date = str(orders_df_last_modified_date).split(".")[0]
print(orders_df_last_modified_date)

# COMMAND ----------

sqlstring = "update [dbo].[control_tbl] set watermark_value = '" + orders_df_last_modified_date + "' where table_name = 'orders'"
print(sqlstring)

# COMMAND ----------

cursor.execute(sqlstring)
conn.commit()
conn.close()