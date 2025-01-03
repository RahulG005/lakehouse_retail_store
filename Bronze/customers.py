# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists bronze; 
# MAGIC create table IF NOT EXISTS bronze.customers(
# MAGIC   id integer,
# MAGIC   name varchar(200),
# MAGIC   address varchar(200),
# MAGIC   city varchar(200),
# MAGIC   postalcode varchar(200),
# MAGIC   country varchar(200),
# MAGIC   phone varchar(200),
# MAGIC   email varchar(200),
# MAGIC   credit_card varchar(200),
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
    .option("query","select * from [dbo].[control_tbl] where table_name = 'customers'")\
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

customers_df = spark.read.format("jdbc")\
    .option("url", sqlserverurl)\
    .option("query","select * from [dbo].[customers]")\
    .option("user",User)\
    .option("password",password)\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .load()

# COMMAND ----------

customers_df.count()

# COMMAND ----------

customers_df = customers_df.filter(customers_df['updated_at']>last_modified)

# COMMAND ----------

display(customers_df)

# COMMAND ----------

customers_df.createOrReplaceTempView("customers_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into bronze.customers
# MAGIC using customers_temp_view
# MAGIC on customers.id = customers_temp_view.id
# MAGIC when matched then update set *
# MAGIC when not matched then insert *

# COMMAND ----------

from pyspark.sql.functions import max
customers_df_last_modified_date = customers_df.agg(max(customers_df["updated_at"])).collect()[0][0]
print(customers_df_last_modified_date)

# COMMAND ----------

import pymssql
conn = pymssql.connect("storeserver.database.windows.net", "Rahul@storeserver", "Data@123", "storedb")
cursor = conn.cursor(as_dict=True)

# COMMAND ----------

customers_df_last_modified_date = str(customers_df_last_modified_date).split(".")[0]
print(customers_df_last_modified_date)

# COMMAND ----------

sqlstring = "update [dbo].[control_tbl] set watermark_value = '" + customers_df_last_modified_date + "' where table_name = 'customers'"
print(sqlstring)

# COMMAND ----------

cursor.execute(sqlstring)
conn.commit()
conn.close()