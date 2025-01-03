# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create table IF NOT EXISTS silver.orders(
# MAGIC   order_number integer,
# MAGIC   customer_id integer,
# MAGIC   product_id integer,
# MAGIC   order_date date,
# MAGIC   units varchar(200),
# MAGIC   sale_price double,
# MAGIC   sale_price_usd double,
# MAGIC   currency varchar(200),
# MAGIC   order_mode varchar(200),
# MAGIC   updated_at timestamp
# MAGIC )

# COMMAND ----------

# %sql
# insert into bronze.control_tbl values ('orders',0);

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,array,ArrayType,DateType,TimestampType
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
import hashlib
import datetime
import urllib.request
import json
from datetime import timedelta, date

# COMMAND ----------

def curate_sales_price(currency, currency_value, sales_price):
  if (currency != 'USD'):
    curated_value = float(sales_price)/float(currency_value)
    return float(curated_value)
  else:
    return float(sales_price)

curate_sales_price_udf = udf(curate_sales_price, FloatType())

# COMMAND ----------

srcDelta = spark.sql("select * from bronze.control_TBL where tablename='orders' ")
last_version=srcDelta.collect()[0][1]
print(last_version)

# COMMAND ----------

from delta.tables import *
srcDeltaTable = DeltaTable.forName(spark, "bronze.orders")
latest_version = srcDeltaTable.history(1).collect()[0][0]
print(latest_version)

# COMMAND ----------

df=spark.sql(f"select * from bronze.orders@v{latest_version} except all select * from bronze.orders@v{last_version} ")

# COMMAND ----------

display(df)

# COMMAND ----------

import requests
url = 'https://api.freecurrencyapi.com/v1/latest?apikey=fca_live_8eds9eYutULIO9xESQ1CJ2jOSplMyHrb6oi0j6NN&currencies=USD%2CEUR%2CCAD%2CINR'

response = requests.get(url)
data_dict = response.json()
# Extract the nested dictionary
data = data_dict['data']

data_tuples = [(currency, float(value)) for currency, value in data.items()]
currency_df = spark.createDataFrame(data_tuples, ["Currency", "currency_value"])
# Display the DataFrame
display(currency_df)


# COMMAND ----------

df_table_curated = df.join(currency_df, on=['currency'], how="inner")

df_table_curated = df_table_curated.withColumn('sale_price_usd',curate_sales_price_udf('currency', 'currency_value', 'sale_price'))



UPDATED=datetime.datetime.today().replace(second=0, microsecond=0)

df_table_curated=df_table_curated.withColumn('updated_at', f.lit(UPDATED))

display(df_table_curated)

# COMMAND ----------

df_table_curated = df_table_curated.withColumn('order_date_new',date_format(df_table_curated.order_date, 'MM/dd/yyyy')).drop('order_date').withColumnRenamed('order_date_new', 'order_date')

df_table_curated = df_table_curated.drop('currency_value')

display(df_table_curated)

# COMMAND ----------

df_table_curated = df_table_curated.withColumn('order_date', to_date(col('order_date'), 'MM/dd/yyyy'))

# COMMAND ----------

df_table_curated.createOrReplaceTempView("delta_diff")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silver.orders
# MAGIC USING delta_diff 
# MAGIC ON delta_diff.order_number = silver.orders.order_number
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

spark.sql("update bronze.control_tbl set version="+str(latest_version)+" where tablename='orders'")