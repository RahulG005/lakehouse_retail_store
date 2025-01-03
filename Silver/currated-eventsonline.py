# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists silver; 
# MAGIC CREATE TABLE if not exists silver.eventsonline (
# MAGIC   id INT,
# MAGIC   customer_name STRING,
# MAGIC   address STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   currency STRING,
# MAGIC   email STRING,
# MAGIC   order_date STRING,
# MAGIC   order_mode STRING,
# MAGIC   order_number INT,
# MAGIC   phone STRING,
# MAGIC   postalcode STRING,
# MAGIC   product_name STRING,
# MAGIC   sale_price FLOAT,
# MAGIC   sale_price_usd FLOAT)

# COMMAND ----------

# %sql
# insert into bronze.control_tbl values ('eventsonline',0);

# COMMAND ----------

srcDelta = spark.sql("select * from bronze.control_tbl where tablename='eventsonline'")
last_version=srcDelta.collect()[0][1]
print(last_version)

# COMMAND ----------

import datetime
from pyspark.sql import functions as f
from delta.tables import *
srcDeltaTable = DeltaTable.forName(spark, "bronze.eventsonline")
latest_version = srcDeltaTable.history(1).collect()[0][0]
print(latest_version)
UPDATED=datetime.datetime.today().replace(second=0, microsecond=0)

# COMMAND ----------

df=spark.sql(f"select * from bronze.eventsonline@v{latest_version} except all select * from bronze.eventsonline@v{last_version} ")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,array,ArrayType,DateType,TimestampType,FloatType
import hashlib
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

def curate_sales_price(currency, currency_value, sales_price):
  if (currency != 'USD'):
    curated_value = float(sales_price)/float(currency_value)
    return float(curated_value)
  else:
    return float(sales_price)

def curate_country(country):
    if(country == 'USA' or country == 'United States'):
        curated_value = 'USA'
    elif(country == 'UK' or country == 'United Kingdom'):
        curated_value = 'UK'
    elif(country == 'CAN' or country == 'Canada'):
        curated_value = 'CAN'
    elif(country == 'IND' or country == 'India'):
        curated_value = 'IND'
    else:
        curated_value = country
    return curated_value
        
def mask_value(column):
    mask_value = hashlib.sha256(column.encode()).hexdigest()
    return mask_value

mask_udf = udf(mask_value, StringType())
curate_country_udf = udf(curate_country, StringType())
curate_sales_price_udf = udf(curate_sales_price, FloatType())

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

df_data_values = df.select('id','customer_name', 'address', 'city', 'country', 'currency', 'email',
                                      'order_date', 'order_mode', 'order_number', 'phone','postalcode', 'product_name', 'sale_price' )

df_data_values = df_data_values.withColumn('phone_masked',mask_udf('phone')).drop('phone').withColumnRenamed('phone_masked', 'phone')
df_data_values = df_data_values.withColumn('address_masked',mask_udf('address')).drop('address').withColumnRenamed('address_masked', 'address')
df_data_values = df_data_values.withColumn('order_date', from_unixtime(unix_timestamp('order_date', 'dd/MM/yyy')))

# COMMAND ----------

df_data_values = df_data_values.withColumn('country_curated',curate_country_udf('country')).drop('country').withColumnRenamed('country_curated', 'country')

# COMMAND ----------

df_data_values = df_data_values.join(currency_df, on=['currency'], how="inner")
df_data_values = df_data_values.withColumn('sale_price_usd',curate_sales_price_udf('currency', 'currency_value', 'sale_price'))

df_data_values = df_data_values.withColumn('order_date_new', to_date(df_data_values.order_date, 'yyyy-MM-dd HH:mm:ss')).drop('order_date').withColumnRenamed('order_date_new', 'order_date')

df_data_values=df_data_values.drop("df_data_values")
display(df_data_values)

# COMMAND ----------

from delta.tables import *
SilverDetlaTable = DeltaTable.forName(spark, "silver.eventsonline")

SilverDetlaTable.alias("silver").merge(
df_data_values.alias("bronze_data"),
                  "silver.email = bronze_data.email")                                          \
                  .whenMatchedUpdate(set = {
                                            "id":    "bronze_data.id", 	\
                                            "customer_name":    "bronze_data.customer_name", 	\
                                            "address":          "bronze_data.address",           \
                                            "city":             "bronze_data.city",              \
                                            "country":          "bronze_data.country",           \
                                            "currency":         "bronze_data.currency",          \
                                            "email":            "bronze_data.email",             \
                                            "order_date":       "bronze_data.order_date",        \
                                            "order_mode":       "bronze_data.order_mode",        \
                                            "order_number":     "bronze_data.order_number",      \
                                            "phone":            "bronze_data.phone",             \
                                            "postalcode":       "bronze_data.postalcode",        \
                                            "product_name":     "bronze_data.product_name",      \
                                            "sale_price":       "bronze_data.sale_price",        \
                                            "sale_price_usd":   "bronze_data.sale_price_usd"    \
                                            } )     \
                  .whenNotMatchedInsert(values =                                                  \
                     {                      "id":    "bronze_data.id", 	\
                                            "customer_name":    "bronze_data.customer_name", 	\
                                            "address":          "bronze_data.address",           \
                                            "city":             "bronze_data.city",              \
                                            "country":          "bronze_data.country",           \
                                            "currency":         "bronze_data.currency",          \
                                            "email":            "bronze_data.email",             \
                                            "order_date":       "bronze_data.order_date",        \
                                            "order_mode":       "bronze_data.order_mode",        \
                                            "order_number":     "bronze_data.order_number",      \
                                            "phone":            "bronze_data.phone",             \
                                            "postalcode":       "bronze_data.postalcode",        \
                                            "product_name":     "bronze_data.product_name",      \
                                            "sale_price":       "bronze_data.sale_price",        \
                                            "sale_price_usd":   "bronze_data.sale_price_usd"   \
                                                  \
                     }                                                                            \
                   ).execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from silver.eventsonline

# COMMAND ----------

spark.sql("update bronze.control_tbl set version="+str(latest_version)+" where tablename='eventsonline'")