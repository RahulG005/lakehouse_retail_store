# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists silver; 
# MAGIC create table IF NOT EXISTS silver.customers(
# MAGIC id integer, 
# MAGIC name varchar(200), 
# MAGIC address varchar(200),
# MAGIC city varchar(200),
# MAGIC postalcode varchar(200),
# MAGIC country varchar(200),
# MAGIC phone varchar(200),
# MAGIC email varchar(200),
# MAGIC credit_card varchar(200),
# MAGIC updated_at TIMESTAMP
# MAGIC )

# COMMAND ----------

# %sql
# insert into bronze.control_tbl values ('customers',0);

# COMMAND ----------

import datetime
import hashlib
from datetime import timedelta, date
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,array,ArrayType,DateType,TimestampType

def curate_email(email):
  curated_value = email.lower()
  return curated_value

def mask_value(column):
    mask_value = hashlib.sha256(column.encode()).hexdigest()
    return mask_value

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

mask_udf = udf(mask_value, StringType())
curate_country_udf = udf(curate_country, StringType())
curate_email_udf = udf(curate_email, StringType())




# COMMAND ----------

srcDelta = spark.sql("select * from bronze.control_tbl where tablename='customers' ")


# COMMAND ----------

last_version=srcDelta.collect()[0][1]

# COMMAND ----------

from delta.tables import *
srcDeltaTable = DeltaTable.forName(spark, "bronze.customers")

# COMMAND ----------

display(srcDeltaTable.history())

# COMMAND ----------

latest_version=srcDeltaTable.history(1).collect()[0][0]
print(latest_version)

# COMMAND ----------

df=spark.sql(f"select * from bronze.customers@v{latest_version} except all select * from bronze.customers@v{last_version} ")

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.withColumn('email_curated',curate_email_udf('email')).drop('email').withColumnRenamed('email_curated', 'email')
df = df.withColumn('country_curated',curate_country_udf('country')).drop('country').withColumnRenamed('country_curated', 'country')
df = df.withColumn('phone_masked',mask_udf('phone')).drop('phone').withColumnRenamed('phone_masked', 'phone')
df = df.withColumn('credit_card_masked',mask_udf('credit_card')).drop('credit_card').withColumnRenamed('credit_card_masked', 'credit_card')
df = df.withColumn('address_masked',mask_udf('address')).drop('address').withColumnRenamed('address_masked', 'address')

UPDATED=datetime.datetime.today().replace(second=0, microsecond=0)

df=df.withColumn('updated_at', f.lit(UPDATED))

display(df)

# COMMAND ----------

df.createOrReplaceTempView("delta_diff")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silver.customers
# MAGIC USING delta_diff 
# MAGIC ON delta_diff.id = silver.customers.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

spark.sql("update bronze.control_tbl set version="+str(latest_version)+" where tablename='customers'")