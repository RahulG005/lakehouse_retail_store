# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists silver; 
# MAGIC create table IF NOT EXISTS silver.weblogs(
# MAGIC   agent varchar(255),
# MAGIC   remote_ip varchar(255),
# MAGIC   request varchar(255),
# MAGIC   response bigint,
# MAGIC   time string,
# MAGIC   city string,
# MAGIC   country string
# MAGIC )

# COMMAND ----------

# %sql
# insert into bronze.control_tbl values ('weblogs',0);

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
import hashlib
from datetime import timedelta, date
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,array,ArrayType,DateType,TimestampType

# COMMAND ----------

srcDelta = spark.sql("select * from bronze.control_tbl where tablename='weblogs' ")
last_version=srcDelta.collect()[0][1]
print(last_version)

# COMMAND ----------

from delta.tables import *
srcDeltaTable = DeltaTable.forName(spark, "bronze.weblogs")
display(srcDeltaTable.history())
latest_version=srcDeltaTable.history(1).collect()[0][0]
print(latest_version)

# COMMAND ----------

df=spark.sql(f"select * from bronze.weblogs@v{latest_version} except all select * from bronze.weblogs@v{last_version} ")
display(df)

# COMMAND ----------

import ipinfo
access_token = 'f9a0a4703bf9e4'
handler = ipinfo.getHandler(access_token)



# COMMAND ----------


def fetch_city(ip): 
    try: 
        details = handler.getDetails(ip) 
        return details.city
    except Exception as e: 
        return "InvalidIP"

def fetch_country(ip): 
    try: 
        details = handler.getDetails(ip) 
        return details.country 
    except Exception as e: 
        return "InvalidIP"

fetch_city_udf = udf(fetch_city, StringType()) 
fetch_country_udf = udf(fetch_country, StringType())


# COMMAND ----------

df_with_details = df.withColumn("city", fetch_city_udf(df.remote_ip)).withColumn("country", fetch_country_udf(df.remote_ip))

# COMMAND ----------

display(df_with_details)

# COMMAND ----------

df_with_details.createOrReplaceTempView('delta_diff')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silver.weblogs
# MAGIC USING delta_diff 
# MAGIC ON delta_diff.remote_ip = silver.weblogs.remote_ip
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *