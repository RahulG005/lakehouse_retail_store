# Databricks notebook source
# MAGIC %scala
# MAGIC
# MAGIC Class.forName("org.apache.spark.sql.eventhubs.EventHubsSource")

# COMMAND ----------

from pyspark.sql.functions import *

connstring="Endpoint=sb://onlinestore.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=j+lYpTxNIaJsHzGXsMgMn/LFsZhxuBSmX+AEhGcq42U="

eventhubname="retail"

event_connstring=connstring+";EntityPath="+eventhubname

encrypt_obj=sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(event_connstring)

encrypt_conf={"eventHubs.connectionString":encrypt_obj}

df=spark.readStream.format("eventhubs").options(**encrypt_conf)

raw_df=df.load()



schema=StructType().add("id","integer").add("customer_name","string").add("address","string").add("city","string").add("country","string").add("currency","string").add("email","string").add("order_date","string").add("order_mode","string").add("order_number","integer").add("phone","string").add("postalcode","string").add("product_name","string").add("sale_price","float")

raw_df_1=raw_df.withColumn("data",from_json(col("body").cast("string"),schema))



curated_df=raw_df_1.withColumn("id",col("data.id")).withColumn("customer_name",col("data.customer_name")).withColumn("address",col("data.address")).withColumn("city",col("data.city")).withColumn("country",col("data.country")).withColumn("currency",col("data.currency")).withColumn("email",col("data.email")).withColumn("order_date",col("data.order_date")).withColumn("order_mode",col("data.order_mode")).withColumn("order_number",col("data.order_number")).withColumn("phone",col("data.phone")).withColumn("postalcode",col("data.postalcode")).withColumn("product_name",col("data.product_name")).withColumn("sale_price",col("data.sale_price"))

final_df=curated_df.select("id","customer_name","address","city","country","currency","email","order_date","order_mode","order_number","phone","postalcode","product_name","sale_price")

display(final_df)

final_df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/delta/events/_checkpoints/").toTable("bronze.eventsonline")
