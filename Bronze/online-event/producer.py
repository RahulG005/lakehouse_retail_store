# Databricks notebook source
pip install azure-eventhub

# COMMAND ----------

eventMsg1='{"id": 3001, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01", "customer_name": "Julie Rich", "address": "Ap #255-3031 Dui Avenue","city": "Billings","postalcode": "80834","country": "USA","phone": "1-528-884-4331","email": "Donec.felis@nequesedsem.ca","product_name": "microwave","order_date": "18/05/2021","currency": "EUR","order_mode": "NEW","sale_price": 32.34,"order_number": 385 ,"dataVersion": "1.0"}'

eventMsg2='{"id": 3002, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01", "customer_name": "Ratnali Kumar", "address": "Ap #476-7527 Aenean Road","city": "Bhilwara","postalcode": "827484","country": "India","phone": "+91 0952796185","email": "Donec.feugiat@felisadipiscing.co.uk","product_name": "microwave","order_date": "18/05/2021","currency": "USD","order_mode": "NEW","sale_price": 44.45,"order_number": 386 ,"dataVersion": "1.0"}'

eventMsg3='{"id": 3003, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01", "customer_name": "Eden Byrd", "address": "3840 Sed St.","city": "Milton Keynes","postalcode": "C4T 5H5","country": "UK","phone": "00505 931979","email": "lorem@idmagnaet.org","product_name": "microwave","order_date": "18/05/2021","currency": "USD","order_mode": "NEW","sale_price": 64.68,"order_number": 387 ,"dataVersion": "1.0"}'

eventMsg4='{"id": 3004, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01", "customer_name": "Cara Swanson", "address": "P.O. Box 998, 2884 Pharetra. Road","city": "Harrisburg","postalcode": "65895","country": "USA","phone": "1-375-910-8385","email": "velit.justo@lacus.com","product_name": "microwave","order_date": "18/05/2021","currency": "INR","order_mode": "NEW","sale_price": 74.65,"order_number": 388 },"dataVersion": "1.0"}'

eventMsg5='{"id": 3005, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01", "customer_name": "Chaney Figueroa", "address": "546-5675 Pellentesque St.","city": "Newport News","postalcode": "23276","country": "USA","phone": "1-853-137-7417","email": "hymenaeos@Suspendissealiquet.com","product_name": "microwave","order_date": "18/05/2021","currency": "EUR","order_mode": "NEW","sale_price": 85.48,"order_number": 389 },"dataVersion": "1.0"}'

eventMsg6='{"id": 3006, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01", "customer_name": "Naval Ismail", "address": "3687 Magnis Rd.","city": "Itanagar","postalcode": "747675","country": "India","phone": "+91 6997655593","email": "fringilla.est.Mauris@nonsollicitudina.ca","product_name": "microwave","order_date": "18/05/2021","currency": "EUR","order_mode": "NEW","sale_price": 16.77,"order_number": 390 },"dataVersion": "1.0"}'

eventMsg7='{"id": 3007, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01", "customer_name": "Calista French", "address": "P.O. Box 462, 5596 Morbi St.","city": "Flin Flon","postalcode": "H5X 5P1","country": "Canada","phone": "1 (879) 192-881","email": "Etiam.vestibulum@nonlaciniaat.ca","product_name": "microwave","order_date": "18/05/2021","currency": "USD","order_mode": "NEW","sale_price": 17.98,"order_number": 391 },"dataVersion": "1.0"}'

eventMsg8='{"id": 3008, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01", "customer_name": "Magee Cash", "address": "P.O. Box 728, 3231 Vel Street","city": "Chesapeake","postalcode": "46087","country": "USA","phone": "1-488-816-9178","email": "ultrices.Vivamus@acarcu.co.uk","product_name": "microwave","order_date": "18/05/2021","currency": "CAD","order_mode": "NEW","sale_price": 94.88,"order_number": 392 },"dataVersion": "1.0"}'

eventMsg9='{"id": 3009, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01", "customer_name": "Alvin Russell", "address": "P.O. Box 333, 5820 Vel Avenue","city": "New Haven","postalcode": "14867","country": "USA","phone": "1-117-280-4751","email": "egestas@leoMorbi.com","product_name": "microwave","order_date": "18/05/2021","currency": "INR","order_mode": "NEW","sale_price": 24.98,"order_number": 393 },"dataVersion": "1.0"}'

eventMsg10='{"id": 3010, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01", "customer_name": "Chadwick Hebert", "address": "Ap #556-497 In Ave","city": "Hamilton","postalcode": "K3W 8V9","country": "Canada","phone": "1 (365) 681-6481","email": "elit@cursus.org","product_name": "microwave","order_date": "18/05/2021","currency": "CAD","order_mode": "NEW","sale_price": 17.70,"order_number": 394 ,"dataVersion": "1.0"}'

eventMsg11='{"id": 3011, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01",  "customer_name": "Kenyon Daugherty", 	"address": "1339 Elit. Ave","city": "Birmingham","postalcode": "14456","country": "USA","phone": "1-236-491-4683","email": "lacus.Mauris@nonummyultricies.ca","product_name": "microwave","order_date": "19/05/2021","currency": "EUR","order_mode": "NEW","sale_price": 17.70,"order_number": 397 ,"dataVersion": "1.0"}'

eventMsg12='{"id": 3012, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01",  "customer_name": "Vera Russell","address": "Ap #934-9975 Iaculis St.","city": "Bognor Regis","postalcode": "RF1 3IR","country": "UK","phone": "01717 566325","email": "sociosqu@dui.org","product_name": "microwave","order_date": "19/05/2021","currency": "EUR","order_mode": "NEW","sale_price": 45.33,"order_number": 398 ,"dataVersion": "1.0"}'

eventMsg13='{"id": 3013, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01",  "customer_name": "Linda Pugh","address": "Ap #598-6303 Cum Avenue","city": "Gloucester","postalcode": "P1P 6B5","country": "Canada","phone": "1 (450) 461-4621","email": "tincidunt@tempuseu.net","product_name": "microwave","order_date": "19/05/2021","currency": "CAD","order_mode": "NEW","sale_price": 65.32,"order_number": 399 ,"dataVersion": "1.0"}'

eventMsg14='{"id": 3014, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01",  "customer_name": "Brynn Nelson","address": "7635 Dictum Rd.","city": "Boston","postalcode": "75124","country": "USA","phone": "1-769-479-6047","email": "nonummy.ultricies.ornare@Nunc.com","product_name": "microwave","order_date": "19/05/2021","currency": "USD","order_mode": "NEW","sale_price": 105.44,"order_number": 400 ,"dataVersion": "1.0"}'

eventMsg15='{"id": 3015, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01",  "customer_name": "Nelle Moore","address": "5495 Magna. Ave","city": "McCallum","postalcode": "H3K 0A4","country": "Canada","phone": "1 (416) 966-3866","email": "ultrices.a@leo.co.uk","product_name": "microwave","order_date": "19/05/2021","currency": "CAD","order_mode": "NEW","sale_price": 54.33,"order_number": 401 ,"dataVersion": "1.0"}'

eventMsg16='{"id": 3016, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01",  "customer_name": "Ignacia Price","address": "6009 Ipsum Av.","city": "Lerwick","postalcode": "J0 3WK","country": "UK","phone": "06818 437518","email": "non@facilisisvitae.ca","product_name": "microwave","order_date": "19/05/2021","currency": "EUR","order_mode": "NEW","sale_price": 55.33,"order_number": 402 ,"dataVersion": "1.0"}'

eventMsg17='{"id": 3017, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01",  "customer_name": "Ciara Salinas","address": "758-7308 Mauris. Rd.","city": "Burlington","postalcode": "95877","country": "USA","phone": "1-276-382-6027","email": "ullamcorper.velit.in@iaculisaliquet.net","product_name": "microwave","order_date": "19/05/2021","currency": "USD","order_mode": "NEW","sale_price": 76.29,"order_number": 403 ,"dataVersion": "1.0"}'

eventMsg18='{"id": 3018, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01",  "customer_name": "Beatrice Tucker","address": "649-7099 Nulla St.","city": "Oakham","postalcode": "QE23 6GA"	,"country": "UK","phone": "04484 843599","email": "ullamcorper.Duis@est.net","product_name": "microwave","order_date": "19/05/2021","currency": "EUR","order_mode": "NEW","sale_price": 98.11,"order_number": 404 ,"dataVersion": "1.0"}'

eventMsg19='{"id": 3019, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01",  "customer_name": "Brenna Robles","address": "246-3360 Venenatis Street","city": "Stratford","postalcode": "N4R 2M9"	,"country": "Canada","phone": "1 (403) 201-6443","email": "dui@ligula.edu","product_name": "microwave","order_date": "19/05/2021","currency": "CAD","order_mode": "NEW","sale_price": 90.56,"order_number": 405 ,"dataVersion": "1.0"}'

eventMsg20='{"id": 3020, "eventType": "recordInserted", "subject": "ecomm/customers", "eventTime": "2021-01-01",  "customer_name": "Beverly Tillman","address": "501-9350 Mauris Street","city": "Jedburgh","postalcode": "PO3C 0VI"	,"country": "UK","phone": "07464 422658","email": "tempor.bibendum.Donec@Vivamus.net","product_name": "microwave","order_date": "19/05/2021","currency": "EUR","order_mode": "NEW","sale_price": 17.70,"order_number": 406 ,"dataVersion": "1.0"}'


# COMMAND ----------

from azure.eventhub import EventHubProducerClient,EventData

producer=EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://onlinestore.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=j+lYpTxNIaJsHzGXsMgMn/LFsZhxuBSmX+AEhGcq42U=",eventhub_name="retail")

event_batch_data=producer.create_batch()

event_batch_data.add(EventData(eventMsg1))

event_batch_data.add(EventData(eventMsg2))
event_batch_data.add(EventData(eventMsg3))
event_batch_data.add(EventData(eventMsg4))
event_batch_data.add(EventData(eventMsg5))
event_batch_data.add(EventData(eventMsg6))
event_batch_data.add(EventData(eventMsg7))
event_batch_data.add(EventData(eventMsg8))
event_batch_data.add(EventData(eventMsg9))
event_batch_data.add(EventData(eventMsg10))
event_batch_data.add(EventData(eventMsg11))
event_batch_data.add(EventData(eventMsg12))
event_batch_data.add(EventData(eventMsg13))
event_batch_data.add(EventData(eventMsg14))
event_batch_data.add(EventData(eventMsg15))
event_batch_data.add(EventData(eventMsg16))
event_batch_data.add(EventData(eventMsg17))
event_batch_data.add(EventData(eventMsg18))
event_batch_data.add(EventData(eventMsg19))
event_batch_data.add(EventData(eventMsg20))


producer.send_batch(event_batch_data)

print("Done")

producer.close()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze.eventsonline