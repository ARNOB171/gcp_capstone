#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#fact_daily_orders

from google.cloud import bigquery
from google.oauth2 import service_account
import os
credential_path = "C:\\Users\\arnob.chakraborty\\OneDrive - Fractal Analytics Pvt. Ltd\\Desktop\\Capstone_Project\\access-2-352609-ca316bb8c3b7.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# Construct a BigQuery client object.
client = bigquery.Client()

schema = [
    bigquery.SchemaField("customerid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("orderid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("order_received_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("order_delivery_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("pincode", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("order_amount", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("item_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("order_delivery_time_seconds", "FLOAT", mode="REQUIRED"),
]

table = bigquery.Table("access-2-352609.star_schema.fact_daily_orders", schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)


# In[ ]:



#dim_customer

from google.cloud import bigquery
from google.oauth2 import service_account
import os
credential_path = "C:\\Users\\arnob.chakraborty\\OneDrive - Fractal Analytics Pvt. Ltd\\Desktop\\Capstone_Project\\access-2-352609-ca316bb8c3b7.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
# table_id = "your-project.your_dataset.your_table_name"

schema = [
    bigquery.SchemaField("customerid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("address_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("start_date", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("end_date", "TIMESTAMP", mode="REQUIRED")
]

table = bigquery.Table("access-2-352609.star_schema.dim_customer", schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)


# In[ ]:


#dim_product

from google.cloud import bigquery
from google.oauth2 import service_account
import os
credential_path = "C:\\Users\\arnob.chakraborty\\OneDrive - Fractal Analytics Pvt. Ltd\\Desktop\\Capstone_Project\\access-2-352609-ca316bb8c3b7.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
# table_id = "your-project.your_dataset.your_table_name"

schema = [
    bigquery.SchemaField("productid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("productcode", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("productname", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("sku", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rate", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("isactive", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("start_date", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("end_date", "TIMESTAMP", mode="REQUIRED")
]

table = bigquery.Table("access-2-352609.star_schema.dim_product", schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)


# In[ ]:


#dim_address

from google.cloud import bigquery
from google.oauth2 import service_account
import os
credential_path = "C:\\Users\\arnob.chakraborty\\OneDrive - Fractal Analytics Pvt. Ltd\\Desktop\\Capstone_Project\\access-2-352609-ca316bb8c3b7.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
# table_id = "your-project.your_dataset.your_table_name"

schema = [
    bigquery.SchemaField("address_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("address", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("state", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("pincode", "INTEGER", mode="REQUIRED")
]

table = bigquery.Table("access-2-352609.star_schema.dim_address", schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)


# In[ ]:


#f_order_details

from google.cloud import bigquery
from google.oauth2 import service_account
import os
credential_path = "C:\\Users\\arnob.chakraborty\\OneDrive - Fractal Analytics Pvt. Ltd\\Desktop\\Capstone_Project\\access-2-352609-ca316bb8c3b7.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
# table_id = "your-project.your_dataset.your_table_name"

schema = [
    bigquery.SchemaField("orderid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("order_delivery_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("productid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("quantity", "INTEGER", mode="REQUIRED")
]

table = bigquery.Table("access-2-352609.star_schema.f_order_details", schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)


# In[ ]:


#dim_order

from google.cloud import bigquery
from google.oauth2 import service_account
import os
credential_path = "C:\\Users\\arnob.chakraborty\\OneDrive - Fractal Analytics Pvt. Ltd\\Desktop\\Capstone_Project\\access-2-352609-ca316bb8c3b7.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
# table_id = "your-project.your_dataset.your_table_name"

schema = [
    bigquery.SchemaField("orderid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("order_status_update_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("order_status", "STRING", mode="REQUIRED")
]

table = bigquery.Table("access-2-352609.star_schema.dim_order", schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)

