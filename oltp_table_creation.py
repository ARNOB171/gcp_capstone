#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
from collections import defaultdict

from faker import Faker
import uuid
from faker.providers import DynamicProvider
fake = Faker(['en_IN'])

# Will return ['en_US']
fake.locales


# DATA CREATION FOR customer_master

# In[ ]:


import random
State = DynamicProvider(
     provider_name="State",
     elements=["Maharashtra", "Karnataka", "Andhra_Pradesh", "Uttar_Pradesh", "Haryana", "Rajasthan"],
)

City = DynamicProvider(
     provider_name="City",
     elements=["Pune", "Mumbai", "Satara", "Nagpur", "Nasik", "Bengalore", "Mysore", "Hubli", "Visakhapatnam", "Vijayawada", "Nellore", "Lucknow", "Kanpur", "Ghaziabad", "Chandigarh", "Delhi", "Gurgaon", "Jaipur", "Kota", "Jodhpur"],
)


fake = Faker(['en_IN'])

# then add new provider to faker instance
fake.add_provider(State)
fake.add_provider(City)

# now you can use:
fake.State()
fake.City()


# In[ ]:


fake_data_dn = defaultdict(list)


# In[ ]:


fake.unique.clear()


# In[ ]:


for _ in range(1000):
    fake_data_dn['customerid'].append(fake.unique.random_int(1,1000))
    #fake.unique.clear()
    fake_data_dn['name'].append(fake.name())
    fake_data_dn['address'].append(fake.street_address())
    fake_data_dn['city'].append(fake.City())
    fake_data_dn['state'].append(fake.State())
    fake_data_dn['pincode'].append(fake.postcode())
    fake_data_dn['update_timestamp'].append(fake.date_time_this_year())
    
#fake.unique.clear()


    


# In[ ]:


df = pd.DataFrame(fake_data_dn)


# In[ ]:


df.loc[df["city"] == "Pune", "state"] = "Maharashtra"
df.loc[df["city"] == "Mumbai", "state"] = "Maharashtra"
df.loc[df["city"] == "Satara", "state"] = "Maharashtra"
df.loc[df["city"] == "Nasik", "state"] = "Maharashtra"
df.loc[df["city"] == "Nagpur", "state"] = "Maharashtra"

df.loc[df["city"] == "Hubli", "state"] = "Karnataka"
df.loc[df["city"] == "Mysore", "state"] = "Karnataka"
df.loc[df["city"] == "Bengalore", "state"] = "Karnataka"

df.loc[df["city"] == "Jodhpur", "state"] = "Rajasthan"
df.loc[df["city"] == "Jaipur", "state"] = "Rajasthan"
df.loc[df["city"] == "Kota", "state"] = "Rajasthan"

df.loc[df["city"] == "Gurgaon", "state"] = "Haryana"
df.loc[df["city"] == "Delhi", "state"] = "Haryana"
df.loc[df["city"] == "Chandigarh", "state"] = "Haryana"

df.loc[df["city"] == "Kanpur", "state"] = "Uttar_Pradesh"
df.loc[df["city"] == "Ghaziabad", "state"] = "Uttar_Pradesh"
df.loc[df["city"] == "Lucknow", "state"] = "Uttar_Pradesh"

df.loc[df["city"] == "Visakhapatnam", "state"] = "Andhra_Pradesh"
df.loc[df["city"] == "Vijayawada", "state"] = "Andhra_Pradesh"
df.loc[df["city"] == "Nellore", "state"] = "Andhra_Pradesh"


# In[ ]:


import psycopg2


# In[ ]:


#establishing the connection
conn = psycopg2.connect(
   database="postgres", user='postgres', password='1234', host='35.237.21.125', port= '5432')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()



# In[ ]:


temp =0
for i in range(len(df)):
    customerid = int(df['customerid'].iloc[i])
    name = str(df['name'].iloc[i])
    address = str(df['address'].iloc[i])
    city = str(df['city'].iloc[i])
    state = str(df['state'].iloc[i])
    pincode = int(df['pincode'].iloc[i])
    update_timestamp = str(df['update_timestamp'].iloc[i])
    
    query = ("insert into customer_master(customerid,name, address, city, state, pincode, update_timestamp)"
         "values (%s, %s, %s, %s, %s, %s, %s)")

    val = (customerid,name, address, city, state, pincode, update_timestamp)
    cursor.execute(query,val)
    conn.commit()
    temp = temp + 1
    print(temp, "record inserted",customerid)


# In[ ]:




fake = Faker(['en_IN'])

# then add new provider to faker instance
fake.add_provider(product_name)
# fake.add_provider(City)

# now you can use:
fake.product_name()





# In[ ]:


fake_data_prod = defaultdict(list)


# In[ ]:


#product_master

import random
from string import ascii_lowercase
L = list(ascii_lowercase) + [letter1+letter2 for letter1 in ascii_lowercase for letter2 in ascii_lowercase]

elements=["apples", "oranges", "peaches", "pears", "prunes", "plums", "strawberries", "raisins", "kiwi", "pineapple", 
"bananas", "cauliflower", "broccoli", "carrots", "garlic", "celery", "green peppers", "corn", "tomatoes", "mushrooms", 
"rice", "canned vegetables", "cheese", "oatmeal", "butter", "chicken", "fresh fish", "potato chips", "toilet paper", "laundry soap", 
"wieners", "wiener buns", "eggs", "bread", "bread", "jelly", "cheese", "mayonnaise", "pasta", "ketchup", 
"yogurt", "cottage cheese", "cereal", "milk", "orange juice", "apple", "juice", "prune juice", "ice cream", "soda"] 

pre_sku=0
j=0
for name in elements:
  j=j+1 
  for i in range(2):
    fake_data_prod['productid'].append(fake.unique.random_int(1,100))
    fake_data_prod['productcode'].append(L[j-1]+str(format(i+1, '02d')))
    fake_data_prod['productname'].append(name)
    for x in range(5):
     sku = fake.random_int(1,5)
     if (x!=0):
      if (sku==pre_sku):
        continue
      else:
        break
    pre_sku = sku
    
    fake_data_prod['sku'].append(str(sku)+"KG")
    fake_data_prod['rate'].append(str(fake.random_int(30,200)))
    fake_data_prod['isactive'].append(bool(random.getrandbits(1)))
    
#fake.unique.clear()


# In[ ]:


df = pd.DataFrame(fake_data_prod)


# In[ ]:


df


# In[ ]:


import psycopg2


# In[ ]:


#establishing the connection
conn = psycopg2.connect(
   database="postgres", user='postgres', password='1234', host='35.237.21.125', port= '5432')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()


# In[ ]:


temp =0
for i in range(len(df)):
    productid = int(df['productid'].iloc[i])
    productcode = str(df['productcode'].iloc[i])
    productname = str(df['productname'].iloc[i])
    sku = str(df['sku'].iloc[i])
    rate = int(df['rate'].iloc[i])
    isactive = bool(df['isactive'].iloc[i])
    
    
    query = ("insert into product_master(productid, productcode, productname, sku, rate, isactive)"
         "values (%s, %s, %s, %s, %s, %s)")

    val = (productid, productcode, productname, sku, rate, isactive)
    cursor.execute(query,val)
    conn.commit()
    temp = temp + 1
    print(temp, "record inserted",productid)


# In[ ]:


fake_data_order_details = defaultdict(list)


# In[ ]:


#order_details

order_status_list = ["Received","InProgress","Delivered"]

import random
from datetime import timedelta, datetime

for i in range(20000):
#  o_id = fake.unique.random_int(1,20000)
#  c_id = fake.unique.random_int(1,1000)
 x = 0
 o_id = i+1
 c_id = fake.random_int(1,1000)
 r_datetime = fake.date_time_this_year()  
 for olist in order_status_list:
  fake_data_order_details['orderid'].append(o_id)
  fake_data_order_details['customerid'].append(c_id)
    
 
  
  while(x<10):
     nexttime = fake.random_int(10,48)
     
     if (x!=0):
      
      n_datetime = date_time + timedelta(hours = nexttime)
      if (n_datetime<=date_time):
        continue
      else:
        date_time = n_datetime
        break
     else:
       date_time = r_datetime
       break
  x = 1
  
  fake_data_order_details['order_status_update_timestamp'].append(date_time)
  fake_data_order_details['order_status'].append(olist)
 i=i+1

    
    
#fake.unique.clear()


# In[ ]:


df = pd.DataFrame(fake_data_order_details)


# In[ ]:


import psycopg2


# In[ ]:


#establishing the connection
conn = psycopg2.connect(
   database="postgres", user='postgres', password='1234', host='35.237.21.125', port= '5432')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()


# In[ ]:


temp =0
for i in range(len(df)):
    orderid = int(df['orderid'].iloc[i])
    customerid = int(df['customerid'].iloc[i])
    order_status_update_timestamp = str(df['order_status_update_timestamp'].iloc[i])
    order_status = str(df['order_status'].iloc[i])
   
    
    
    query = ("insert into order_details(orderid, customerid, order_status_update_timestamp, order_status)"
         "values (%s, %s, %s, %s)")

    val = (orderid, customerid, order_status_update_timestamp, order_status)
    cursor.execute(query,val)
    conn.commit()
    temp = temp + 1
    print(temp, "record inserted",orderid)


# In[ ]:





# In[ ]:


fake_data_order_items = defaultdict(list)


# In[ ]:


#order_items


import random
from datetime import timedelta, datetime

for _ in range(200000):
    fake_data_order_items['orderid'].append(fake.random_int(1,20000))
    fake_data_order_items['productid'].append(fake.random_int(1,100))
    fake_data_order_items['quantity'].append(random.choice([1,2,3,4,5]))
    
    
#fake.unique.clear()


# In[ ]:


df = pd.DataFrame(fake_data_order_items)


# In[ ]:


newdf = df.drop_duplicates(
  subset = ['orderid', 'productid'],
  keep = 'last').reset_index(drop = True)


# In[ ]:


import psycopg2


# In[ ]:


#establishing the connection
conn = psycopg2.connect(
   database="postgres", user='postgres', password='1234', host='35.237.21.125', port= '5432')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()


# In[ ]:


temp =0
for i in range(len(newdf)):
    orderid = int(newdf['orderid'].iloc[i])
    productid = str(newdf['productid'].iloc[i])
    quantity = str(newdf['quantity'].iloc[i])
    
    query = ("insert into order_items(orderid, productid, quantity)"
         "values (%s, %s, %s)")

    val = (orderid, productid, quantity)
    cursor.execute(query,val)
    conn.commit()
    temp = temp + 1
    print(temp, "record inserted",orderid)


# In[ ]:


#adding more 5000 orders


# In[139]:


fake_data_order_details_five = defaultdict(list)


# In[140]:


#order_details

order_status_list = ["Received","InProgress","Delivered"]

import random
from datetime import timedelta, datetime

for i in range(20000,25000):
#  o_id = fake.unique.random_int(1,20000)
#  c_id = fake.unique.random_int(1,1000)
 x = 0
 o_id = i+1
 c_id = fake.random_int(1,1000)
 r_datetime = fake.date_time_this_year()  
 for olist in order_status_list:
  fake_data_order_details_five['orderid'].append(o_id)
  fake_data_order_details_five['customerid'].append(c_id)
    
 
  
  while(x<10):
     nexttime = fake.random_int(10,48)
     
     if (x!=0):
      
      n_datetime = date_time + timedelta(hours = nexttime)
      if (n_datetime<=date_time):
        continue
      else:
        date_time = n_datetime
        break
     else:
       date_time = r_datetime
       break
  x = 1
  
  fake_data_order_details_five['order_status_update_timestamp'].append(date_time)
  fake_data_order_details_five['order_status'].append(olist)
 i=i+1

    
    
#fake.unique.clear()


# In[141]:


df = pd.DataFrame(fake_data_order_details_five)


# In[142]:


# import packages
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# establish connections
conn_string = 'postgresql://postgres:1234@35.237.21.125/postgres'

db = create_engine(conn_string)
conn = db.connect()
conn1 = psycopg2.connect(
	database="postgres",
user='postgres',
password='1234',
host='35.237.21.125',
port= '5432'
)

conn1.autocommit = True
cursor = conn1.cursor()


# converting data to sql
df.to_sql('order_details', conn, if_exists= 'append', index=False)


conn1.commit()
conn1.close()


# In[148]:


fake_data_order_items_five = defaultdict(list)


# In[149]:


#order_items


import random
from datetime import timedelta, datetime

for _ in range(50000):
    fake_data_order_items_five['orderid'].append(fake.random_int(20001,25000))
    fake_data_order_items_five['productid'].append(fake.random_int(1,100))
    fake_data_order_items_five['quantity'].append(random.choice([1,2,3,4,5]))
    
    
#fake.unique.clear()


# In[150]:


df = pd.DataFrame(fake_data_order_items_five)


# In[ ]:





# In[151]:


newdf = df.drop_duplicates(
  subset = ['orderid', 'productid'],
  keep = 'last').reset_index(drop = True)


# In[152]:


len(newdf["orderid"].unique())


# In[153]:


# import packages
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# establish connections
conn_string = 'postgresql://postgres:1234@35.237.21.125/postgres'

db = create_engine(conn_string)
conn = db.connect()
conn1 = psycopg2.connect(
	database="postgres",
user='postgres',
password='1234',
host='35.237.21.125',
port= '5432'
)

conn1.autocommit = True
cursor = conn1.cursor()


# converting data to sql
newdf.to_sql('order_items', conn, if_exists= 'append', index=False)


conn1.commit()
conn1.close()


# In[ ]:




