#!/usr/bin/env python
# coding: utf-8

# In[26]:


import pandas as pd


# In[27]:


import pandas_gbq as pd_gbq


# In[28]:


from pythonbq import pythonbq

myProject=pythonbq(
  bq_key_path="/home/airflow/gcs/dags/scripts/access-2-352609-ca316bb8c3b7.json",
  project_id='access-2-352609'
)
#dim_order
SQL_CODE="""
SELECT * FROM EXTERNAL_QUERY("projects/access-2-352609/locations/us-east1/connections/customer_master", 
"SELECT orderid,order_status_update_timestamp, order_status FROM order_details;")
where orderid>(select COALESCE(max(orderid),0) from star_schema.dim_order)
"""
output=myProject.query(sql=SQL_CODE)

pd_gbq.to_gbq(output, 'star_schema.dim_order', project_id='access-2-352609', if_exists='append')


# In[29]:


#f_order_details
SQL_CODE1="""
SELECT * FROM EXTERNAL_QUERY("projects/access-2-352609/locations/us-east1/connections/customer_master", 
"Select o_d.orderid,o_d.order_status_update_timestamp as order_delivery_timestamp,o_i.productid,o_i.quantity from order_details o_d inner join (select * from order_items) o_i on o_d.orderid=o_i.orderid where o_d.order_status = 'Delivered';")
where orderid>(select COALESCE(max(orderid),0) from star_schema.f_order_details)
"""
output1=myProject.query(sql=SQL_CODE1)

pd_gbq.to_gbq(output1, 'star_schema.f_order_details', project_id='access-2-352609', if_exists='append')


# In[30]:


#fact_daily_orders
SQL_CODE2="""
SELECT * FROM EXTERNAL_QUERY("projects/access-2-352609/locations/us-east1/connections/customer_master", 
"select x.customerid,x.orderid,x.order_received_timestamp,x.order_delivery_timestamp,x.pincode,cast(y.order_amount as int),y.item_count, ((DATE_PART('day', x.order_delivery_timestamp::timestamp - x.order_received_timestamp::timestamp) * 24 + DATE_PART('hour', x.order_delivery_timestamp::timestamp - x.order_received_timestamp::timestamp)) * 60 + DATE_PART('minute', x.order_delivery_timestamp::timestamp - x.order_received_timestamp::timestamp)) * 60 + DATE_PART('second', x.order_delivery_timestamp::timestamp - x.order_received_timestamp::timestamp) as order_delivery_time_seconds from (SELECT a.orderid, a.customerid,a.order_received_timestamp, a.order_delivery_timestamp, b.pincode from (SELECT orderid, customerid,MAX(order_status_update_timestamp) FILTER (WHERE order_status = 'Received') AS order_received_timestamp, MAX(order_status_update_timestamp) FILTER (WHERE order_status = 'Delivered') AS order_delivery_timestamp FROM order_details GROUP BY orderid, customerid) a INNER JOIN customer_master b on a.customerid=b.customerid) x INNER JOIN (select e.orderid, sum(e.quantity) as item_count, sum(e.amount) as order_amount from(SELECT d.orderid, c.productid, c.rate, d.quantity , c.rate*d.quantity as amount from product_master c INNER JOIN order_items d on c.productid=d.productid where isactive=True) e group by e.orderid) y on x.orderid=y.orderid")
where orderid>(select COALESCE(max(orderid),0) from star_schema.fact_daily_orders)
"""
output2=myProject.query(sql=SQL_CODE2)

pd_gbq.to_gbq(output2, 'star_schema.fact_daily_orders', project_id='access-2-352609', if_exists='append')


# In[31]:


#dim_customer
SQL_CODE4="""
SELECT * FROM EXTERNAL_QUERY("projects/access-2-352609/locations/us-east1/connections/customer_master", 
"select customerid,name,ROW_NUMBER() OVER() as address_id, update_timestamp as start_date, cast(null as timestamp) as end_date from customer_master;")
where customerid>(select COALESCE(max(customerid),0) from star_schema.dim_customer)
"""
output4=myProject.query(sql=SQL_CODE4)

pd_gbq.to_gbq(output4, 'star_schema.dim_customer', project_id='access-2-352609', if_exists='append')



# In[32]:


#dim_address
SQL_CODE5="""
select `star_schema.dim_customer`.address_id,a.address, a.city, a.state, a.pincode from
(SELECT * FROM EXTERNAL_QUERY("projects/access-2-352609/locations/us-east1/connections/customer_master", 
"select customerid, address, city, state, pincode from customer_master;")) a
INNER JOIN `star_schema.dim_customer` 
on a.customerid=`star_schema.dim_customer`.customerid
where `star_schema.dim_customer`.address_id not in(select distinct address_id from star_schema.dim_address)
"""
output5=myProject.query(sql=SQL_CODE5)

pd_gbq.to_gbq(output5, 'star_schema.dim_address', project_id='access-2-352609', if_exists='append')



# In[33]:


#dim_product
SQL_CODE6="""
SELECT * FROM EXTERNAL_QUERY("projects/access-2-352609/locations/us-east1/connections/customer_master", 
"select a.productid,a.productcode,a.productname,a.sku,cast(a.rate as int),a.isactive, cast(a.start_date as timestamp), cast(a.end_date as timestamp) from(select *,'2020-01-01 00:00:00.417 +0530' start_date, case when isactive=false then '2021-01-01 00:00:00.417 +0530' else null end as end_date from product_master) a;")
where productid>(select COALESCE(max(productid),0) from star_schema.dim_product)
"""
output6=myProject.query(sql=SQL_CODE6)

pd_gbq.to_gbq(output6, 'star_schema.dim_product', project_id='access-2-352609', if_exists='append')



# In[ ]:




