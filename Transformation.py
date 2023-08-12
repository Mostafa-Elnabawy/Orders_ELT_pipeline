#!/usr/bin/env python
# coding: utf-8

# In[19]:


import pymysql
import pandas as pd
import boto3
import configparser


# In[20]:


parser = configparser.ConfigParser()
parser.read("pipeline.conf")


# In[21]:


hostname = parser.get("mysql_config" , "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config" , "username")
dbname = parser.get("mysql_config" , "database")
password = parser.get("mysql_config" , "password")


# In[22]:


# if connection failed try to modify inboud rules in EC2 
conn = pymysql.connect(host= hostname , 
                       user = username,
                       password=password , 
                       db=dbname,
                       port=int(port))
if conn is None: 
    print("Error connecting to MySQL")
else : 
    print("connection established")


# ## Removing duplicates from orders table

# In[23]:


cur = conn.cursor()


# In[24]:


cur.execute("""
select * from Orders
""")
cur.fetchall()


# In[25]:


cur.execute("""
create table order_dup as 
select OrderId , OrderStatus
 ,OrderDate ,  CustomerId , OrderTotal , 
 ROW_NUMBER() OVER(PARTITION BY OrderId , OrderStatus
 ,OrderDate ,  CustomerId , OrderTotal) as dup_count from Orders;
 """)
cur.fetchall()


# In[26]:


cur.execute("""
TRUNCATE TABLE Orders""")


# In[27]:


cur.execute("""
insert into Orders
select OrderId , OrderStatus
 ,OrderDate ,  CustomerId , OrderTotal from order_dup
 where dup_count = 1
 """)
cur.execute("""
DROP TABLE IF EXISTS order_dup
"""
           )


# In[28]:


cur.execute("""
select * from Orders
""")
cur.fetchall()


# ## Using SCD in table customer_scd represented in columns valid_from and expired to build a model that joins the orders and customer_scd tables

# In[29]:


cur.execute("""
select * from Customers_scd
""")
cur.fetchall()


# ## Here we can see that customer Jane changed her location from 'USA' to 'UK' as of '2020-06-20' so we want to track that change 

# In[32]:


cur.execute("""
create table order_customer as 
SELECT 
    o.OrderId 
    ,o.OrderDate
    ,c.CustomerName
    ,c.CustomerCountry
FROM Orders as o
INNER JOIN Customers_scd as c
    on o.CustomerId = c.CustomerId
    and o.OrderDate between c.ValidFrom and Expired;
""")
cur.execute("""
SELECT * FROM order_customer
""" )
cur.fetchall()


# ## As you can see in order id 1 the country related to customer Jane was USA as of the time she purchased that order however,
# ## in order id 6 we can see that country is now UK as country record changed.

# In[33]:


conn.close()
cur.close()


# In[ ]:




