#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymysql
import configparser 
import pandas as pd
import boto3


# In[2]:


parser = configparser.ConfigParser()
parser.read('pipeline.conf')


# In[3]:


hostname = parser.get("mysql_config" , "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config" , "username")
dbname = parser.get("mysql_config" , "database")
password = parser.get("mysql_config" , "password")


# In[4]:


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


# In[5]:


cur = conn.cursor()


# In[6]:


cur.execute( 
    '''
    INSERT INTO Orders
values(4,'Shipped','2020-07-11',101,57.45),
(5,'Shipped','2020-07-12',102,135.99),
(4,'Shipped','2020-07-11',101,57.45),
(5,'Shipped','2020-07-12',102,135.99),
(6,'Shipped','2020-07-12',100,43.00);
    '''
)


# In[7]:


conn.commit()
conn.close()
cur.close()


# In[ ]:




