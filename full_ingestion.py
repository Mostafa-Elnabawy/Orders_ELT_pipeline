#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymysql
import csv
import pandas as pd
import boto3
import configparser


# In[2]:


parser = configparser.ConfigParser()
parser.read("pipeline.conf")


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


cur.execute("select * from Customers_scd")


# In[7]:


customer_df = pd.DataFrame(cur.fetchall() , columns = ['CustomerId' , 'CustomerName' , 'CustomerCountry' , 'ValidFrom' , 'Expired'])


# In[8]:


access_key = parser.get("aws_boto_credentials",
"access_key")
secret_key = parser.get("aws_boto_credentials",
"secret_key")
bucket_name = parser.get("aws_boto_credentials",
"bucket_name")
file_loc = parser.get("aws_boto_credentials",
"customer_file_location")


# In[9]:


s3 = boto3.client('s3',
aws_access_key_id=access_key,
aws_secret_access_key=secret_key)


# In[10]:


customer_df.to_csv(
    f"s3://{bucket_name}/{file_loc}",
    index=False,
    storage_options={
        "key": access_key,
        "secret": secret_key
    },
)


# In[11]:


conn.close()
cur.close()


# In[ ]:




