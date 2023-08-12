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


access_key = parser.get("aws_boto_credentials",
"access_key")
secret_key = parser.get("aws_boto_credentials",
"secret_key")
bucket_name = parser.get("aws_boto_credentials",
"bucket_name")
file_loc = parser.get("aws_boto_credentials",
"order_file_location")


# In[7]:


s3 = boto3.client('s3',
aws_access_key_id=access_key,
aws_secret_access_key=secret_key)


# In[8]:


try:
    response = s3.get_object(Bucket=bucket_name, Key=file_loc)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        orders_df = pd.read_csv(response.get("Body"))
        print(orders_df)
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
except Exception as error:
    if type(error).__name__ == 'NoSuchKey':
        orders_df = pd.DataFrame(columns=['OrderId', 'OrderStatus', 'OrderDate' , 'CustomerId' , 'OrderTotal'])


# In[9]:


orders_df


# In[10]:


hostname = parser.get("mysql_config" , "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config" , "username")
dbname = parser.get("mysql_config" , "database")
password = parser.get("mysql_config" , "password")


# In[11]:


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


# In[12]:


if orders_df.empty:
    query = "select * from Orders"
else:
    last_update = max(orders_df.OrderDate)
    query = f"select * from Orders where OrderDate > \'{last_update}\'"


# In[13]:


print(query)


# In[14]:


cur = conn.cursor()


# In[15]:


cur.execute(query)


# In[16]:


results = cur.fetchall()


# In[17]:


orders_full_df = pd.concat([orders_df , pd.DataFrame(results , columns = orders_df.columns)])


# In[21]:


orders_full_df.drop_duplicates(inplace=True)


# In[22]:


orders_full_df.to_csv(
    f"s3://{bucket_name}/{file_loc}",
    index=False,
    storage_options={
        "key": access_key,
        "secret": secret_key
    },
)


# In[23]:


cur.close
conn.close()


# In[ ]:




