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


cur.execute("drop table if exists Orders")
cur.execute("drop table if exists Customers_scd")
conn.commit()


# In[7]:


cur.execute(
"""
CREATE TABLE Orders (
 OrderId int,
 OrderStatus varchar(30),
 OrderDate timestamp,
 CustomerId int,
 OrderTotal numeric
);

"""
)


# In[8]:


cur.execute(
"""

CREATE TABLE Customers_scd
(
 CustomerId int,
 CustomerName varchar(20),
 CustomerCountry varchar(10),
 ValidFrom timestamp,
 Expired timestamp
);

"""
           )


# In[9]:


cur.execute( 
    '''
    INSERT INTO Orders
 VALUES(1,'Shipped','2020-06-09',100,50.05),
 (1,'Backordered', '2020-06-01 12:00:00' , 100 , 20.00),
 (2,'Shipped', '2020-06-09 12:00:25' , 101 , 30.00),
 (2,'Shipped', '2020-06-09 12:00:25' , 101 , 30.00),
 (3,'Shipped', '2020-06-09 11:50:00', 101 , 50.55);
    '''
)


# In[10]:


cur.execute( 
    '''
    INSERT INTO Customers_scd
 VALUES (100,'Jane','USA' , '2019-05-01 7:01:10','2020-06-20 8:15:34'),
 (100,'Jane','UK','2020-06-20 8:15:34',
 '2022-12-31 00:00:00'),
 (101,'Bob','UK', '2020-05-10 8:15:34',
 '2022-12-31 00:00:00'),
(102,'Miles','UK' , '2020-05-20 8:15:34',
 '2022-12-31 00:00:00');   '''
)


# In[11]:


conn.commit()
cur.close()
conn.close()


# In[ ]:




