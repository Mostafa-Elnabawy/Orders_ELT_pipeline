# [title]: 
## Orders_ELT_pipeline
# [description] : 
### This project is a simple execution for the ELT pattern using concepts like : Full and Incremental ingestion of data and
### transforming data with SCD
# [Technologies]
### AWS RDS,  AWS S3, python (e.g pandas, boto3, configparser, pymysql)
# [Content] : 
### Abstract.conf: a configuration file where you can add your access credentials to AWS services
### requirements.txt : a file with all packages you need to run the project
### first_fill.py : a python script to create tables and insert initial values on your RDS instance
### sec_fill.py : a python script to imitate data change in your database
### full_ingestion.py : a python script that extracts and loads your data from RDS and save it to S3 in full mode.
### Incremental_ingestion.py : a python script to extract and load your data from RDS and save it to S3 in incremental mode.
### transformation.py : a python script to apply basic transformations on the data and create a data model applying SCD concept.
# [How to run] :
-- first create a virtual environment and install all the packages in the requirements.txt file in it using pip
-- create a RDS and S3 instances and fill in the configuration file with information needed
-- run the first_fill.py to fill create and fill your tables with data you can ignore that step if you have data already inserted in your database
-- run full_ingestion.py or incremental_ingestion.py depending on whether you need to load data fully or incrementally
-- finally run transformation.py to apply transformations and create the final data model
# [CREDITS] :
## data pipeline pocket reference book from O'Reilly chapters 4,5 and 6. 
