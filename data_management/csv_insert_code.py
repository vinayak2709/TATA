# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 10:37:20 2021

@author: swd7788
"""

import pandas as pd
from pymongo import MongoClient


from TATA_assignment.database_operations import database_operations

csv_path=r"C:\Users\swd7788\Downloads\CGB_-_Consumer_Complaints_Data__2017_ (1).csv"

csv_Data=pd.read_csv(csv_path)

non_nan_csv_Data=csv_Data.fillna("")

dictionary_data=non_nan_csv_Data.to_dict('records')
insert_data=dictionary_data[0]

insert_data2=dictionary_data[:5]


url='mongodb://localhost:27017/'


database_name="telecom_customers"
collection_name="feedback"

obj=database_operations(url)
obj.create_connection()
obj.connect_database(database_name,collection_name)

# to insert one document 
obj.insert_data(insert_data)  

#bulk insert 
obj.insert_many(insert_data2)   

database_data=obj.find_data()

 

