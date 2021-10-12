# -*- coding: utf-8 -*-
"""
Created on Thu Jul 22 00:10:07 2021

@author: swd7788
"""

from pymongo import MongoClient
import datetime

# url='mongodb://localhost:27017/'

class database_operations:
    def __init__(self,url):
        
        self.url=url

    def create_connection(self):
        self.client = MongoClient(self.url)
        return self.client

    def connect_database(self,database_name,collection_name):       

        self.mydb = self.client [database_name]
        self.my_collection = self.mydb[collection_name]
        return self.mydb
    
    def find_data(self,query={},projections={}):
        if query=={}:
            data=list(self.my_collection.find())
        else:
            if projections=={}:
                data=list(self.my_collection.find(query))
                
            else:
                data=list(self.my_collection.find(query,projections))

        return data

    def insert_data(self,insert_data):
        record_id2 = self.my_collection.insert_one(insert_data)
        
            
    def insert_many(self,insert_data):
        
        for k in insert_data:
            if k.get("_id"):
                k.pop("_id")

        record_id2 = self.my_collection.insert_many(insert_data)



# a=database_operations(url)
# a.create_connection()
# a.connect_database()
# print(a.find_data())