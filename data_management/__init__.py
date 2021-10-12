import logging
import pandas as pd
import azure.functions as func

import os
import sys

parent_dir=os.getcwd()
sys.path.insert(0,parent_dir)

# from TATA_assignment.database_operations import database_operations
import database_operations
import json


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    headers={}
    headers["Content-type"]="application/json"
    headers['Access-Control-Allow-Origin']="*"
    headers['Access-Control-Allow-Headers']="Authorization"
    
    try:
        file_obj = open("Mongo_database_config.txt", "r+")    
        file_data = file_obj.read()
        mongo_config = file_data.split("\n")
        
        if len(mongo_config)>1:    
            url=mongo_config[1]
            database_name=mongo_config[3]
            collection_name=mongo_config[5]
        else:
            message="*** Please enter MongoDB connection string in Mongo_database_config.txt file and save...*** "
            response_data={"message":message}
            print(message)
            logging.info(message)
            return func.HttpResponse(json.dumps(response_data),status_code=200)
    
        obj=database_operations.database_operations(url)
        obj.create_connection()
        obj.connect_database(database_name,collection_name)
    
        if req.method=="GET":
            if req.params.get('feedback'):
                database_data=obj.find_data()    
            if database_data:
                response_data=database_data
                message="data fetched from database sucessfully"
            else:
                message="No data in database"
            response_data={"message":"No data in database"}
            logging.info(message)
            return func.HttpResponse(json.dumps(response_data),status_code=200)

    
        elif req.method=="POST":
            if req.params.get('feedback'):
                if req.params.get('get_results'):
                    req_data = req.get_json()

                    filters=req_data.get("filters")
                    query=filters
                    projections=req_data.get("projections")
                    database_data=obj.find_data(query,projections) 
                    if database_data:
                        response_data=database_data
                        message="data fetched from database sucessfully"
                    else:
                        message="No data in database"
             

                    logging.info(message)
                    return func.HttpResponse(json.dumps(response_data),status_code=200)


                
                if req.params.get('bulk_insert'):
                    status=False
                    if req.params.get('csv_link'):
                        collection_name="csv_links"
                        obj.connect_database(database_name,collection_name)
                        csv_link=req.params.get('csv_link')
                        bulk_insert_link_data={"csv_link":csv_link,"processing_status":"not_processed"}
                        # csv_data = pd.read_csv(csv_path)
                        status=obj.insert_data(bulk_insert_link_data)                       
                                         
                    else:
                        insert_data = req.get_json()                        
                    #bulk insert 
                        status=obj.insert_many(insert_data)
                    message="data inserted from database sucessfully"   
                    
                if req.params.get('insert_one'):
                    req_data = req.get_json()
                    status=False
                    if req_data:                        
                        for insert_data in req_data:
                          # to insert one document                     
                            status=obj.insert_data(insert_data) 
                            message="data inserted from database sucessfully"
                            break
                logging.info(message)
                response_data={"status":"insertion Successful data will get reflect shortly"}
                logging.info("Python HTTP trigger function processed a request.......................")
        
                return func.HttpResponse(json.dumps(response_data),status_code=200)            
            else:
                response_data={"message":"request method not defined, Please specify appropriate parameters"}
                return func.HttpResponse(json.dumps(response_data),status_code=200)

    
    except Exception as e:
        status_code=501
        response_data={"error":str(e)}
        logging.exception(str(e))
        return func.HttpResponse(json.dumps(response_data),status_code=200)

    
