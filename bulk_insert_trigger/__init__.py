import datetime
import logging
import os
import sys
import pandas as pd

parent_dir=os.getcwd()
sys.path.insert(0,parent_dir)
import azure.functions as func
import database_operations

def data_validation(insert_data):
    final_insert_data=[]
    
    
    if type(insert_data)==list:
        pass
    else:
        insert_data=[insert_data]
        
    for k in insert_data:
        # _id should not be therer in data
        if k.get("_id"):
             k.pop("_id")
             
        # Ticket ID must be in the data since its going to be Primary key
        if k.get('Ticket ID'):
             if k.get('Ticket ID') not in ["",None]:
                 
                 # location must be valid
                 
                if k.get('Location (Center point of the Zip Code)'):
                     coord_array=k.get('Location (Center point of the Zip Code)').split("\n")
                     if len(coord_array)>1:
                         raw_coordinates=coord_array[1] 
                         raw_coordinates2=raw_coordinates.replace("(","")
                         raw_coordinates3=raw_coordinates2.replace(")","")
                         coordinate_array=raw_coordinates3.split(",")
                         if len(coordinate_array)>1:
                             lat=float(coordinate_array[0])                         
                             lon=float(coordinate_array[1])
                             message=[]
                             if lon>80 or lon <-180:
                                 message.append("longitude not valid")
                             if lat>90 or lat<-90:
                                 message.append("lattitude not valid")
                             k["error_message"] = message
                             k["logitude"]=lon
                             k["latitude"]=lat
                             final_insert_data.append(k)


              
    return final_insert_data



def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    try:
        file_obj = open("Mongo_database_config.txt", "r+")    
        file_data = file_obj.read()
        mongo_config = file_data.split("\n")
        
        if len(mongo_config)>1:    
            url=mongo_config[1]
            database_name=mongo_config[3]
            collection_name_text=mongo_config[5]
        else:
            message="*** Please enter MongoDB connection string in Mongo_database_config.txt file and save...*** "
            response_data={"message":message}
            print(message)
            logging.info(message)
        
            obj=database_operations.database_operations(url)
            obj.create_connection()
            collection_name="csv_links"
    
            obj.connect_database(database_name,collection_name)
        
            database_data=obj.find_data({'processing_status':'not_processed'})
            if database_data:
                
                csv_link=database_data[0].get("csv_link")        
                csv_data=pd.read_csv(csv_link)
                non_nan_csv_data=csv_data.fillna("")        
                insert_data=non_nan_csv_data.to_dict('records') 
                insert_data2=insert_data
                insert_data=insert_data2[:9]
                insert_data=data_validation(insert_data)
                obj.connect_database(database_name,collection_name_text)
                status=obj.insert_many(insert_data)

    except Exception as e:
        response_data={"error":str(e)}
        logging.exception(str(e))
      

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
