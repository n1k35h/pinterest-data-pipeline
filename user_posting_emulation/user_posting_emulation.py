import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text

random.seed(100)

class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

def run_infinite_post_data_loop():

    '''
    What should happen
    ------------------

    Runs a loop of post data from the table
    
    Gets a random data from the corresponding tables (pinterest, geolocation and user)

    Invoke URL - when the code is running it sends the Data to the S3 Bucket where each of the URL stores a json file
    to their corresponding URL (pin_url, geo_url and user_url)

    Code will continue to run until it gets interrupted

    Attribute:
    ----------
    pin_result into json file
        pin data gets a random row from the pinterest_data table
    geo_result into json file
        geo data gets a random row from the geolocation_data table
    user_result into json file
        user data gets a random row from the user_data table

    Parameter:
    ----------
    None

    Return:
    -------
    None

    '''

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)

            pin_url = "https://41mrms02f1.execute-api.us-east-1.amazonaws.com/prod/topics/0e0816526d11.pin"

            pin_payload = json.dumps({
                "records": [
                    {
                    #Data should be send as pairs of column_name:value, with different columns separated by commas       
                    "value": {"index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"], "description": pin_result["description"], "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"], "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"], "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], "save_location": pin_result["save_location"], "category": pin_result["category"]}
                    }
                ]
            })

            geo_url = "https://41mrms02f1.execute-api.us-east-1.amazonaws.com/prod/topics/0e0816526d11.geo"

            geo_payload = json.dumps({
                "records": [
                    {
                    #Data should be send as pairs of column_name:value, with different columns separated by commas       
                    "value": {"ind": geo_result["ind"], "timestamp": str(geo_result["timestamp" ]), "latitude": geo_result["latitude"], "longitude": geo_result["longitude"], "country": geo_result["country"]}
                    }
                ]
            })
            
            user_url = "https://41mrms02f1.execute-api.us-east-1.amazonaws.com/prod/topics/0e0816526d11.user"
            
            #To send JSON messages you need to follow this structure
            user_payload = json.dumps({
                "records": [
                    {
                    #Data should be send as pairs of column_name:value, with different columns separated by commas       
                    "value": {"ind": user_result["ind"], "first_name": user_result["first_name"], "last_name": user_result["last_name"], "age": user_result["age"], "date_joined": str(user_result["date_joined"])},
                    }
                ]
            })    

            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            response = requests.request("POST", pin_url, headers=headers, data=pin_payload)
            response = requests.request("POST", geo_url, headers=headers, data=geo_payload)
            response = requests.request("POST", user_url, headers=headers, data=user_payload)
            print(response.status_code)
            
            # print(pin_result)
            # print(geo_result)
            # print(user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


