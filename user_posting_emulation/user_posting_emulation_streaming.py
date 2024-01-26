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

    Similar structure to the user_posting_emulation.py

    The payload is created for the stream data to be sent to AWS MSK console

    Sends a PUT request to the API for the stream data to be sent the stream


    '''

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_streaming_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_streaming_selected_row = connection.execute(pin_streaming_string)
            
            for row in pin_streaming_selected_row:
                pin_streaming_result = dict(row._mapping)

            geo_streaming_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_streaming_selected_row = connection.execute(geo_streaming_string)
            
            for row in geo_streaming_selected_row:
                geo_streaming_result = dict(row._mapping)

            user_streaming_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_streaming_selected_row = connection.execute(user_streaming_string)
            
            for row in user_streaming_selected_row:
                user_streaming_result = dict(row._mapping)

            pin_streaming_url = "https://41mrms02f1.execute-api.us-east-1.amazonaws.com/prod2/streams/streaming-0e0816526d11-pin/record"

            pin_streaming_payload = json.dumps({
                "StreamName": "streaming-0e0816526d11-pin",
                "Data" : {
                    "index": pin_streaming_result["index"], "unique_id": pin_streaming_result["unique_id"], "title": pin_streaming_result["title"], "description": pin_streaming_result["description"], "poster_name": pin_streaming_result["poster_name"], "follower_count": pin_streaming_result["follower_count"], "tag_list": pin_streaming_result["tag_list"], "is_image_or_video": pin_streaming_result["is_image_or_video"], "image_src": pin_streaming_result["image_src"], "downloaded": pin_streaming_result["downloaded"], "save_location": pin_streaming_result["save_location"], "category": pin_streaming_result["category"]
                },
                "PartitionKey" : "pin_streaming_partition"
            })

            geo_streaming_url = "https://41mrms02f1.execute-api.us-east-1.amazonaws.com/prod2/streams/streaming-0e0816526d11-geo/record"

            geo_streaming_payload = json.dumps({
                "StreamName": "streaming-0e0816526d11-geo",
                "Data" : {
                    "ind": geo_streaming_result["ind"], "timestamp": str(geo_streaming_result["timestamp" ]), "latitude": geo_streaming_result["latitude"], "longitude": geo_streaming_result["longitude"], "country": geo_streaming_result["country"]
                },
                "PartitionKey" : "geo_streaming_partition"
            })

            user_streaming_url = "https://41mrms02f1.execute-api.us-east-1.amazonaws.com/prod2/streams/streaming-0e0816526d11-user/record"

            user_streaming_payload = json.dumps({
                "StreamName": "streaming-0e0816526d11-user",
                "Data" : {
                    "ind": user_streaming_result["ind"], "first_name": user_streaming_result["first_name"], "last_name": user_streaming_result["last_name"], "age": user_streaming_result["age"], "date_joined": str(user_streaming_result["date_joined"])
                },
                "PartitionKey" : "user_streaming_partition"
            })

            headers = {'Content-Type': 'application/json'}
            response = requests.request("PUT", pin_streaming_url, headers=headers, data=pin_streaming_payload)
            response = requests.request("PUT", geo_streaming_url, headers=headers, data=geo_streaming_payload)            
            response = requests.request("PUT", user_streaming_url, headers=headers, data=user_streaming_payload)
            print(response.status_code)
            
            # print(pin_streaming_result)
            # print(geo_streaming_result)
            # print(user_streaming_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


