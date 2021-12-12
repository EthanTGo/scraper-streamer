from typing import final
from config import *
from pymongo import MongoClient
from kafka import KafkaConsumer
import json


def get_database():

    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    client = MongoClient("mongodb+srv://frozlanes:gARF13LD@cluster0.vdoua.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    # Create the database for our example (we will use the same database throughout the tutorial
    return client['random_data_one']
    
# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":    
    topic_name = 'WordCount'
    # Get the database
    client = MongoClient("mongodb+srv://frozlanes:gARF13LD@cluster0.vdoua.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = client.random

    collection = db.random

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        auto_commit_interval_ms =  5000,
        fetch_max_bytes = 128,
        max_poll_records = 100,
        value_deserializer=lambda x: x
    )

    for msg in consumer:
        print("Inserting....")
        string = bytes.decode(msg.value)
        value = string.split(',')
        try:
            dictionary_val ={"Word":value[0], "Number" :value[1]}
            print(dictionary_val)
            collection.insert_one(dictionary_val)
        except:
            print('skipping')