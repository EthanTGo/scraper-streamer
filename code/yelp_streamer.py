import numpy as np
import pandas as pd
import requests
from YelpAPIConfig import get_my_key
import time
from kafka import KafkaProducer

API_KEY = get_my_key
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
HEADERS = {'Authorization': 'bearer %s' %API_KEY}

topic_name = 'Yelp Stream'


PARAMETERS = {
    'term':'restaurant',
    'location':'Boston',
    'radius': 1000
}


producer = KafkaProducer(bootstrap_servers='localhost:9092')

while(True):
    response = requests.get(url = ENDPOINT, params = PARAMETERS, headers = HEADERS)
    # convert JSON String to Dictionary
    business_data = response.json()
    print('hello')
    for biz in business_data['businesses']:
        print(f"sending this data {biz} to topic {topic_name}")
        producer.send(topic_name, biz)
    time.sleep(1.0)