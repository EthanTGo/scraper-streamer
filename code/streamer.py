'''
This python file will contain the file to begin our stream services such as Twitter and other Streaming Service
'''
import numpy as np
import pandas as pd
import datetime
import time

# Install the following with pip if haven't
import tweepy

# config.py that contains the consumer key and access token
from config import *
from kafka import KafkaProducer

# Creates the Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092') #Same port as your Kafka server

#Name of Kafka Topic
topic_name = "Twitter_Stream"

class twitterAuth():
    """SET UP TWITTER AUTHENTHICATION"""

    def authenthicateTwitterApp(self):
        auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
        auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
        return auth


class TwitterStreamer():

    """SET UP STREAMER"""
    def __init__(self):
        self.twitterAuth = twitterAuth()
    
    def stream_tweets(self):
        while True:
            stream = ListernerTS(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET,TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
            stream.filter(track=['Apple'], stall_warnings=True,languages=['en'])

class ListernerTS(tweepy.Stream):

    def on_data(self, raw_data):
        print(raw_data)
        producer.send(topic_name, raw_data)
        return True 


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()