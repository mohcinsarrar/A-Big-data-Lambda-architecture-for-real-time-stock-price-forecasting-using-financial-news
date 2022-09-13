from kafka import KafkaProducer
import tweepy
import configparser
import sys
import re
from json import dumps
from time import sleep
from datetime import datetime, timedelta


KAFKA_SERVER = 'kafka'
KAFKA_PORT = '9092'
KAFKA_TOPIC = 'twitterAPI'

Bearer_Token = 'AAAAAAAAAAAAAAAAAAAAAFDvcQEAAAAAbUWmbxxXABti9ExmV2AK3VRf8R4%3Dx3oD5lkN1lYaHMLhGmT7UW47IyygAns5NPbq3YGLVEly3XFF6I'

producer = None
try:
    producer = KafkaProducer(bootstrap_servers=[''+KAFKA_SERVER+':'+KAFKA_PORT+''], api_version=(0, 10),value_serializer=lambda x: dumps(x).encode('utf-8'))
except Exception as ex:
    print('Exception while connecting Kafka', producer)
    sys.exit(1)

class MyStreamListener(tweepy.StreamingClient):

    def on_tweet(self, tweet):
        if 'RT @' not in tweet.text and ('AAPL' in tweet.text or 'apple' in tweet.text or 'Apple' in tweet.text or 'APPLE' in tweet.text):
            processed_tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",tweet.text).split())
            processed_tweet.replace(';',' ')
            #dateminusday = tweet.created_at - timedelta(days=0)
            #date = dateminusday.strftime("%Y-%m-%d")
            date = tweet.created_at.strftime("%Y-%m-%d")
            message = {'Date':date, 'Text':processed_tweet}
            print(message)
            producer.send(KAFKA_TOPIC, message)
            sleep(1)
        
    
    def on_request_error(self, status_code):
        print('Encountered error with status code: '+str(status_code), file=sys.stderr)
        return True # Don't kill the stream

    def on_errors(self, status_code):
        print('Encountered error with status code: '+str(status_code), file=sys.stderr)
        return True # Don't kill the stream
    def on_connection_error():
        print('Encountered connection error', file=sys.stderr)
        return True # Don't kill the stream
    


myStreamListener = MyStreamListener(Bearer_Token)
myStreamListener.add_rules(add=tweepy.StreamRule('lang:en AND -is:retweet'),dry_run=True)

myStreamListener.filter(tweet_fields=['created_at'])

