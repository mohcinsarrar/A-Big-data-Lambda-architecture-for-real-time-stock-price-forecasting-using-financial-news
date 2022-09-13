from kafka import KafkaProducer
import requests
import sys
import configparser
from json import dumps
from time import sleep
from pprint import pprint
from datetime import datetime
import sys
config = configparser.ConfigParser()
config.read('config.ini')

KAFKA_SERVER = 'kafka'
KAFKA_PORT = '9092'
KAFKA_TOPIC = 'nytAPI'

api_key = "4TMP53lAH2ZxQc9ITpvKaaoIUe8KB6Xb"

query = "apple"

last_id = ""

producer = None
try:
    producer = KafkaProducer(bootstrap_servers=[''+KAFKA_SERVER+':'+KAFKA_PORT+''], api_version=(0, 10),value_serializer=lambda x: dumps(x).encode('utf-8'))
except Exception as ex:
    print('Exception while connecting Kafka', producer)
    sys.exit(1)

while True:
    query_date = datetime.now().strftime("%Y%m%d")
    url = f'https://api.nytimes.com/svc/search/v2/articlesearch.json?q={query}&begin_date={query_date}&end_date={query_date}&fq=organizations.contains:("Apple" "apple" "AAPL")&fl=_id,abstract,lead_paragraph&api-key={api_key}'
    response = requests.get(url).json()
    print(response)
    if 'fault' in response:
        sleep(100)
        continue
    status = response['status']
    if status != 'OK':
        continue
    body = response['response']['docs']
    if not body:
        continue

    article_text = response[0]['abstract'] +' '+ response[0]['lead_paragraph']
    article_id = response[0]['_id']
    if article_id != last_id:
        print(article_text)
        last_id = article_id
        #producer.send(KAFKA_TOPIC, {'id':article_id,'date':datetime.now().strftime("%Y-%m-%d"),'text':article_text})
        sleep(4)

