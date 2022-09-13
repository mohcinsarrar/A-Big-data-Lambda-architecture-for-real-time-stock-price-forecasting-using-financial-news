import yfinance as yf
from datetime import datetime, timedelta
from kafka import KafkaProducer
import sys
import configparser
from json import dumps
from time import sleep


KAFKA_SERVER = 'kafka'
KAFKA_PORT = '9092'
KAFKA_TOPIC = 'yfAPI'


producer = None
try:
    producer = KafkaProducer(bootstrap_servers=[''+KAFKA_SERVER+':'+KAFKA_PORT+''], api_version=(0, 10),value_serializer=lambda x: dumps(x).encode('utf-8'))
except Exception as ex:
    print('Exception while connecting Kafka', producer)
    sys.exit(1)


while True:
    data = yf.download(tickers='AAPL', period='1d').reset_index()
    dateNow = datetime.now().strftime("%Y-%m-%d")
    try:
        data['Date'] = data['Date'].dt.strftime("%Y-%m-%d")
    except:
        continue
    if data['Date'][0]!=dateNow:
        data['Date']=dateNow
    data.drop('Adj Close', axis=1, inplace=True)
    msg = data.iloc[0].to_dict()
    print(msg)
    try:
        producer.send(KAFKA_TOPIC, msg)
    except:
        continue
    sleep(4)

