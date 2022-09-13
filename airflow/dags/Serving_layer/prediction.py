import pickle
import pmdarima
from sparknlp.base import *
from sparknlp.annotator import *

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# model initialization
features_list = [
                'Open',
                'High',
                'Low',
                'Volume',
                'Nbr_article',
                'Positive',
                'Negative',
                'Neutre'
            ]

# create spark session
spark = SparkSession.builder \
    .appName("prediction")\
    .master("local[*]")\
    .config("spark.driver.memory", "16G") \
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryoserializer.buffer.max", "2000m") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

  
# get all data from speed_view
query_date = '{ "query": {"range": { "Date": { "gte": "now/d", "lt": "now+1d/d" } } } }'


myreader = spark.read.format("org.elasticsearch.spark.sql") \
  .option("es.nodes", "http://elasticsearch") \
  .option("es.port", "9200") \
  .option("es.net.http.auth.user", "elastic") \
  .option("es.net.http.auth.pass", "elastic") \
  .option("es.query", query_date)

speed = myreader.load("speed_view")


# get 10 days data from batch_view
query_date = '{ "query": {"range": { "Date": { "gte": "now-8d/d", "lt": "now-2d/d" } } } }'

myreader = spark.read.format("org.elasticsearch.spark.sql") \
  .option("es.nodes", "http://elasticsearch") \
  .option("es.port", "9200") \
  .option("es.net.http.auth.user", "elastic") \
  .option("es.net.http.auth.pass", "elastic") \
  .option("es.query", query_date)

batch = myreader.load("batch_view")


# load SARIMAX model
with open("/opt/airflow/dags/models/sarimax.pkl", 'rb') as pkl:
    sarimax = pickle.load(pkl)


# select features
speed.show()
pandasDF = speed.select(features_list).toPandas()

# apply SARIMAX
predictions_sv  = sarimax.predict(n_periods = 1,X = pandasDF)

# create the serving view from speed view + the predicted close price
serving = speed.withColumn("Close", lit(predictions_sv[0]))
serving.show()


# write data to batch view

serving.write\
    .format("org.elasticsearch.spark.sql")\
    .option("es.nodes","http://elasticsearch")\
    .option("es.port","9200")\
    .option("es.nodes.wan.only","true")\
    .option("es.mapping.id","Date")\
    .option("es.mapping.date.rich","false")\
    .option("es.resource", "serving_view")\
    .option("es.net.http.auth.user","elastic")\
    .option("es.net.http.auth.pass","elastic")\
    .mode("append")\
    .save()
