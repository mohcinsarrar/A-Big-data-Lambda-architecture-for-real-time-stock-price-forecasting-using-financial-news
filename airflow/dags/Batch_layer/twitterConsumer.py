import sys
from datetime import datetime
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *

from pyspark.ml import Pipeline
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.functions import *

# create spark session
spark = SparkSession.builder \
    .appName("twitter Consumer")\
    .master("local[*]")\
    .config("spark.driver.memory", "16G") \
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryoserializer.buffer.max", "2000m") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.4")\
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

  
# read data from kafka
dsraw = spark\
  .readStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "kafka:9092")\
  .option("subscribe", "twitterAPI")\
  .option("failOnDataLoss","false")\
  .load()\
  .selectExpr("CAST(value AS STRING)")


# parse kafka data to spark dataframe
tweet_schema = StructType([
    StructField("Date", StringType()), 
    StructField("Text", StringType())
    ])


# parse json from kafka using schema
result = dsraw.select(from_json(col("value"), tweet_schema).alias("data")).select("data.*")

print("data loaded")
# write output to ES

query = result\
    .writeStream\
    .format("org.elasticsearch.spark.sql")\
    .option("checkpointLocation", "checkpoint")\
    .option("es.nodes","http://elasticsearch")\
    .option("es.port","9200")\
    .option("es.nodes.wan.only","true")\
    .option("es.resource", "news_master")\
    .option("es.net.http.auth.user","elastic")\
    .option("es.net.http.auth.pass","elastic")\
    .outputMode("append")\
    .trigger(processingTime="30 seconds")\
    .start()

query.awaitTermination()


