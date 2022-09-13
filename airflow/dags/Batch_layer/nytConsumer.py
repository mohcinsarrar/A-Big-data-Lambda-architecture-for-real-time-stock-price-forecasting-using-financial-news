import sys

from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.sql.streaming import DataStreamWriter

#import os
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-sql-kafka-0-10_2.11-2.4.1.jar'


# create spark session
spark = SparkSession.builder \
    .appName("nyt Consumer")\
    .master("local[*]")\
    .config("spark.driver.memory", "16G") \
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryoserializer.buffer.max", "2000m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

dsraw = spark\
  .readStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "kafka:9092")\
  .option("subscribe", "nytAPI")\
  .option("failOnDataLoss","false")\
  .load()\
  .selectExpr("CAST(value AS STRING)")

# parse kafka data to spark dataframe
news_schema = StructType([
  StructField("Id", StringType()), 
  StructField("Date", StringType()),
  StructField("text", StringType()),
  ])

# parse json from kafka using schema
result = dsraw.select(from_json(col("value"), news_schema).alias("data")).select("data.*")


# push stock data to elasticsearch
'''
query = result\
    .writeStream\
    .format("org.elasticsearch.spark.sql")\
    .option("checkpointLocation", "checkpoint")\
    .option("es.nodes","http://192.168.96.5")\
    .option("es.port","9200")\
    .option("es.nodes.wan.only","true")\
    .option("es.resource", "stock_batch_view")\
    .option("es.mapping.id","Date")\
    .option("es.net.http.auth.user","elastic")\
    .option("es.net.http.auth.pass","elastic")\
    .outputMode("append")\
    .trigger(processingTime="30 seconds")\
    .start()

'''
query = result\
    .writeStream\
    .format("console")\
    .outputMode("append")\
    .trigger(processingTime="30 seconds")\
    .start()
query.awaitTermination()



