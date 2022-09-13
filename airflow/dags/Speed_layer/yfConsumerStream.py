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
    .appName("yf stream Consumer")\
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
  .option("subscribe", "yfAPI")\
  .option("failOnDataLoss","false")\
  .load()\
  .selectExpr("CAST(value AS STRING)")

# parse kafka data to spark dataframe
stock_schema = StructType([
  StructField("Date", StringType()), 
  StructField("Open", StringType()),
  StructField("High", StringType()),
  StructField("Low", StringType()),
  StructField("Close", StringType()),
  StructField("Adj Close", StringType()),
  StructField("Volume", StringType()),
  ])

# parse json from kafka using schema
result = dsraw.select(from_json(col("value"), stock_schema).alias("data")).select("data.*")
print("data loaded")
result = result\
        .withColumn('Nbr_article',lit(0))\
        .withColumn('Positive',lit(0.33))\
        .withColumn('Negative',lit(0.33))\
        .withColumn('Neutre',lit(0.33))


# write output to elasticsearch
script = "\
            ctx._source.Open=params.Open;\
            ctx._source.High=params.High;\
            ctx._source.Low=params.Low;\
            ctx._source.Close=params.Close;\
            ctx._source.Volume=params.Volume;\
            ctx._source.Nbr_article=ctx._source.Nbr_article;\
            ctx._source.Positive=ctx._source.Positive;\
            ctx._source.Negative=ctx._source.Negative;\
            ctx._source.Neutre=ctx._source.Neutre;\
        "
params = "Open:Open,High:High,Low:Low,Close:Close,Volume:Volume"

query = result\
    .writeStream\
    .format("org.elasticsearch.spark.sql")\
    .option("checkpointLocation", "checkpoint_yfStream")\
    .option("es.nodes","http://elasticsearch")\
    .option("es.port","9200")\
    .option("es.nodes.wan.only","true")\
    .option("es.mapping.id","Date")\
    .option("es.write.operation","upsert")\
    .option("es.update.script.inline",script)\
    .option("es.update.script.lang","painless")\
    .option("es.update.script.params",params)\
    .option("es.resource", "speed_view")\
    .option("es.net.http.auth.user","elastic")\
    .option("es.net.http.auth.pass","elastic")\
    .outputMode("append")\
    .trigger(processingTime="30 seconds")\
    .start()

query.awaitTermination()



