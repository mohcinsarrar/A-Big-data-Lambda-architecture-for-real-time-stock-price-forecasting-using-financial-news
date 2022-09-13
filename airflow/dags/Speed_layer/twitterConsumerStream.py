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
    .appName("twitter Stream Consumer")\
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
sdfTweet = dsraw.select(from_json(col("value"), tweet_schema).alias("data")).select("data.*")
print("data loaded")
# create spark nlp pipeline for FinBERT model
document_assembler = DocumentAssembler()\
    .setInputCol('Text')\
    .setOutputCol('document')

tokenizer = Tokenizer() \
    .setInputCols(['document']) \
    .setOutputCol('token')

sequenceClassifier = BertForSequenceClassification \
      .pretrained("bert_sequence_classifier_finbert", "en") \
      .setInputCols(['token', 'document']) \
      .setOutputCol('class') \
      .setCaseSensitive(True) \
      .setMaxSentenceLength(512)

pipeline = Pipeline(stages=[
    document_assembler,
    tokenizer,
    sequenceClassifier
])

# apply FinBER model to RDDs
result = pipeline.fit(sdfTweet.select('Date','Text')).transform(sdfTweet.select('Date','Text'))

# parse the class the model output to positive, negative and neutre percentage

result = result\
        .withColumn('Open',lit(0))\
        .withColumn('High',lit(0))\
        .withColumn('Low',lit(0))\
        .withColumn('Close',lit(0))\
        .withColumn('Volume',lit(0))\
        .withColumn('Nbr_article',lit(1))\
        .withColumn('Positive',map_values(col('class')[0]['metadata'])[1].cast('double'))\
        .withColumn('Negative',map_values(col('class')[0]['metadata'])[2].cast('double'))\
        .withColumn('Neutre',map_values(col('class')[0]['metadata'])[3].cast('double'))

result = result.select(['Date','Nbr_article','Positive','Negative','Neutre'])


# write output to elasticsearch
script = "\
            ctx._source.Open=ctx._source.Open;\
            ctx._source.High=ctx._source.High;\
            ctx._source.Low=ctx._source.Low;\
            ctx._source.Close=ctx._source.Close;\
            ctx._source.Volume=ctx._source.Volume;\
            ctx._source.Nbr_article+=params.Nbr_article;\
            ctx._source.Positive+=params.Positive;\
            ctx._source.Positive=ctx._source.Positive/2;\
            ctx._source.Negative+=params.Negative;\
            ctx._source.Negative=ctx._source.Negative/2;\
            ctx._source.Neutre+=params.Neutre;\
            ctx._source.Neutre=ctx._source.Neutre/2;\
        "
params = "Nbr_article:Nbr_article,Positive:Positive,Negative:Negative,Neutre:Neutre"

query = result\
    .writeStream\
    .format("org.elasticsearch.spark.sql")\
    .option("checkpointLocation", "checkpoint_twitterStream")\
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


