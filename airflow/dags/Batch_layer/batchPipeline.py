from sparknlp.base import *
from sparknlp.annotator import *

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F


# transformers
import re
import emoji
from datetime import datetime
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol


class CleanNews(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol  : str = None, outputCol : str = None,):
        super(Transformer, self).__init__()
        # set the input keywork arguments
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, 
                  inputCol  : str = None, 
                  outputCol : str = None, 
      ) -> None:
        """
        Function to set the keyword arguemnts
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)


    def cleaning_news(self,article):
        if article is None or len(article)<10:#remove short text
            article = None
        else:
            article = ''.join(c for c in article if c not in emoji.EMOJI_DATA) #Remove Emojis
            article = re.sub(r"(?:\@|http?\://|https?\://|www)\S+", "", article)# remove URLs
            article = article.replace('$','')# remove $ sign
            article = re.sub("@[A-Za-z0-9]+","",article) # remove @ sign
            article = article.replace("#", "").replace("_", " ") #Remove hashtag sign but keep the text
            article = article.strip() #remove leading and trailing spaces 
        
        return article

    def _transform(self, df: DataFrame) -> DataFrame:

        # Get the names of the input and output columns to use
        out_col = self.getOutputCol()
        in_col  = self.getInputCol()

        clear_func_udf = F.udf(self.cleaning_news, StringType())

        df2 = df.withColumn(out_col, clear_func_udf(df[in_col]))

        df2 = df2.dropna()

        return df2


class AggNews(Transformer):

    @keyword_only
    def __init__(self):
        super(Transformer, self).__init__()

    def _transform(self, df: DataFrame) -> DataFrame:

        # select positive, negative and neutre from FinBert output
        result = df\
        .withColumn('Nbr_article',lit(1))\
        .withColumn('Positive',map_values(col('class')[0]['metadata'])[1].cast('double'))\
        .withColumn('Negative',map_values(col('class')[0]['metadata'])[2].cast('double'))\
        .withColumn('Neutre',map_values(col('class')[0]['metadata'])[3].cast('double'))

        result = result.select(['Date','Nbr_article','Positive','Negative','Neutre'])

        # aggregate dataframe by date
        result = result.groupBy("Date").agg(
            sum("Nbr_article").alias("Nbr_article"),
            avg("Positive").alias("Positive"),
            avg("Negative").alias("Negative"),
            avg("Neutre").alias("Neutre"))


        return result

# create spark session
spark = SparkSession.builder \
    .appName("Batch Pipline")\
    .master("local[*]")\
    .config("spark.driver.memory", "16G") \
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryoserializer.buffer.max", "2000m") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.4")\
    .getOrCreate()

# get all data from news_master
myquery = '{ "query": { "match_all": {} }}'

myreader = spark.read.format("org.elasticsearch.spark.sql") \
  .option("es.nodes", "http://elasticsearch") \
  .option("es.port", "9200") \
  .option("es.net.http.auth.user", "elastic") \
  .option("es.net.http.auth.pass", "elastic") \
  .option("es.query", myquery)

news = myreader.load("news_master")

# get all data from stock_master
myreader = spark.read.format("org.elasticsearch.spark.sql") \
  .option("es.nodes", "http://elasticsearch") \
  .option("es.port", "9200") \
  .option("es.net.http.auth.user", "elastic") \
  .option("es.net.http.auth.pass", "elastic") \
  .option("es.query", myquery)

stock = myreader.load("stock_master")

stock = stock.withColumnRenamed("Date","DateStock").select(['DateStock','Open','High','Low','Volume','Close'])

# pipeline
cleaner = CleanNews(inputCol="Text", outputCol="cleaned_text")

document_assembler = DocumentAssembler()\
    .setInputCol('cleaned_text')\
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

aggregator = AggNews()

pipeline = Pipeline(stages=[
    cleaner,
    document_assembler,
    tokenizer,
    sequenceClassifier,
    aggregator
])

# fin and transform pipeline
result = pipeline.fit(news.select('Date','Text')).transform(news.select('Date','Text'))

# concat the stock and news data by date
new = stock.join(result,stock.DateStock == result.Date,'inner')
new = new.select(['DateStock','Open','High','Low','Volume','Close','Nbr_article','Positive','Negative','Neutre']).withColumnRenamed("DateStock","Date")
new = new.dropna()
dateNow = datetime.now().strftime("%Y-%m-%d")
result = new.filter((new.Date != dateNow))
# write data to batch view
result.write\
    .format("org.elasticsearch.spark.sql")\
    .option("es.nodes","http://elasticsearch")\
    .option("es.port","9200")\
    .option("es.nodes.wan.only","true")\
    .option("es.mapping.id","Date")\
    .option("es.mapping.date.rich","false")\
    .option("es.resource", "batch_view")\
    .option("es.net.http.auth.user","elastic")\
    .option("es.net.http.auth.pass","elastic")\
    .mode("overwrite")\
    .save()
        
