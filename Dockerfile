FROM apache/airflow:2.3.3

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

RUN pip install --no-cache-dir pyspark==3.2.1 spark-nlp==3.4.4 emoji==2.0.0 apache-airflow-providers-apache-spark yfinance==0.1.70 kafka-python==2.0.2 tweepy==4.9.0 pmdarima==1.8.5
