FROM python:3.8-slim

# Définir les variables d'environnement pour Spark
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless wget procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.4.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/spark

RUN wget --no-verbose https://downloads.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C / && \
    mv /spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# Pour ajouter Spark au PATH !!
ENV PATH=$PATH:$SPARK_HOME/bin

COPY . /app
WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

COPY start-spark.sh /start-spark.sh
RUN chmod +x /start-spark.sh

CMD ["/start-spark.sh"]

