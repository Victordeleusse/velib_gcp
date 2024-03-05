FROM openjdk:8-jdk

ENV SPARK_VERSION=3.1.1
ENV HADOOP_VERSION=3.2
ENV SPARK_HOME=/spark

RUN curl -sL --retry 3 \
  "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" \
  | gunzip \
  | tar x -C / \
&& mv /spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME \
&& chown -R root:root $SPARK_HOME

ENV PATH=$PATH:$SPARK_HOME/bin

COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

EXPOSE 4040

CMD ["spark-shell"]
