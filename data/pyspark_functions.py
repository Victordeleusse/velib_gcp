import pyspark
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, functions
from pyspark.sql.functions import dayofmonth, month, year, hour, minute

from datetime import date
from datetime import timedelta


project_id = "booming-splicer-415918"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key-" + project_id + ".json"
gcp_key_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# # # # Using Spark to manipulate data # # #
# 1. Cluster Creation
cluster = 'https://dataproc.googleapis.com/v1/projects/booming-splicer-415918/regions/us-central1/clusters/velib-api1-cluster'
cluster_name = 'velib-api1-cluster'
region = "us-central1"
# 2. 

def get_yesterday():
    today = date.today()
    yesterday = today - timedelta(days=4)
    return yesterday

def aggregate_data():

    # yesterday = get_yesterday().strftime("%Y-%m-%d")
    yesterday = "2024-03-03"
    bucket_data_csv = 'gs://victordeleusse' + yesterday + '/join_status.csv'
    
    spark = SparkSession.builder \
    .appName("SQLite Data Merge GCS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_key_file) \
    .getOrCreate()
    trips = spark.read.csv(bucket_data_csv, header=True, inferSchema=True)
    trips_day = trips.withColumn("day", functions.dayofmonth("date")) \
                     .withColumn("month", functions.month("date")) \
                     .withColumn("year", functions.year("date")) \
                     .withColumn("hour", functions.hour("date")) \
                     .withColumn("minute", functions.minute("date"))

    # Afficher les premières lignes pour tester
    trips_day.show()
    
if __name__ == "__main__":
    aggregate_data()