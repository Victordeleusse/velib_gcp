import pyspark
import sys
import os
import numpy as np
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, functions, Row
import matplotlib.pyplot as plt
from google.cloud import storage
from google.cloud.exceptions import NotFound

from datetime import date
from datetime import timedelta


project_id = "booming-splicer-415918"
# project_id = sys.argv[0]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key-" + project_id + ".json"
gcp_key_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# # # # Using Spark to manipulate data # # #
# 1. Cluster Creation
cluster = "https://dataproc.googleapis.com/v1/projects/booming-splicer-415918/regions/europe-west1/clusters/velib-api1-cluster"
cluster_name = "velib-api1-cluster"
region = "europe-west1"

def get_prw_week_bdays():
    today = date.today()
    day_of_week = today.weekday()
    day_list = []
    for i in range(day_of_week + 3, day_of_week + 8):
        day = today - timedelta(days=i)
        day_list.append(day.strftime("%Y-%m-%d"))
    return day_list


def aggregate_data():
    day_list = get_prw_week_bdays()
    spark = (
        SparkSession.builder.appName("SQLite Data Merge GCS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_key_file
        )
        .getOrCreate()
    )
    week_trips = None
    for day in day_list:
        bucket_data_csv = "gs://velib-data-day-" + day + "/join_status.csv"
        day_trips = spark.read.csv(bucket_data_csv, header=True, inferSchema=True) \
            .withColumn("day", functions.dayofmonth("date")) \
            .withColumn("month", functions.month("date")) \
            .withColumn("year", functions.year("date")) \
            .withColumn("hour", functions.hour("date")) \
            .withColumn("minute", functions.minute("date"))
        if week_trips is None:
            week_trips = day_trips
        else:
            week_trips = week_trips.union(day_trips)
    return week_trips

# # 2. MAP REDUCE implementation

# # 2.1. MAP : Define a key-value pair
# #   -> key identifier to get value per station and month/day/hour.
# #   -> unicity of values. Looking for minute (as we want to get our data per hour), nbBike, nbEBike, nbEDock.

def map_trips(row):
    key = (int(row.code), row.day, row.month, row.hour)
    value = np.array([int(row.minute), int(row.nbBike4), int(row.nbEBike5), int(row.nbEDock7)])
    return key, value

# # 2.2. REDUCE = AGGREGATION STRATEGIE
# # Define a way to get only one value per key.

def get_data_per_hour(value_x, value_y):
    # Here minute in at first position of the array
    # Keeping the closest value from the targeting hour
    if value_x[0] < value_y[0]:
        return value_x
    return value_y


def get_daily_dataframe(data_rdd):
    global_data = np.asarray(data_rdd.collect())
    data_raw = []
    for station in global_data:
        data_raw.append(station.flatten())
    data_raw = np.asarray(data_raw)
    new_columns = ["code", "day", "month", "hour", "minute", "nbBike", "nbEBike", "nbEDock"]
    data_raw = pd.DataFrame(data=data_raw, columns=new_columns)
    data_raw = data_raw.drop(['minute'], axis=1)
    data_raw['bikes_availibility'] = (data_raw['nbBike'] + data_raw['nbEBike']) / data_raw['nbEDock']
    return data_raw

def get_visualization_station(daily_dataframe, station_code):
    plt.figure(figsize=(12,8))
    station_sub = daily_dataframe[daily_dataframe['code'] == station_code]
    station_sub = station_sub.sort_values(['day', 'hour'])
    
    hours = np.arange(24)
    days_of_the_week = station_sub['day'].unique()
    days_of_the_week = np.sort(days_of_the_week)
    week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    for i, day in enumerate(days_of_the_week):
        availability = []
        daily_data = station_sub[station_sub['day'] == day]
        for h in hours:
            availability.append(daily_data[daily_data['hour'] == h]['bikes_availibility'])
        plt.plot(hours, availability, label=f'{week[i]}')
    plt.xlabel('Hour')
    plt.ylabel('Availability rate')
    plt.title('Last week bikes availibility rate.')
    plt.legend()
    plt.savefig('/tmp/graph.png')
    
    storage_client = storage.Client(project=project_id)
    prefix = "velib-data-day-"
    bucket_name = prefix + 'graph_result'
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Bucket {bucket_name} already there.")
    except NotFound:
        print(f"Creating bucket {bucket_name} ...")
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Creation of a new bucket : {bucket_name}")
    blob_name = 'graph_result.png'
    blob = bucket.blob(blob_name)
    if blob.exists():
        print(f"Blob {blob_name} already exists. Deleting...")
        blob.delete()
        print(f"Blob {blob_name} deleted.")
    blob.upload_from_filename('/tmp/graph.png')
    # blob.download_to_filename('./result.png')
    print(f"File uploaded.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        station_id = int(sys.argv[1])
        print(f"Station ID : {station_id}")
        trips_day = aggregate_data()
        trips_per_station_rdd = trips_day.rdd.map(map_trips)
        trips_group_per_day = trips_per_station_rdd.reduceByKey(get_data_per_hour)
        data = get_daily_dataframe(trips_group_per_day)
        get_visualization_station(data, station_id)
    else:
        print("Please provide station ID as argument.\n gcloud dataproc jobs submit pyspark gs://pysparkfunctions/functions.py --cluster=YOUR_CLUSTER_NAME --region=YOUR_CLUSTER_REGION --files=gs://BUCKET/key.json --py-files=gs://BUCKET/requirements.txt -- STATION_ID")