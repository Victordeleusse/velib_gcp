import pyspark
import os
import numpy as np
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, functions, Row
import matplotlib.pyplot as plt
from google.cloud import storage

from datetime import date
from datetime import timedelta


project_id = "booming-splicer-415918"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key-" + project_id + ".json"
gcp_key_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# # # # Using Spark to manipulate data # # #
# 1. Cluster Creation
cluster = "https://dataproc.googleapis.com/v1/projects/booming-splicer-415918/regions/us-central1/clusters/velib-api1-cluster"
cluster_name = "velib-api1-cluster"
region = "us-central1"


def get_yesterday():
    today = date.today()
    yesterday = today - timedelta(days=4)
    return yesterday


def aggregate_data():
    # yesterday = get_yesterday().strftime("%Y-%m-%d")
    yesterday = "2024-03-03"
    bucket_data_csv = "gs://victordeleusse" + yesterday + "/join_status.csv"

    spark = (
        SparkSession.builder.appName("SQLite Data Merge GCS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_key_file
        )
        .getOrCreate()
    )
    trips = spark.read.csv(bucket_data_csv, header=True, inferSchema=True)
    trips_day = (
        trips.withColumn("day", functions.dayofmonth("date"))
        .withColumn("month", functions.month("date"))
        .withColumn("year", functions.year("date"))
        .withColumn("hour", functions.hour("date"))
        .withColumn("minute", functions.minute("date"))
    )
    return trips_day


# 2. MAP REDUCE implementation

# 2.1. MAP : Define a key-value pair
#   -> key identifier to get value per station and month/day/hour
#   -> unicity of values (here looking for minute nbBike nbEBike, nbEDock)


def map_trips(row):
    index = (int(row.code), row.day, row.month, row.hour)
    value = np.array(
        [int(row.minute), int(row.nbBike4), int(row.nbEBike5), int(row.nbEDock7)]
    )
    return index, value


# 2.2. REDUCE = AGGREGATION STRATEGIE
# Define a way to get only one value per key here <- We want our data distributed per hour


def group_per_station_by_hour(value_x, value_y):
    # Here minute in at first position of the array
    # Keeping the closest value to the targeting hour
    if value_x[0] < value_y[0]:
        return value_x
    return value_y


def get_daily_dataframe(data_rdd):
    global_data = np.asarray(data_rdd.collect())
    # print(global_data.shape)
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
    station_sub = station_sub.sort_values('hour')
    
    hours = np.arange(24)
    availability = []
    for h in hours: 
        availability.append(station_sub[station_sub['hour'] == h]['bikes_availibility'])
    plt.plot(hours, availability, label='Disponibilité par heure')
    plt.xlabel('Heure')
    plt.ylabel('Taux de disponibilité')
    plt.title('Disponibilité des vélos par heure')
    plt.legend()
    plt.savefig('/tmp/graph.png')
    
    storage_client = storage.Client()
    bucket = storage_client.bucket('graph_result')
    blob = bucket.blob('graph_result.png')

    blob.upload_from_filename('/tmp/graph.png')

    print(f"File uploaded.")

if __name__ == "__main__":
    trips_day = aggregate_data()
    # print(trips_day.columns)
    trips_per_station_rdd = trips_day.rdd.map(map_trips)
    # for result in trips_per_station_rdd.take(10):
    #     print(result)
    trips_group_per_day = trips_per_station_rdd.reduceByKey(group_per_station_by_hour)
    data = get_daily_dataframe(trips_group_per_day)
    # print(data.head())
    get_visualization_station(data, 31104)