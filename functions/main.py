import base64
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
import requests
import json
import pytz
import os


# Define variables for Cloud Functions
project_id = 'booming-splicer-415918'
bucket_stations_records = 'velib_api_data_stations_records' # to store datas from stations (high frequency)
dataset_id = 'velib_api_dataset'
# table_id_stations = project_id + '.' + dataset_id + '.stations'
table_id_records = project_id + '.' + dataset_id + '.records'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key-" + project_id + ".json"

# url_stations_list = 'https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json'
url_stations_records = 'https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json'
paris_tz = pytz.timezone('Europe/Paris')
str_time_paris = datetime.now(paris_tz).strftime('%Y-%m-%d_%H:%M:%S')


# To extract data from API
def get_json_data(url):
    response = requests.get(url)
    return response.json()

# To store data to GCS bucket
def store_data_json_to_gcs_bucket(data, bucket_name=bucket_stations_records, str_time_paris=str_time_paris):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Replace with the desired object name
    object_name = "data___" + str_time_paris + ".json"
    
    # ========== Anticipating Spark process : ================== #
    # replace object_name characters ":" and "-" with "_", 
    object_name = object_name.replace(":", "_").replace("-", "_")
    # ========================================================== #

    blob = bucket.blob(object_name)

    # Convert data to JSON string and upload to GCS
    json_data = json.dumps(data)
    blob.upload_from_string(json_data)

# To store data into my tables
def insert_data_json_to_bigquery(data, project_id=project_id, dataset_id=dataset_id, table_id=table_id_records):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)  # API call

    data_to_insert = []
    last_report = data["lastUpdatedOther"]
    for record in data['data']['stations']:
        row = {}
        row["stationCode"]              = record["stationCode"]
        row["station_id"]               = record["station_id"]
        row["num_bikes_available"]      = record["num_bikes_available"]
        row["numBikesAvailable"]        = record["numBikesAvailable"]
        row["num_bikes_available_types"]["mechanical"]  = record["num_bikes_available_types"]["mechanical"]
        row["num_bikes_available_types"]["ebike"]       = record["num_bikes_available_types"]["ebike"]
        row["num_docks_available"]      = record["num_docks_available"]
        row["numDocksAvailable"]        = record["numDocksAvailable"]
        row["is_installed"]             = record["is_installed"]
        row["is_returning"]             = record["is_returning"]
        row["is_renting"]               = record["is_renting"]
        row["last_reported"]            = last_report
        data_to_insert.append(row)
    client.insert_rows(table, data_to_insert)

# Function aut. executed when order sent
def velib_pubsub(event, paris_tz=paris_tz, url_name=url_stations_records, bucket_name=bucket_stations_records, context='context'):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    str_time_paris = datetime.now(paris_tz).strftime('%Y_%m_%d_%H_%M_%S')

    try:
        json_data = get_json_data(url_name)
        print("Data extracted from API")
    except Exception as e:
        print(e)
    
    try:
        store_data_json_to_gcs_bucket(json_data, bucket_name, str_time_paris)
        print("File uploaded to gs://" +  bucket_name + "/{}.".format("data___" + str_time_paris + ".json"))
    except Exception as e:
        print(e)

    try:
        insert_data_json_to_bigquery(json_data)
        print("Data inserted into BigQuery")
    except Exception as e:
        print(e)

if __name__ == "__main__":
    velib_pubsub('data', 'context')
    # velib_pubsub('data', paris_tz, url_stations_list, bucket_stations_list, 'context')