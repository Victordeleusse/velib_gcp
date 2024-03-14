import os
import sqlite3
import pandas as pd
import requests
import tempfile
import pandas as pd
from datetime import datetime
from datetime import date
from datetime import timedelta
from google.cloud import storage
from google.cloud import exceptions
from google.cloud.exceptions import NotFound


# # # GC Storage # # #
project_id = os.getenv("PROJECT_ID")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key-" + project_id + ".json"
gcp_key_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# # # URL historical data link # # #
url_historical_data_base = "https://velib.nocle.fr/dump/"  # 2024-MM-DD-data.db

def get_prw_week_bdays():
    today = date.today()
    day_of_week = today.weekday()
    day_list = []
    for i in range(day_of_week + 3, day_of_week + 8):
        day = today - timedelta(days=i)
        day_list.append(day.strftime("%Y-%m-%d"))
    return day_list

def load_data_in_temp_bucket(day_list):
    for day in day_list:
        storage_client = storage.Client(project=project_id)
        prefix = "velib-data-day-"
        bucket_name = prefix + day
        # Check if the bucket already exists
        try:
            bucket = storage_client.get_bucket(bucket_name)
            print(f"Bucket {bucket_name} already there.")
        except NotFound:
            print(f"Creating bucket {bucket_name} ...")
            bucket = storage_client.create_bucket(bucket_name)
            print(f"Creation of a new bucket : {bucket_name}")
        blob_name = bucket_name + ".db"
        blob = bucket.blob(blob_name)
        url = url_historical_data_base + day + "-data.db"
        response = requests.get(url)
        if response.status_code == 200:
            blob.upload_from_string(response.content)
            print(f"Data from {url} has been loaded in gs://{bucket_name}/{blob_name}.")
            sqlite_to_gcs(bucket_name, blob_name)
        else:
            print(f"Loading data from {url} failed.")

def sqlite_to_gcs(bucket_name, file_name):
    print("Conversion en .csv")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_bytes()
    # Create a temporary file to store SQLite data
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(data)
        temp_file_name = temp_file.name
    try:
        with sqlite3.connect(temp_file_name) as conn:
            # # Join status and statusConso tables :
            query_join_status = "SELECT * FROM status LEFT JOIN statusConso ON statusConso.id = status.idConso"
            df_join_status = pd.read_sql_query(query_join_status, conn)
            csv_data_join_status = df_join_status.to_csv(index=False)
            blob_name = "join_status.csv"
            new_blob = bucket.blob(blob_name)
            new_blob.upload_from_string(csv_data_join_status, content_type="text/csv")
            print(
                f"Tables status et statusConso merged, converties et sauvegardées en CSV."
            )
            # # stations table
            query_stations = f"SELECT * FROM stations"
            df_stations = pd.read_sql_query(query_stations, conn)
            csv_data_stations = df_stations.to_csv(index=False)
            blob_name = "stations.csv"
            new_blob = bucket.blob(blob_name)
            new_blob.upload_from_string(csv_data_stations, content_type="text/csv")
            print(f"Table stations convertie et sauvegardée en CSV.")
    finally:
        os.remove(temp_file_name)

if __name__ == "__main__":
    print("EXECUTION")
    day_list = get_prw_week_bdays()
    print(f"Loading data for days : {day_list}")
    load_data_in_temp_bucket(day_list)