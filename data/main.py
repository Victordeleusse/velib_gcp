import findspark
import os
import sqlite3
import requests
import csv
import pandas as pd
import pytz
from datetime import datetime
from google.cloud import storage
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQLite Data Processing").getOrCreate()

# # # GC Storage # # # 
project_id = 'booming-splicer-415918'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key-" + project_id + ".json"
bucket_historical_data = 'velib_api_historical_data'

# # # URL data link # # #
url_historical_data_base = 'https://velib.nocle.fr/dump/' #2024-MM-DD-data.db

# # # Settings # # #
paris_tz = pytz.timezone('Europe/Paris')
str_time_paris = datetime.now(paris_tz).strftime('%Y-%m-%d_%H:%M:%S')

def download_historical_data(bucket_name, url):
    """Télécharge un contenu directement depuis une URL vers un blob GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    for i in range(1, 3):
        destination_blob_name = f"2024-03-0{i}-data.db"
        blob = bucket.blob(destination_blob_name)
        if blob.exists():
            print(f"Le blob {destination_blob_name} existe déjà dans gs://{bucket_name}. Téléchargement ignoré.")
        # Télécharger le contenu depuis l'URL
        else:
            url_date = url + f"2024-03-0{i}-data.db"
            response = requests.get(url_date)
            if response.status_code == 200:
                blob.upload_from_string(response.content)
                print(f"Le contenu de {url} a été téléchargé dans gs://{bucket_name}/{destination_blob_name}.")
            else:
                print(f"Échec du téléchargement depuis {url}")    

if __name__ == "__main__":
    download_historical_data(bucket_historical_data, url_historical_data_base)
