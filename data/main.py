# import findspark
from io import BytesIO
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
# from pyspark.sql import SparkSession


# # # GC Storage # # # 
project_id = 'booming-splicer-415918'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key-" + project_id + ".json"
gcp_key_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
bucket_historical_data = 'velib_api_historical_data'

# spark = SparkSession.builder \
#     .appName("SQLite Data Merge GCS") \
#     .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
#     .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_key_file) \
#     .getOrCreate()

# # # URL data link # # #
url_historical_data_base = 'https://velib.nocle.fr/dump/' #2024-MM-DD-data.db

def get_yesterday():
    today = date.today()
    yesterday = today - timedelta(days = 4)
    return yesterday

def load_data_in_temp_bucket():
    """
    Background Cloud Function to be triggered manually by an event HTTP.
    """
    storage_client = storage.Client()
    yesterday_date = get_yesterday().strftime("%Y-%m-%d")
    prefix = 'victordeleusse'
    bucket_name = prefix + yesterday_date
    # Check if the bucket already exists
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Le bucket {bucket_name} existe déjà.")
    except NotFound:
        print(f"Le bucket {bucket_name} n'existe pas. Création en cours...")
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Creation of a new bucket : {bucket_name}")    
    blob_name = bucket_name + '.db'
    blob = bucket.blob(blob_name)
    url = url_historical_data_base + yesterday_date + '-data.db'
    response = requests.get(url)
    if response.status_code == 200:
        blob.upload_from_string(response.content)
        print(f"Le contenu de {url} a été téléchargé dans gs://{bucket_name}/{blob_name}.")
        sqlite_to_gcs(bucket_name, blob_name)
    else:
        print(f"Échec du téléchargement depuis {url}")


def sqlite_to_gcs(bucket_name, file_name):
    print("Conversion en .csv")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_bytes()
    # Créer un fichier temporaire pour stocker les données SQLite
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(data)
        temp_file_name = temp_file.name
    try:
        with sqlite3.connect(temp_file_name) as conn:
            # # Join status and statusConso tables :
            query_join_status = "SELECT * FROM status LEFT JOIN statusConso ON statusConso.id = status.idConso" 
            df_join_status = pd.read_sql_query(query_join_status, conn)
            csv_data_join_status = df_join_status.to_csv(index=False)
            blob_name = 'join_status.csv'
            new_blob = bucket.blob(blob_name)
            new_blob.upload_from_string(csv_data_join_status, content_type='text/csv')
            print(f"Tables status et statusConso merged, converties et sauvegardées en CSV.")
            # # stations table
            query_stations = f"SELECT * FROM stations" 
            df_stations = pd.read_sql_query(query_stations, conn)
            csv_data_stations = df_stations.to_csv(index=False)
            blob_name = 'stations.csv'
            new_blob = bucket.blob(blob_name)
            new_blob.upload_from_string(csv_data_stations, content_type='text/csv')
            print(f"Table stations convertie et sauvegardée en CSV.")
    finally:
        os.remove(temp_file_name)
    
def read_csv(data):
    df = pd.read_csv(data)
    print(df.head())
    
if __name__ == "__main__":
    print("EXECUTION")
    # load_data_in_temp_bucket()
    data_csv = 'gs://victordeleusse2024-03-03/join_status.csv'
    read_csv(data_csv)