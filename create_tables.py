from google.cloud import bigquery
import requests
import os
import sys

# get the args from the command line
try:
    project_id = sys.argv[1]
    dataset_id = sys.argv[2]
except Exception as e:
    print(e)
    print("Usage: python3 create_tables.py <project_id> <dataset_id>")
    sys.exit(1)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key-" + project_id + ".json"

client = bigquery.Client()

# Create a 'stations' table 
table_id_stations = project_id + '.' + dataset_id + '.stations'

stations_schema = [
    bigquery.SchemaField("station_id",  "INT64"),
    bigquery.SchemaField("name",        "STRING"),
    bigquery.SchemaField("latitude",    "FLOAT64"),
    bigquery.SchemaField("longitude",   "FLOAT64"),
    bigquery.SchemaField("capacity",    "INT64"),
    bigquery.SchemaField("stationCode", "STRING"),
]
table = bigquery.Table(table_id_stations, schema=stations_schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)

# Create a 'records' table
table_id_records = project_id + '.' + dataset_id + '.records'
record_schema = [
    bigquery.SchemaField("stationCode",             "INT64"),
    bigquery.SchemaField("station_id",              "INT64"),
    bigquery.SchemaField("num_bikes_available",     "INT64"),
    bigquery.SchemaField("numBikesAvailable",       "INT64"),
    bigquery.SchemaField("num_bikes_available_types", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("mechanical",          "INT64"),
        bigquery.SchemaField("ebike",               "INT64"),
    ]),
    bigquery.SchemaField("num_docks_available",     "INT64"),
    bigquery.SchemaField("numDocksAvailable",       "INT64"),
    bigquery.SchemaField("is_installed",            "INT64"),
    bigquery.SchemaField("is_returning",            "INT64"),
    bigquery.SchemaField("is_renting",              "INT64"),
    bigquery.SchemaField("last_reported",             "TIMESTAMP"),
]
table = bigquery.Table(table_id_records, schema=record_schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)

# Populate the stations table once with a query
# Will get a new bucket to upload new data weekly or monthly maybe in the futur
url_stations_list = 'https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json'
response = requests.get(url_stations_list)
data = response.json()["data"]["stations"]

rows_to_insert = []
for record in data:
    rows_to_insert.append(
        (
            record["station_id"],
            record["name"],
            record["lat"], #latitude
            record["lon"], #longitude
            record["capacity"],
            record["stationCode"],
        )
    )

table_id = project_id + '.' + dataset_id + '.stations' # previously named table_id_stations
try:
    table = client.get_table(table_id)
    client.insert_rows(table, rows_to_insert)
    print("Station's rows inserted into table {}".format(table_id))
except Exception as e:
    print(e)