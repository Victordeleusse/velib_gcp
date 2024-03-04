import os
import requests
import pandas as pd
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.cloud import bigquery

project_id = "booming-splicer-415918"
dataset_name = "velib_api_dataset"

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key-" + project_id + ".json"
with open("GOOGLE_MAPS_API_KEY.txt", "r") as f:
    GOOGLE_MAPS_API_KEY = f.read()

client = bigquery.Client()


def create_app():
    app = Flask(__name__)
    CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)
    # debug_mode = os.environ.get('FLASK_DEBUG', 'false').lower() in ['true', '1', 't']
    # app.debug = debug_mode
    return app


app = create_app()


# # # # # ROUTES # # # # #

def get_realtime_data(stationCode=None):
    url_stations_records = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
    response = requests.get(url_stations_records)
    records = response.json()["data"]["stations"]

    data = []
    for record in records:
        # To use when we will implement request on specific station --- 
        
        mechanical_bikes = 0
        ebike_bikes = 0
        
        if stationCode and record["stationCode"].lower() != stationCode.lower():
            continue
        
        for bike_type in record["num_bikes_available_types"]:
            if "mechanical" in bike_type:
                mechanical_bikes = bike_type["mechanical"]
            elif "ebike" in bike_type:
                ebike_bikes = bike_type["ebike"]

        station = {}
        station["stationCode"]              = record["stationCode"]
        station["station_id"]               = record["station_id"]
        station["num_bikes_available"]      = record["num_bikes_available"]
        station["numBikesAvailable"]        = record["numBikesAvailable"]
        station["num_bikes_available_types"] = {"mechanical": mechanical_bikes, "ebike": ebike_bikes}
        station["num_docks_available"]      = record["num_docks_available"]
        station["numDocksAvailable"]        = record["numDocksAvailable"]
        station["is_installed"]             = record["is_installed"]
        station["is_returning"]             = record["is_returning"]
        station["is_renting"]               = record["is_renting"]
        station["last_reported"]            = record["last_reported"]

        data.append(station)

    return data


@app.route("/")
def index():
    # Real-time stations infos are requested from the bike sharing service API, and stored in the variable stations_infos via the get_realtime_data call.
    # As it is "real-time" data, no need to source from Bigquery tables
        # Data is gonna be used by the google maps API for displaying markers and stations informations on the map
    stations_infos = get_realtime_data()
    general_infos = {
        "nb_available_bikes":                           sum([station["num_bikes_available"] for station in stations_infos]),
        "nb_available_places":                          sum([station["num_docks_available"] for station in stations_infos]),
        "nb_empty_stations":                            sum([(station["num_bikes_available"] == 0) and (station["is_renting"] == 1) for station in stations_infos]),
        "nb_full_stations":                             sum([(station["num_docks_available"] == 0) and (station["is_returning"] == 1) for station in stations_infos]),
        "nb_stations_w_n_bikes_greater_than_zero":      sum([(station["num_bikes_available"] > 0) and (station["is_renting"] == 1) for station in stations_infos]),
        "nb_stations_w_n_places_greater_than_zero":     sum([(station["num_docks_available"] > 0) and (station["is_returning"] == 1) for station in stations_infos]),
        "nb_stations_in_service":                       sum([(station["is_renting"] == 1) or (station["is_returning"] == 1) for station in stations_infos]),
        "nb_stations_in_maintenance":                   sum([station["is_installed"] for station in stations_infos]) - sum([(station["is_renting"] == 1) or (station["is_returning"] == 1) for station in stations_infos]),
        # "todays_loan_count":                            todays_loan_count(),
    }
    
    return render_template('index.html', 
                            stations_infos  =   stations_infos,
                            general_infos   =   general_infos,
                            GOOGLE_MAPS_API_KEY = GOOGLE_MAPS_API_KEY
                           )

if __name__ == "__main__":
    app.run(debug=True)
