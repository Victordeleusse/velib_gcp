# velib_gcp
Collecting data from velib_metropole, storing and processing it on GCP.


## 1. GCP Project Creation and Configuration

Create a project on GCP after authenticating with Google Cloud CLI.

*Please read the documentation.*


## 2. Data Collection and Storage from API (Functions, Pub/Sub, Scheduler), BigQuery

``` 
# Enabling APIs: Build, Functions, Pub/Sub, Scheduler
gcloud services enable cloudbuild.googleapis.com
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable cloudscheduler.googleapis.com

# Creating GCS bucket for data collection
gcloud storage buckets create gs://velib_api_data_records
# Creating GCS bucket for function storage
gcloud storage buckets create gs://velib_api_bucket_functions

# Creating a BigQuery dataset
bq mk velib_api_dataset 
```

Using Python, we create with **create_tables.py** the BigQuery tables, and will populate the stations table (*Could be a way to improve the app to create an other GCS bucket to load stations data on a different schedule, in order to get change of Velib Idf Landscape*).

### Cloud Functions : content and script transfert to GCS bucket

Zipping the functions files **main.py, key-your-project-id.json and requirements.txt** and transferring to our GCS bucket designed to get functions 

```
gsutil cp cloud-functions-velib-api.zip gs://velib_api_bucket_functions
```

Creating a job scheduler that sends a message to the Pub/Sub topic cloud-function-trigger-velib every minute

```
gcloud scheduler jobs create pubsub cf-velib-minute --schedule="* * * * *" --topic=cloud-function-trigger-velib --message-body="{Message from the cf-velib-minute Pub/Sub scheduler}" --time-zone="Europe/Paris" --location=europe-west1 --description="Scheduler every minute"
```

### Cloud Functions : deployment

Creating a Cloud Function triggered by the Pub/Sub topic cloud-function-trigger-velib

```
gcloud functions deploy velib_scheduled_function --region=europe-west1 --runtime=python311 --trigger-topic=cloud-function-trigger-velib --source=gs://velib_api_bucket_functions/cloud-functions-velib-api.zip --entry-point=velib_pubsub
```