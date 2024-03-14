# PRESENTATION

I completed this project in order to learn how to use Google Cloud Platform's some common data engineering services.

This application allows a user to find the availability of bicycles for a desired Velib' station over the past week.

Collecting data from the velib.nocle.fr API, storing and processing it on GCP: Storage, Functions, Cluster, Dataproc, BigQuery, Run + Docker.

## 1. GCP Project Creation and Configuration

Create a project on GCP after authenticating with Google Cloud CLI :

```
# Creating a new gcloud project
gcloud projects create YOUR_PROJECT

# List projects
gcloud projects list

# Project activation
gcloud config set project YOUR_PROJECT

# Creating a service account
gcloud iam service-accounts create admin-YOUR_PROJECT

# List service accounts
gcloud iam service-accounts list
# admin-YOUR_PROJECT@YOUR_PROJECT.iam.gserviceaccount.com

# Granting permissions (bigquery admin) to the service account
gcloud projects add-iam-policy-binding YOUR_PROJECT --member="serviceAccount:admin-YOUR_PROJECT@YOUR_PROJECT.iam.gserviceaccount.com" --role="roles/bigquery.admin"

# Granting permissions (storage admin) to the service account
gcloud projects add-iam-policy-binding YOUR_PROJECT --member="serviceAccount:admin-YOUR_PROJECT@YOUR_PROJECT.iam.gserviceaccount.com" --role="roles/storage.admin"

# Creating a key for the service account
gcloud iam service-accounts keys create key-YOUR_PROJECT.json --iam-account=admin-YOUR_PROJECT@YOUR_PROJECT.iam.gserviceaccount.com

# Linking the billing account to the project
gcloud alpha billing accounts list

gcloud alpha billing projects link YOUR_PROJECT --billing-account=YOUR_ACCOUNT_ID
```

Please add at the root of the project you key_PROJECT_ID.json key. Then create a **.env** file and add your personnal informations :
```
PROJECT_ID = 'YOUR_PROJECT_ID'
SERVICE_ACCOUNT_EMAIL = 'YOUR_PROJECT@YOUR_PROJECT_ID.iam.gserviceaccount.com'
GOOGLE_APPLICATION_CREDENTIALS=./PATH_TO_YOUR_KEY_PROJECT_ID.json 
```

## 2. Data extraction and storage

The process of extracting and storing the data is done by running the command **docker-compose up** from the root of the project. The data will then be extracted through successive requests to the API at **https://velib.nocle.fr** and stored in automatically created buckets following the extraction operation. Please note that this operation will only be executed once: the extracted data is based on a daily basis for all Velib' stations in the city of Paris.

The data provided in **SQL** format is distributed across 3 tables: 'stations', 'status', and 'statusConso'. An initial **JOIN** operation based on station IDs is performed on the 'status' and 'statusConso' tables to form the dataframe: df_join_status. A second dataframe is formed from the 'stations' table. These 2 dataframes are then exported in **CSV** format to the working bucket.

## 3. Use of Dataproc to handle Apache Spark cluster operations (MAP/REDUCE)

The data has been processed in CSV to allow Spark operations.

First create a Cluster on GCP :

```
gcloud dataproc clusters create YOUR_CLUSTER_NAME \                    
--region us-central1 \
--zone us-central1-a \ 
--master-machine-type n1-standard-2 \
--master-boot-disk-size 50 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 50 \
--num-workers 2 \
--project YOUR_PROJECT_ID
```

Generate a bucket named for example ```gs://pysparkfunctions```
Store in this bucket ```pyspark_functions.py```, ```requirements.txt``` and ```key-YOUR_PROJECT_ID.json``` files :

```
- pyspark_functions.py (gsutil cp pyspark_functions.py gs://pysparkfunctions/functions.py)
- requirements.txt (gsutil cp requirements.txt gs://pysparkfunctions/requirements.txt)
- key-YOUR_PROJECT_ID.json (gsutil cp YOUR_PROJECT_ID.json gs://pysparkfunctions/YOUR_PROJECT_ID.json)
```

Please retrieve YOUR_STATION_ID on **https://velib.nocle.fr** and then run :
```
gcloud dataproc jobs submit pyspark gs://pysparkfunctions/functions.py --cluster=YOUR_CLUSTER_NAME --region=XXXXXXXXX --files=gs://pysparkfunctions/key-YOUR_PROJECT_ID.json --py-files=gs://pysparkfunctions/requirements.txt -- YOUR_STATION_ID

# Export the result to a local file
gsutil cp gs://velib-data-day-graph_result/graph_result.png ./graph_result.png

```
