
## Build Automated or on-demand processes to ingest and store the data

# How to use the code

**Prerequisites**


- CLI with gcloud 
- GCP project with right to execute and build code
- GCP project with billing account
- Cloud function and cloud build enabled (example gcloud services enable cloudfunctions.googleapis.com)
- BQ fully set up with right schema and data
- CLI with git
- BQ tables as stated in variables table_json and table_csv needs to be created in dataset named mpay
- Five GCS buckets needs to be created with names as ref with variables initial gs_ 


**Limitation/Next steps**


- Function max memory size has to be more than CSV or JSON file size
- A Function will fire for every file loaded in gs_landingzone
- If CSV type of the file is different to sample file function will not load data properly 
- schema of the files cant be dynamic
- no immediate data quality check at stage of ingestion.
- full documentation on all functions
- error catch for all scenario of loading data   


## Step 1

Paste the folder in your desired CLI location. Open a terminal in the same folder

```bash

cd data-challenge/data_loader/
ls

```

## Step 2


Deploying the function using the below.


```bash

cd CF/

GCF_NAME="data_loader"
GCF_ENTRY="data_loader"
GCF_REGION="europe-west2"
GS_LANDINGZONE="landingzone"

gcloud functions deploy $GCF_NAME \
  --entry-point $GCF_ENTRY \
  --region $GCF_REGION \
  --runtime python39 \
  --trigger-bucket $GS_LANDINGZONE

```


## Step 3


After the function has been deployed. load CSV and JSON file to GS_LANDINGZONE bucket