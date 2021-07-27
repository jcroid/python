
## Build an API

A service that is able to provide the following functionality:
- Return the total amount processed by acquirer and by currency for a given time range
- Inform about the status of the data ingestion

# How to use the code

**Prerequisites**


- CLI with gcloud 
- GCP project with right to execute and build code
- GCP project with billing account
- Cloud function and cloud build enabled (example gcloud services enable cloudfunctions.googleapis.com)
- BQ fully set up with right schema and data
- CLI with git
- Create BQ table acquirer_value_table (this should merge both the incoming tables types and have columns like acquirer,currency,amount_processed,hourTimestamp as min)
- Create and replace PROJECT_ID and DATASET with appropriate values as per the location of the table stated above.     


**Limitation/Next steps**


- Function max memory size has to be more than type of data accessed.
- Deploy in cloud run for full API deployment for multi purpose use-case and E2E testing
- Function does not have a help section
- ALL CAPS variables need to be replaced with appropriate values
- Testing isn't fully possible unless SDK environment is replicated.
- Full data ingestion can also be obtained by creating a trigger in logger dashboard or using the API to feed information on pending and processed files 
- current data status only gives a down stream view after the file is processed and merged

## Step 1

Paste the folder in your desired CLI location

```bash

cd data-challenge/API/
ls

```

## Step 2

Make a dictionary with data to read via CURL request  

```bash

gedit sample.json

```

ALL CAPS variables need to be replaced with appropriate values in sample.json file.
save the file before closing it

## Step 3

Test the function if needed. To run a full test you would need to set env variables to respond to google.auth.default like application or SDK auth

```bash
pip3 install functions-framework

./test/http_run_ubuntu.sh

```
This should host the CF script on 4000.
CURL request can be run using another terminal on local machine

```bash

curl -vX GET http://localhost:4000 -H "Content-Type: application/json" -d @sample.json


```


## Step 4

Deploying the function using the below or API_service.tf if terraform env and auth are already set-up on CLI 


```bash

cd CF/

GCF_NAME="API_service"
GCF_ENTRY="API_service"
GCF_REGION="europe-west2"

gcloud functions deploy $GCF_NAME \
  --entry-point $GCF_ENTRY \
  --region $GCF_REGION \
  --runtime python39 \
  --trigger-http 

```

## Sample queries for CF from python

```bash
import json

import subprocess
token = '{}'.format(subprocess.Popen(args="gcloud auth print-identity-token",
                                     stdout=subprocess.PIPE, shell=True).communicate()[0])[2:-3]

url = "https://[MYPROJECT].cloudfunctions.net / [FUNC NAME]"

with open('/LOCATION_OF_FILE/sample.json') as jsonfile:
    data = json.load(jsonfile)

req = requests.get(url, json=data, headers={
    "Authorization": "Bearer {}".format(token)})
print(req.status_code)
print(req.text)
```
