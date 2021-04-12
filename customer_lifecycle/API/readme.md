
## Build an API

Based on the &quot;policy\_analysis&quot; dataset above, build a RESTful or RPC web service that can be queried for:

- Total policy count for a given user
- Total days active for a given user
- Total new user count for a given date
- Total lapsed user count for a given month
- Total new users premium per date for a given underwriter

Your solution should also meet the following requirements:

- All responses should be in JSON format.
- Each endpoint should accept an optional parameter for calculating the totals for a specific month, and an optional parameter for a specific underwriter.
- The web service can be built using either Golang or Python. Ideally, we would like it to be provided with a build and dependency manager, such as Pipenv, with all dependencies accessible through publicly available repositories. Otherwise, please package all dependencies in your solution. Nevertheless, please provide instructions for building and deploying the application in a README.md file.
- You are not limited to any particular framework and we don&#39;t expect any data to persist beyond the lifespan of the server.
- Where possible, you should write tests for the application.

Nice to haves:

- Ability to pull all policy data


# How to use the code

**Prerequisites**


- CLI with gcloud 
- GCP project with right to execute and build code
- GCP project with billing account
- Cloud function and cloud build enabled (example gcloud services enable cloudfunctions.googleapis.com)
- Postgres DB fully set up via cloud SQL with right schema and data
- CLI with git 


**Limitation/Next steps**


- Function max memory size has to be more than query response size
- Deploy in cloud run for full API deployment for multi purpose use-case and E2E testing
- Query or other params isn't verified before execution if they are outside the specified format
- Function does not have a help section

## Step 1

Clone the repo to your desired CLI location

```bash

git clone -b cs_lifecycle https://github.com/jcroid/python.git

cd python/API/
ls

```

## Step 2

Make a query and dictionary with postgres details read to pass to CURL request  

```bash

gedit sample.json

```

ALL CAPS variables need to be replaced with appropriate values in sample.json file.
month and underwriter are optional values if not required remove them from dic before passing them. 
For cloud function instead of IP_ADDRESS use '/cloudsql/INSTANCE_CONNECTION_NAME' 


save the file before closing it

## Step 3

Test the function if needed.

```bash
pip3 install functions-framework

./test/http_run_ubuntu.sh

```
This should host the CF script on 4000.
To run on local machine the host should be IP address of accessible dB.

CURL request can be run using another terminal on local machine

```bash

curl -vX GET http://localhost:4000 -H "Content-Type: application/json" -d @sample.json


```


## Step 4

Deploying the function using the below or cs_lifecycle_api.tf if terraform env and auth are already set-up on CLI 


```bash

cd CF/

GCF_NAME="customer_lifecycle"
GCF_ENTRY="customer_lifecycle"
GCF_REGION="europe-west1"

gcloud functions deploy $GCF_NAME \
  --entry-point $GCF_ENTRY \
  --region $GCF_REGION \
  --runtime python37 \
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

## Sample QUERY for SQL with month optional variable

```bash
select user_id from public.policy
where to_char(policy_start_date,'YYYY-MM') = '{month}'

```