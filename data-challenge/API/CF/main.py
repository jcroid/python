from google.cloud import bigquery
from google.cloud import bigquery_storage
import os
import google.auth


def type_acquirer_currency(startdate, enddate):
    return f""" SELECT
    acquirer,
    currency,
   SUM(amount_processed) AS total_amount
   FROM
   `PROJECT_ID.DATASET.acquirer_value_table`
   WHERE
   DATE(hourTimestamp) BETWEEN "{startdate}"
   AND "{enddate}"
   GROUP BY
   1,
   2"""


def type_status(startdate):
    return f""" SELECT
   max(hourTimestamp)
   FROM
   `PROJECT_ID.DATASET.acquirer_value_table`
   WHERE
   DATE(hourTimestamp) BETWEEN "{startdate}"
   AND "{startdate}"
   """


def query_to_json(
    query,
    bqclient,
    bqstorageclient,
    job_config=None
):

    # execute query
    dataframe = (
        bqclient.query(query, job_config)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )
    dataframe = dataframe.to_json(orient='records')
    return dataframe


def API_service(request):
    """ HTTP Cloud Function
    pass json data as input for the required vars
    JSON output for the requested data

    """
    drive = "/tmp/"

    # set directory for the terminal
    os.chdir(drive)
    project_id = "PROJECT_ID"
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    bigquery_client = bigquery.Client(
        credentials=credentials, project=project_id)
    bigquery_storage_client = bigquery_storage.BigQueryReadClient(
        credentials=credentials)
    request_json = request.get_json(silent=True)

    if 'type' in request_json:
        if request_json['type'] == 'TEST' or request_json['startdate'] == 'START_DATE':
            output_json = {"error": "sample file"}
        elif 'startdate' in request_json:
            if request_json['type'] == 'acquirer_currency' and 'enddate' in request_json:
                query = type_acquirer_currency(
                    request_json['startdate'], request_json['enddate'])

                output_json = query_to_json(query=query,
                                            bqclient=bigquery_client,
                                            bqstorageclient=bigquery_storage_client)
            elif request_json['type'] == 'status':
                query = type_status(request_json['startdate'])
                output_json = query_to_json(query=query,
                                            bqclient=bigquery_client,
                                            bqstorageclient=bigquery_storage_client)
                output_json[
                    'status'] = 'Last hourTimestamp for data injested'
        else:
            output_json = {"error": "startdate not present"}
    else:
        output_json = {"error": "type not present"}

    headers = {'Content-Type': 'application/json; charset=utf-8'}
    return output_json, headers
