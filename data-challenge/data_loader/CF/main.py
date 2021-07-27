from google.cloud import bigquery
from google.cloud import storage
import os
import re
import pandas as pd


def download_blob(bucket_name, blob_filepath, local_filepath):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_filepath)
    blob.download_to_filename(local_filepath)
    print(
        "Blob {} downloaded to {}.".format(
            blob_filepath, local_filepath)
    )
    return


def mv_blob(bucket_name, blob_name, new_bucket_name, new_blob_name):
    """
    Function for moving files between directories or buckets. it will use GCP's copy function then delete the blob from the old location.

    Args:
        bucket_name: name of bucket
        blob_name: name of file
        new_bucket_name: name of bucket (can be same as original if we're just moving around directories)
        new_blob_name: name of file in new directory in target bucket
    Returns:
        None; the output is written to Stackdriver Logging
    """

    storage_client = storage.Client()
    source_bucket = storage_client.get_bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.get_bucket(new_bucket_name)

    # copy to new destination
    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, new_blob_name)
    # delete in old destination
    source_blob.delete()

    print(f"File moved from {source_blob} to {new_blob}")


def upload_to_gcs(bucketname, filename, drive):
    """
    Function for uploading file to GCS.

    Args:
        bucket_name: name of bucket
        filename: name of file
        drive: local location of the file 

    Returns:
        blob: this can be used in other steps to access the uploaded file 
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucketname)
    blob = bucket.blob(filename)
    blob.upload_from_filename(drive + filename)
    return blob


def load_bq(filetype, bucketname, drive, dataset_id, table_id, uribucket, filename, gs_staging, gs_processed, gs_failed):
    """
    Function for loading data into bq. it will use GCP's load to bq functions from gcs

    Args:
        filename: name of file
        dataset_id: name of dataset
        table_id: name of table
        uribucket: URI is the gcs link to access the file location
        gs_staging: name of bucket for staging
        gs_processed: name of bucket for processed files
        gs_failed: name of the bucket for failed files
        bucketname: name of the bucket the temp upload takes place to BQ
        drive : location of local file 
    Returns:
        str of status ; the output is written to Stackdriver Logging
    """

    if filetype == "csv":
        filename_gcs = filename.split('.')[0] + ".csv"
    else:
        filename_gcs = filename
    blob = upload_to_gcs(bucketname, filename, drive)
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.ignore_unknown_values = True
    destination_table = client.get_table(table_ref)
    startrow = destination_table.num_rows
    try:
        load_job = client.load_table_from_uri(
            uribucket, table_ref, job_config=job_config
        )  # API request
        print("Starting job {}".format(load_job.job_id))
        load_job.result()  # Waits for table load to complete.
        print("Job finished.")
        # move file to processed
        mv_blob(
            bucket_name=gs_staging,
            blob_name=filename_gcs,
            new_bucket_name=gs_processed,
            new_blob_name=filename_gcs,
        )
        blob.delete()
        os.remove(drive + filename)
        return "Success"
    except Exception as e:
        print(f"Failed to create load job: {e}")
        # move file to failed bucket
        mv_blob(
            bucket_name=gs_staging,
            blob_name=filename_gcs,
            new_bucket_name=gs_failed,
            new_blob_name=filename_gcs,
        )
        destination_table = client.get_table(table_ref)
        endrow = destination_table.num_rows
        print(f"Loaded {endrow-startrow} rows.")
        return "Failed"


def clean_csv(drive, filename):
    """Clean csv file and convert it to ndjson file

    Args:
        filename: this is the file name of the csv file

    Returns: filename_json: this is the file name of the json file
    """
    filename_json = filename.split('.')[0] + ".json"
    df = pd.read_csv(filename, skiprows=10, sep='delimiter', engine='python')
    # steps to clean the csv based on sample file
    df.columns = df.columns.str.replace('"', '')
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.replace("-", "_")
    columns = ''.join(df.columns)
    columns = columns.split(',')
    columns.append("extra_column")
    df = pd.read_csv(filename, skiprows=11, sep=',', names=columns)
    del df['extra_column']
    df.to_json(filename_json, orient="records",  lines=True)
    # remove file to reduce storage usage
    os.remove(drive + filename)
    return filename_json


def clean_json(filename):
    df = pd.read_json(filename)
    df.to_json(filename, orient="records",  lines=True, date_format="iso")

    return filename


def download_from_gcs(client, bucket_name, blob_filepath, local_filepath):
    """Downloads file from Google Cloud Storage

    Args:
        client: GCS storage client
        bucket_name: GCS bucket name
        blob_filepath: GCS blob filepath
        local_filepath: local filepath location

    Returns: None
    """
    # Create a bucket object for our bucket
    bucket = client.get_bucket(bucket_name)
    # Create a blob object from the filepath
    blob = bucket.blob(blob_filepath)
    # Download the file to a destination
    blob.download_to_filename(f"{local_filepath}")
    print(
        f"Successfully downloaded GCS file {blob_filepath} to local filepath {local_filepath}"
    )


def data_loader(data, context):
    """
    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    print("Event ID: {}".format(context.event_id))
    print("Event type: {}".format(context.event_type))
    print("Bucket: {}".format(data["bucket"]))
    print("File: {}".format(data["name"]))
    print("Metageneration: {}".format(data["metageneration"]))
    print("Created: {}".format(data["timeCreated"]))
    print("Updated: {}".format(data["updated"]))

    # set directory for the terminal
    drive = "/tmp/"
    os.chdir(drive)

    gs_landingzone = "mpay-landingzone"
    gs_staging = "mpay-staging"
    gs_failed = "mpay-failed"
    gs_processed = "mpay-processed"
    gs_bqtemp = "bq_temp_upload"

    dataset_id = "mpay"
    table_json = "acquirer_json"
    table_csv = "acquirer_csv"

    filename = data["name"]

    # move file from landingzone to staging bucket
    mv_blob(
        bucket_name=gs_landingzone,
        blob_name=filename,
        new_bucket_name=gs_staging,
        new_blob_name=filename,
    )

    # check if we are ingesting the correct file

    contains_json = re.findall(".json", filename)
    contains_csv = re.findall(".csv", filename)

    if not contains_json and not contains_csv:
        print(f"cant process {filename}")
        status = "Failed to process"
        return f" {filename} {status}"
    elif contains_json:
        # download file
        download_blob(
            bucket_name=gs_staging,
            blob_filepath=filename,
            local_filepath=filename,
        )
        filename_json = clean_json(filename)
        uribucket = f"https://{gs_bqtemp}.storage.googleapis.com/{filename}"
        load_bq(filetype="json", bucketname=gs_bqtemp, drive=drive,
                dataset_id=dataset_id,
                table_id=table_json, uribucket=uribucket,
                filename=filename_json,
                gs_staging=gs_staging,
                gs_processed=gs_processed, gs_failed=gs_failed)
        status = "Processed json file"
        return f" {filename} {status}"
    else:
        # download file
        download_blob(
            bucket_name=gs_staging,
            blob_filepath=filename,
            local_filepath=filename,
        )
        filename_json = clean_csv(drive, filename)
        uribucket = f"https://{gs_bqtemp}.storage.googleapis.com/{filename_json}"
        load_bq(filetype="csv", bucketname=gs_bqtemp, drive=drive,
                dataset_id=dataset_id,
                table_id=table_csv, uribucket=uribucket,
                filename=filename_json,
                gs_staging=gs_staging,
                gs_processed=gs_processed, gs_failed=gs_failed)
        status = "Processed csv file"
        return f" {filename} {status}"
