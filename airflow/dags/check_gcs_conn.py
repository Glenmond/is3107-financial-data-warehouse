from datetime import datetime
from params import google_cloud_path, gs_bucket
import os
from airflow.decorators import task
from google.cloud import storage
import io
import pandas as pd
from dateutil.relativedelta import relativedelta

default_args={
    "start_date":datetime(2021,1,1)
}
def blob_exists(bucket_name, filename):
    """
    Helper function to check if bucket exists in GCS
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    return blob.exists()

def blob_download_to_df(bucket_name, filename):
    """
    Retrieves CSV file from GCS
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    data = blob.download_as_string()
    df = pd.read_csv(io.BytesIO(data))
    return df

def check_gcs_conn():

    @task(task_id='check_conn')
    def check_if_gcs_bucket_exists(bucket_name):
        """
        Check if bucket in Google Cloud Storage Exists for intermediate data dump
        """
        try:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_path
            client = storage.Client()
        except:
            return False
        if not client.bucket(bucket_name).exists():
            client.bucket(bucket_name).create() 
        return True

    bucket = gs_bucket
    check_conn_bucket = check_if_gcs_bucket_exists(bucket)
