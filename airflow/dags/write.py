from asyncore import write
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from google.cloud import storage
from params import google_cloud_path
import os


def write_to_gcs_task_group():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    def write_to_gcs(**kwargs):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """
        try:
            data = kwargs['ti'].xcom_pull(task_ids=kwargs['extract_task_id'])
            if data is None:
                return
            client = storage.Client()
            bucket = client.get_bucket(kwargs['bucket_name'])
            csv_name = kwargs['csv_name']
            bucket.blob(f'{csv_name}.csv').upload_from_string(data.to_csv(), 'text/csv')
        except Exception as e:
            print(e)
            print(kwargs)
            return
            
    bucket = 'is3107_bucket_test'
    # Laod to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_path

    tasks_ids_csvs = {
        'extract_all_prices': 'prices',
        #'extract_all_ta': 'ta_prices',
        'extract_exchange_rates': 'exchange_rates',
        'extract_sg_ir': 'sg_ir',
        'extract_stock_info': 'stock_info',
        'extract_stock_fundamentals': 'stock_fundamentals',
        'extract_stock_dividends': 'stock_dividends'
    }
    for key, value in tasks_ids_csvs.items():
        PythonOperator(task_id=f'write_to_gcs_{value}', python_callable=write_to_gcs, op_kwargs={'extract_task_id': key, 'bucket_name': bucket, 'csv_name': value})