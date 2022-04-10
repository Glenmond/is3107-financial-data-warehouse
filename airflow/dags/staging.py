from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.decorators import task
import os
from params import google_cloud_path, project_id, gs_bucket, staging_dataset
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import exceptions
import os
import logging
from params import google_cloud_path

def gcs_to_staging_task_group():

    def get_client(project_id, service_account):
        '''
        Get client object based on the project_id.
        Parameters:
            - project_id (str): id of project
            - service_account (str): path to a JSON service account. If the path
            is blanked, use application default authentication instead.
        '''
        if service_account:
            logging.info(f'Getting client from json file path {service_account}')
            credentials = service_account.Credentials.from_service_account_file(
                service_account)
            client = bigquery.Client(project_id, credentials = credentials)
        else:
            logging.info('Getting client from application default authentication')
            client = bigquery.Client(project_id)
        return client

    def create_dataset(dataset_id, client):
        '''
        Create dataset in a project
        Parameteres:
            - dataset_id (str): ID of dataset to be created
            - client (obj): client object
        '''

        try:
            dataset = client.get_dataset(dataset_id)
        except exceptions.NotFound:
            logging.info(f'Creating dataset {dataset_id}')
            client.create_dataset(dataset_id)
        else:
            logging.info(f'Dataset not created. {dataset_id} already exists.')

    @task()
    def check_staging_exists(project_id, dataset_name):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_path
        # Get client
        client = get_client(project_id, '')
        # Create datasets
        create_dataset(dataset_name, client)

    # Staging To DWH 

    load_prices = GCSToBigQueryOperator(
        task_id = 'load_prices',
        bucket = gs_bucket,
        source_objects = ['prices.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.PRICE_STAGING',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        skip_leading_rows = 1
    )

    load_exchange_rate = GCSToBigQueryOperator(
        task_id = 'load_exchange_rate',
        bucket = gs_bucket,
        source_objects = ['exchange_rates.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.EXCHANGE_RATE_STAGING',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        skip_leading_rows = 1
    )

    load_sg_ir = GCSToBigQueryOperator(
        task_id = 'load_sg_ir',
        bucket = gs_bucket,
        source_objects = ['sg_ir.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.SG_IR_STAGING',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        skip_leading_rows = 1
    )

    load_stock_info = GCSToBigQueryOperator(
        task_id = 'load_stock_info',
        bucket = gs_bucket,
        source_objects = ['stock_info.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.STOCK_INFO_STAGING',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        skip_leading_rows = 1,
        schema_fields=[
            {'name': 'Index', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Stock', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Ticker', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Market', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Industry', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Summary', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    )

    load_stock_fundamentals = GCSToBigQueryOperator(
        task_id = 'load_stock_fundamentals',
        bucket = gs_bucket,
        source_objects = ['stock_fundamentals.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.STOCK_FUNDAMENTALS_STAGING',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        skip_leading_rows = 1
    )

    load_stock_dividends = GCSToBigQueryOperator(
        task_id = 'load_stock_dividends',
        bucket = gs_bucket,
        source_objects = ['stock_dividends.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.STOCK_DIVIDENDS_STAGING',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        skip_leading_rows = 1
    )

    load_fear_greed_index = GCSToBigQueryOperator(
        task_id = 'load_fear_greed_index',
        bucket = gs_bucket,
        source_objects = ['fear_greed_index.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.FEAR_GREED_INDEX_STAGING',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        skip_leading_rows = 1
    )

    load_esg_score = GCSToBigQueryOperator(
        task_id = 'load_esg_score',
        bucket = gs_bucket,
        source_objects = ['esg_score.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.ESG_SCORE_STAGING',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        skip_leading_rows = 1
    )


    staging_exists = check_staging_exists(project_id, staging_dataset)
    staging_exists >> [load_prices, load_sg_ir, load_exchange_rate, load_stock_info, load_stock_fundamentals, load_stock_dividends, load_fear_greed_index, load_esg_score]