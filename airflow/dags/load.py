# Import packages
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.decorators import task
import os
from params import google_cloud_path

# Define dag variables
project_id = 'test-344015'
staging_dataset = 'TEST_DWH_STAGING'
dwh_dataset = 'TEST_DWH'
gs_bucket = 'is3107_bucket_test'

import staging_schema as ss
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import exceptions
import os
import argparse
import logging
from params import google_cloud_path
logging.basicConfig(level=logging.INFO)

def load_dwh_task_group():
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

    def create_table(table_id, schema, dataset_id, client):
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        try:
            table = client.create_table(table)
        except exceptions.Conflict:
            logging.info(f'Table not created. {table_id} already exists')
        else:
            logging.info(f'Created table {dataset_id}.{table_id}')

    @task()
    def check_dwh_tables_exists(project_id, dataset_name):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_path
        # Get client
        client = get_client(project_id, '')
        # Create datasets
        create_dataset(dataset_name, client)
        # Create staging tables
        for table_id in ss.tables:
            schema = eval('ss.'+table_id)
            create_table('D_' + table_id, schema, dataset_name, client)
        return

    # Operators

    distinct_all_prices = BigQueryExecuteQueryOperator(
        task_id='distinct_all_prices',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        sql=f'''
        INSERT `{project_id}.{dwh_dataset}.D_ALL_PRICE` 
        SELECT DISTINCT *
        FROM
        `{project_id}.{staging_dataset}.S_ALL_PRICE`
        '''
    )

    distinct_exchange_rate = BigQueryExecuteQueryOperator(
        task_id='distinct_exchange_rate',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        sql=f'''
        INSERT `{project_id}.{dwh_dataset}.D_EXCHANGE_RATE` 
        SELECT DISTINCT *
        FROM
        `{project_id}.{staging_dataset}.S_EXCHANGE_RATE`
        '''
    )

    distinct_sg_ir = BigQueryExecuteQueryOperator(
        task_id='distinct_sg_ir',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        sql=f'''
        INSERT `{project_id}.{dwh_dataset}.D_SG_IR` 
        SELECT DISTINCT *
        FROM
        `{project_id}.{staging_dataset}.S_SG_IR`
        '''
    )

    distinct_stock_dividends = BigQueryExecuteQueryOperator(
        task_id='distinct_stock_dividends',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        sql=f'''
        INSERT `{project_id}.{dwh_dataset}.D_STOCK_DIVIDENDS`
        SELECT DISTINCT *
        FROM
        `{project_id}.{staging_dataset}.S_STOCK_DIVIDENDS`
        '''
    )
    
    distinct_stock_fundamentals = BigQueryExecuteQueryOperator(
        task_id='distinct_stock_fundamentals',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        sql=f'''
        INSERT `{project_id}.{dwh_dataset}.D_STOCK_FUNDAMENTALS`
        SELECT DISTINCT *
        FROM
        `{project_id}.{staging_dataset}.S_STOCK_FUNDAMENTALS`
        '''
    )

    distinct_stock_info = BigQueryExecuteQueryOperator(
        task_id='distinct_stock_info',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        sql=f'''
        CREATE OR REPLACE TABLE `{project_id}.{dwh_dataset}.D_STOCK_INFO` AS
        SELECT DISTINCT *
        FROM
        `{project_id}.{staging_dataset}.S_STOCK_INFO`
        '''
    )

    distinct_all_ta = BigQueryExecuteQueryOperator(
        task_id='distinct_all_ta',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        sql=f'''
        INSERT `{project_id}.{dwh_dataset}.D_ALL_TA`
        SELECT DISTINCT *
        FROM
        `{project_id}.{staging_dataset}.S_ALL_TA`
        '''
    )

    check_dwh_exists = check_dwh_tables_exists(project_id, dwh_dataset)
    check_dwh_exists >> [distinct_all_prices, distinct_all_ta, distinct_exchange_rate, distinct_sg_ir, distinct_stock_dividends, distinct_stock_fundamentals, distinct_stock_info]