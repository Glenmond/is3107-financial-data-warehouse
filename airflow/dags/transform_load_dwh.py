# Import packages
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
#from airflow.providers.google.cloud.operators.bigquery import .BigQueryCheckOperator
import os
from params import google_cloud_path

# Define dag variables
project_id = 'test-344015'
staging_dataset = 'TEST_DWH_STAGING'
dwh_dataset = 'TEST_DWH'
gs_bucket = 'is3107_bucket_test'

def transform_load_dwh_task_group():
    # Transform Big Query
    # Create remaining dimensions data
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_path
    create_d_exchange_rate = BigQueryExecuteQueryOperator(
        task_id = 'create_d_exchange_rate',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql = './sql/D_EXCHANGE_RATE.sql'
    )

    create_d_stock_price = BigQueryExecuteQueryOperator(
        task_id = 'create_d_stock_price',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql = './sql/D_STOCK_PRICE.sql'
    )

    create_d_commodities = BigQueryExecuteQueryOperator(
        task_id = 'create_d_commodities',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql = './sql/D_COMMODITIES.sql'
    )

    create_d_stock_ta = BigQueryExecuteQueryOperator(
        task_id = 'create_d_stock_ta',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql = './sql/D_STOCK_TA.sql'
    )

    create_d_us_yields = BigQueryExecuteQueryOperator(
        task_id = 'create_d_us_yields',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql = './sql/D_US_YIELDS.sql'
    )

    create_d_stock_fundamentals = BigQueryExecuteQueryOperator(
        task_id = 'create_d_stock_fundamentals',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql = './sql/D_STOCK_FUNDAMENTALS.sql'
    )

    create_d_stock_info = BigQueryExecuteQueryOperator(
        task_id = 'create_d_stock_info',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql = './sql/D_STOCK_INFO.sql'
    )

    create_d_sg_ir = BigQueryExecuteQueryOperator(
        task_id = 'create_d_sg_ir',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql = './sql/D_SG_IR.sql'
    )