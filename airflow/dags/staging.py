from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Define dag variables
project_id = 'test-344015'
staging_dataset = 'TEST_DWH_STAGING'
dwh_dataset = 'TEST_DWH'
gs_bucket = 'is3107_bucket_test'


def gcs_to_staging_task_group():
    # Staging To DWH 
    load_ta = GCSToBigQueryOperator(
        task_id = 'load_ta',
        bucket = gs_bucket,
        source_objects = ['ta_prices.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.TA_STAGING',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        skip_leading_rows = 1
    )

    load_commodities = GCSToBigQueryOperator(
        task_id = 'load_commodities',
        bucket = gs_bucket,
        source_objects = ['commodities.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.COMMODITIES_STAGING',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        skip_leading_rows = 1
    )

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

    load_us_yields = GCSToBigQueryOperator(
        task_id = 'load_us_yields',
        bucket = gs_bucket,
        source_objects = ['us_yields.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.US_YIELDS_STAGING',
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