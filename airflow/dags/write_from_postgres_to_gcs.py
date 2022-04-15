from asyncore import write
from airflow.operators.python import PythonOperator
from google.cloud import storage
from params import google_cloud_path, gs_bucket
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
import os
import csv
import logging


def write_from_postgres_to_gcs_task_group():
   
    def write_to_gcs_from_postgres(**kwargs):
        
    # Laod to GCS
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_path

        tables_ids = {
            'S_PG_ALL_PRICES': 'all_prices',
            'S_PG_EXCHANGE_RATE': 'exchange_rates',
            'S_PG_SG_IR': 'sg_ir',
            'S_PG_STOCK_INFO': 'stock_info',
            'S_PG_STOCK_FUNDAMENTALS': 'stock_fundamentals',
            'S_PG_STOCK_DIVIDEND': 'stock_dividends',
            'S_PG_FOMC_MINUTES': 'fomc_minutes',
            'S_PG_FOMC_STATEMENT': 'fomc_statement',
            'S_PG_NEWS_SOURCES': 'news_sources',
            'S_PG_NEWS_VOL_SPIKES': 'news_volumes_spikes',
            'S_PG_FEAR_GREED_INDEX': 'fear_greed_index',
            'S_PG_ESG_SCORE': 'esg_score'
        }
        GCS_BUCKET = gs_bucket
        for key, value in tables_ids.items():
            SQL_QUERY = f"SELECT * FROM staging.{key};"
            FILENAME = f"{key}"
            PostgresToGCSOperator(task_id=f"load_pg_{value}_to_gcs", sql=SQL_QUERY, bucket=GCS_BUCKET, filename=FILENAME, gzip=False)