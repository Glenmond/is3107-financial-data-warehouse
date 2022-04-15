from asyncore import write
from airflow.operators.python import PythonOperator
from google.cloud import storage
from params import google_cloud_path, gs_bucket
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
import psycopg2

from tempfile import NamedTemporaryFile

import os
import csv
import logging
import psycopg2 as pg
import pandas as pd


def write_from_postgres_to_gcs_task_group():

    def write_from_psql_to_gcs(**kwargs):
        csv_name = kwargs['csv_name']
        bucket_name = kwargs['bucket_name']
        table = kwargs['table']
        connect_str = "dbname='postgres_db' user='postgres_local' host='localhost' " "password='airflow' port = 5432"
        conn = psycopg2.connect(connect_str)
        cur = conn.cursor()
        conn.autocommit = True
        cur.execute(f"SELECT * FROM staging.{table};")
        data = pd.DataFrame(cur.fetchall())
        cur.execute(f"SELECT column_name FROM information_schema.columns where table_schema='staging' and table_name='{table}';")
        cols = list(cur.fetchall())
        clean_cols = []
        for i in cols:
            if i[0][-1] == "_":
                name = i[0][:-1]
            else:
                name = i[0]
            clean_cols.append(name)

        data.columns = clean_cols
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
    
        bucket.blob(f'{csv_name}.csv').upload_from_string(data.to_csv(), 'text/csv')

    # Load to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_path

    tables_ids = {
        'S_PG_ALL_PRICES': 'prices',
        'S_PG_EXCHANGE_RATE': 'exchange_rates',
        'S_PG_SG_IR': 'sg_ir',
        'S_PG_STOCK_INFO': 'stock_info',
        #'S_PG_STOCK_FUNDAMENTALS': 'stock_fundamentals',
        'S_PG_STOCK_DIVIDEND': 'stock_dividends',
        #'S_PG_FOMC_MINUTES': 'fomc_minutes',
        #'S_PG_FOMC_STATEMENT': 'fomc_statement',
        'S_PG_NEWS_SOURCES': 'news_sources',
        'S_PG_NEWS_VOL_SPIKES': 'news_volumes_spikes',
        'S_PG_FEAR_GREED_INDEX': 'fear_greed_index',
        #'S_PG_ESG_SCORE': 'esg_score'
    }
    
    GCS_BUCKET = gs_bucket
    for key, value in tables_ids.items():
        SQL_QUERY = f"SELECT * FROM staging.{key};"
        FILENAME = f"{value}.csv"
        PostgresToGCSOperator(task_id=f"load_pg_{value}_to_gcs",
                               sql=SQL_QUERY,
                               bucket=GCS_BUCKET,
                               filename=FILENAME,
                               postgres_conn_id='postgres_local',
                               gzip=False)
    
