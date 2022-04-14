from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import logging
import os
from datetime import datetime
import pandas as pd

default_args={
    "start_date":datetime(2021,1,1)
}

def write_to_postgres():
    
    def load_to_table(**kwargs):
        task_id = kwargs['task_id']
        table_name = kwargs['table_name']
        
        data = kwargs['ti'].xcom_pull(dag_id='final_dag',task_ids=f"extract_{task_id}", include_prior_dates=True)
        
        data = pd.DataFrame(data)
        data.drop_duplicates(keep=False, inplace=True)

        print(data.head())
        
        if 'Date' in data.columns:
            data['Date'] = pd.to_datetime(data['Date'])
            data['Date'] = data['Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
            data.rename(columns={'Date':'Date_'}, inplace=True)

        os.makedirs("airflow/stock_data", exist_ok=True)
        data.to_csv(f"airflow/stock_data/{task_id}.csv", index=False)

        hook = PostgresHook(postgres_conn_id="postgres_local")
    
        with open(f"airflow/stock_data/{task_id}.csv", 'r+') as f:
            hook.copy_expert(f"COPY staging.{table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", f"airflow/stock_data/{task_id}.csv")
        return

    tables_ids = {
        'S_PG_ALL_PRICES': 'all_prices',
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

    for key, value in tables_ids.items(): 
        try:
            PythonOperator(task_id=f'load_psql_{value}', python_callable=load_to_table, op_kwargs={'table_name': key, 'task_id': value})
        except:
            logging.info(f"Error loading Postgresql table {key}")
        else:
            logging.info(f"Successfully loaded Postgresql table {key}")


    
    
    

    