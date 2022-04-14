from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime

default_args={
    "start_date":datetime(2021,1,1)
}

def transform_pg():

    tables_ids = {
        'T_PG_ALL_PRICES': 'prices',
        'T_PG_EXCHANGE_RATE': 'exchange_rates',
        'T_PG_SG_IR': 'sg_ir',
        'T_PG_STOCK_INFO': 'stock_info',
        #'T_PG_STOCK_FUNDAMENTALS': 'stock_fundamentals',
        'T_PG_STOCK_DIVIDEND': 'stock_dividends',
        #'T_PG_FOMC_MINUTES': 'fomc_minutes',
        #'T_PG_FOMC_STATEMENT': 'fomc_statement',
        'T_PG_NEWS_SOURCES': 'news_sources',
        'T_PG_NEWS_VOL_SPIKES': 'news_volumes_spikes',
        'T_PG_FEAR_GREED_INDEX': 'fear_greed_index',
        #'T_PG_ESG_SCORE': 'esg_score'
    }
    for key, value in tables_ids.items():
        PostgresOperator(task_id=f"transform_pg_{value}", postgres_conn_id="postgres_local", sql=f"psql/{key}.sql")
