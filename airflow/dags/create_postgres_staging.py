from airflow.providers.postgres.operators.postgres import PostgresOperator


from datetime import datetime

default_args={
    "start_date":datetime(2021,1,1)
}

def create_pg_staging():

    tables_ids = {
        'S_PG_ALL_PRICES': 'prices',
        'S_PG_EXCHANGE_RATE': 'exchange_rates',
        'S_PG_SG_IR': 'sg_ir',
        'S_PG_STOCK_INFO': 'stock_info',
        'S_PG_STOCK_FUNDAMENTALS': 'stock_fundamentals',
        'S_PG_STOCK_DIVIDEND': 'stock_dividends',
        'S_PG_FEAR_GREED_INDEX': 'fear_greed_index',
        'S_PG_FOMC_MINUTES':'fomc_minutes',
        'S_PG_FOMC_STATEMENT':'fomc_statement',
        'S_PG_NEWS_SOURCES':'news_sources',
        'S_PG_NEWS_VOL_SPIKES':'news_volume_spikes',
    }

    create_schema = PostgresOperator(task_id=f"create_schema", postgres_conn_id="postgres_local", sql="CREATE SCHEMA IF NOT EXISTS staging;")

    for key, value in tables_ids.items():
        create_task = PostgresOperator(task_id=f"create_{key}", postgres_conn_id="postgres_local", sql=f"psql/{key}.sql")
        truncate_task = PostgresOperator(task_id=f'truncate_{key}', postgres_conn_id='postgres_local',sql=f"TRUNCATE TABLE staging.{key}")
        create_schema.set_downstream(create_task)
        create_task.set_downstream(truncate_task)
