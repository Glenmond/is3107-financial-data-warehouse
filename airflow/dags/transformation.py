# Import packages
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
#from airflow.providers.google.cloud.operators.bigquery import .BigQueryCheckOperator
from google.oauth2 import service_account
import os
from params import google_cloud_path, staging_dataset, project_id
import pandas as pd
import talib as tb
from airflow.decorators import task
import pandas_gbq
from google.cloud import bigquery
import os

def transform_task_group():
    def get_client(project_id, service_account):
        '''
        Get client object based on the project_id.
        Parameters:
            - project_id (str): id of project
            - service_account (str): path to a JSON service account. If the path
            is blanked, use application default authentication instead.
        '''
        if service_account:
            credentials = service_account.Credentials.from_service_account_file(
                service_account)
            client = bigquery.Client(project_id, credentials = credentials)
        else:
            client = bigquery.Client(project_id)
        return client

    
    def array_to_df(values, ticker, ta, subta=None):
        df = pd.DataFrame(values, columns=['Value'])
        df = df.dropna()
        df['Ticker_id'] = ticker
        df['TA_id'] = ta
        df['TA_description'] = subta
        df = df.rename_axis('Date').reset_index()
        return df

    def scrape_sti_tickers():
        sti = pd.read_html('https://en.wikipedia.org/wiki/Straits_Times_Index', match='List of STI constituents')[0]
        sti['Stock Symbol'] = sti['Stock Symbol'].apply(lambda x: x.split(" ")[1] + ".SI" )
        return sti.set_index('Stock Symbol').to_dict()['Company']

    @task()
    def transform_prices_to_ta(project_id, dataset_name, target_table_name, destination_table_name):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_path
        # Get client
        client = get_client(project_id, '')
        query_job = client.query(
            f"""
            SELECT
            *
            FROM `{project_id}.{dataset_name}.{target_table_name}`
            """
        )
        df = query_job.result().to_dataframe().set_index('Date')
        sti_tickers = scrape_sti_tickers()
        data = pd.DataFrame(columns=['Date', 'Ticker_id', 'Name_id', 'TA_id', 'TA_description', 'Value'])
        tickers_list = [*sti_tickers.keys()]
        for i in tickers_list:
            try:
                df_ticker = df[df['Ticker'] == i].dropna()
                open, high, low, close, vol, adj_close = df_ticker['Open'], df_ticker['High'], df_ticker['Low'], df_ticker['Close'], df_ticker['Volume'], df_ticker['Adj_Close']
                # SMA
                data = data.append(array_to_df(tb.SMA(adj_close, timeperiod=30), i, 'SMA'))
                # EMA
                data = data.append(array_to_df(tb.EMA(adj_close, timeperiod=30), i, 'EMA'))
                # WMA
                data = data.append(array_to_df(tb.WMA(adj_close, timeperiod=30), i, 'WMA'))
                # Bollinger Bands
                lower, middle, upper = tb.BBANDS(adj_close, timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
                data = data.append(array_to_df(lower, i, 'BBANDS', 'Lower Band'))
                data = data.append(array_to_df(lower, i, 'BBANDS', 'Middle Band'))
                data = data.append(array_to_df(lower, i, 'BBANDS', 'Upper Band'))
                # Momentum Indicators
                # MACD
                macd, macdsignal, macdhist = tb.MACD(adj_close, fastperiod=12, slowperiod=26, signalperiod=9)
                data = data.append(array_to_df(macd, i, 'MACD', 'MACD'))
                data = data.append(array_to_df(macdsignal, i, 'MACD', 'MACD Signal'))
                data = data.append(array_to_df(macdhist, i, 'MACD', 'MACD Historical'))
                # STOCH
                slowk, slowd = tb.STOCH(high, low, adj_close, fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
                data = data.append(array_to_df(slowk, i, 'STOCH', 'SlowK'))
                data = data.append(array_to_df(slowd, i, 'STOCH', 'SlowD'))   
                #RSI
                data = data.append(array_to_df(tb.RSI(adj_close, timeperiod=14), i, 'RSI'))
                # Volume Indicators
                # AD
                real = tb.AD(high, low, adj_close, vol)
                data = data.append(array_to_df(real, i, 'AD'))
                # ADOSC
                real = tb.ADOSC(high, low, adj_close, vol, fastperiod=3, slowperiod=10)
                data = data.append(array_to_df(real, i, 'ADOSC'))
                # OBV
                real = tb.OBV(adj_close, vol)
                data = data.append(array_to_df(real, i, 'OBV'))
                # Volatility Indicators
                # ATR
                real = tb.ATR(high, low, adj_close, timeperiod=14)
                data = data.append(array_to_df(real, i, 'ATR'))
                # NATR
                real = tb.NATR(high, low, adj_close, timeperiod=14)
                data = data.append(array_to_df(real, i, 'NATR'))
                # TRANGE
                real = tb.TRANGE(high, low, adj_close)
                data = data.append(array_to_df(real, i, 'TRANGE'))
            except Exception as e:
                print(e)
                print(i)
        data['Name_id'] = data['Ticker_id'].map(sti_tickers)
        data = data.reset_index(drop=True)
        credentials = service_account.Credentials.from_service_account_file(
                        google_cloud_path,
                    )
        return pandas_gbq.to_gbq(data, f'{dataset_name}.{destination_table_name}', project_id=f'{project_id}', credentials=credentials, if_exists='replace')    

    # Transform Big Query
    # Create remaining dimensions data
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_path
    transform_exchange_rate = BigQueryExecuteQueryOperator(
        task_id = 'transform_exchange_rate',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_source_dataset': staging_dataset,
            'staging_destination_dataset': staging_dataset
        },
        sql = './sql/S_EXCHANGE_RATE.sql'
    )

    transform_all_price = BigQueryExecuteQueryOperator(
        task_id = 'transform_all_price',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_source_dataset': staging_dataset,
            'staging_destination_dataset': staging_dataset
        },
        sql = './sql/S_ALL_PRICE.sql'
    )

    transform_stock_fundamentals = BigQueryExecuteQueryOperator(
        task_id = 'transform_stock_fundamentals',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_source_dataset': staging_dataset,
            'staging_destination_dataset': staging_dataset
        },
        sql = './sql/S_STOCK_FUNDAMENTALS.sql'
    )

    transform_stock_info = BigQueryExecuteQueryOperator(
        task_id = 'transform_stock_info',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_source_dataset': staging_dataset,
            'staging_destination_dataset': staging_dataset
        },
        sql = './sql/S_STOCK_INFO.sql'
    )

    transform_stock_dividends = BigQueryExecuteQueryOperator(
        task_id = 'transform_stock_dividends',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_source_dataset': staging_dataset,
            'staging_destination_dataset': staging_dataset
        },
        sql = './sql/S_STOCK_DIVIDENDS.sql'
    )

    transform_sg_ir = BigQueryExecuteQueryOperator(
        task_id = 'transform_sg_ir',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_source_dataset': staging_dataset,
            'staging_destination_dataset': staging_dataset
        },
        sql = './sql/S_SG_IR.sql'
    )
    
    transform_prices_to_ta_ = transform_prices_to_ta(project_id, staging_dataset, 'PRICE_STAGING', 'S_ALL_TA')
