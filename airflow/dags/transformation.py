# Import packages
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
#from airflow.providers.google.cloud.operators.bigquery import .BigQueryCheckOperator
from google.oauth2 import service_account
import os
from params import google_cloud_path, staging_dataset, project_id, model_path, vectorizer_path
import pandas as pd
import talib as tb
from airflow.decorators import task
import pandas_gbq
from google.cloud import bigquery
import os
from extract import recent_date

import datetime
import pickle
from sentiment_model.datapreprocessor import DataPreprocessor
from sentiment_model.dictmodel import DictionaryModel
from sentiment_model.backtest import Backtest

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

    def run_sentiment_model(dict_df):
        '''
        Sentiment Model to score News Data
        '''
        from_year = datetime.datetime.now().year # can change based on scheduler

        print(f"===== Running Hawkish-Dovish Index Model =====".title())
        batch_id = datetime.date.today().strftime("%y%m%d")

        # Preprocessing
        datapreprecessor = DataPreprocessor(dict_df, batch_id)

        bt = Backtest(datapreprecessor.data, from_year)
        bt_based = bt.predict()

        dict_based = DictionaryModel(bt_based, from_year)
        fomc_df = dict_based.predict()

        print(f"===== Modelling Process Completed =====".title())

        return fomc_df

    @task()
    def transform_prices_to_ta(project_id, dataset_name, target_table_name, destination_table_name):
        '''
        Transform Prices to TA Indicators
        '''
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
        data['Date'] = pd.to_datetime(data['Date'])
        result = data[data['Date'] >= recent_date]
        return pandas_gbq.to_gbq(result, f'{dataset_name}.{destination_table_name}', project_id=f'{project_id}', credentials=credentials, if_exists='replace')    
    
    @task(task_id='transform_esg_score')
    def transform_esg_score(project_id, dataset_name, target_table_name, destination_table_name):
        '''
        Transform ESG Score 
        '''
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

        df_final = query_job.result().to_dataframe()
        df_final = df_final.iloc[: , 1:]
        #sti_tickers = scrape_sti_tickers()
        #tickers_list = [*sti_tickers.keys()]
        df_final['newMarketCap'] = df_final['previousClose'] * df_final['sharesOutstanding']
        total_index_mcap = df_final['newMarketCap'].sum()
        df_final['marketWeight'] = (df_final['newMarketCap']/total_index_mcap)*100
        #empty df
        final_esg_df = pd.DataFrame()
        #getting list of sectors in index
        sector_list = df_final['sector'].unique().tolist()

        #looping over each sector and apply .mean to calculate average
        for sector in sector_list:
            sector_df = df_final[df_final['sector'] == sector]
            sector_df['socialScore'].fillna(round(sector_df['socialScore'].mean(),2), inplace=True)
            sector_df['governanceScore'].fillna(round(sector_df['governanceScore'].mean(),2), inplace=True)
            sector_df['totalEsg'].fillna(round(sector_df['totalEsg'].mean(),2), inplace=True)
            sector_df['environmentScore'].fillna(round(sector_df['environmentScore'].mean(),2), inplace=True)

            final_esg_df = final_esg_df.append(sector_df)

        #also adding the weighted average columns into this new final_esg_df
        final_esg_df['mktweightedEsg'] = (final_esg_df['marketWeight'] * final_esg_df['totalEsg'])/100
        final_esg_df['mktweightedEnvScore'] = (final_esg_df['marketWeight'] * final_esg_df['environmentScore'])/100
        final_esg_df['mktweightedSocScore'] = (final_esg_df['marketWeight'] * final_esg_df['socialScore'])/100
        final_esg_df['mktweightedGovScore'] = (final_esg_df['marketWeight'] * final_esg_df['governanceScore'])/100
        
        final_esg_df = final_esg_df.reset_index(drop=True)
        credentials = service_account.Credentials.from_service_account_file(
                        google_cloud_path,
                    )
        return pandas_gbq.to_gbq(final_esg_df, f'{dataset_name}.{destination_table_name}', project_id=f'{project_id}', credentials=credentials, if_exists='replace')    
    
    @task()
    def transform_fomc_to_sentiment(project_id, dataset_name, target_table_name_1, target_table_name_2, destination_table_name):
        '''
        Generate Sentiment Scores from FOMC Data
        '''
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_path
        # Get client - Statements
        client = get_client(project_id, '')
        query_job = client.query(
            f"""
            SELECT
            *
            FROM `{project_id}.{dataset_name}.{target_table_name_1}`
            """
        )
        df_1 = query_job.result().to_dataframe() #.set_index('date')

        # Get client - Minutes
        query_job = client.query(
            f"""
            SELECT
            *
            FROM `{project_id}.{dataset_name}.{target_table_name_2}`
            """
        )
        df_2 = query_job.result().to_dataframe() #.set_index('date')

        # Transformation tasks
        dict_df = {
            "statements": df_1,
            "minutes": df_2,    
        }
        # load in ML model and vectorizer
        f = open(model_path, "rb")
        model = pickle.load(f)
        dict_df['model'] = model

        f = open(vectorizer_path, "rb")
        vectorizer = pickle.load(f)
        dict_df['vectorizer'] = vectorizer
        
        # run sentiment model
        fomc_df = run_sentiment_model(dict_df)
        fomc_df['Date'] = pd.to_datetime(fomc_df['Date'])

        credentials = service_account.Credentials.from_service_account_file(
                        google_cloud_path,
                    )

        return pandas_gbq.to_gbq(fomc_df, f'{dataset_name}.{destination_table_name}', project_id=f'{project_id}', credentials=credentials, if_exists='replace')

    
    
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
            'staging_destination_dataset': staging_dataset,
            "recent_date": (recent_date).strftime("%Y-%m-%d")
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

    transform_fear_greed_index = BigQueryExecuteQueryOperator(
        task_id = 'transform_fear_greed_index',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_source_dataset': staging_dataset,
            'staging_destination_dataset': staging_dataset
        },
        sql = './sql/S_FEAR_GREED_INDEX.sql'
    )
    
    transform_news_sources = BigQueryExecuteQueryOperator(
        task_id = 'transform_news_sources',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_source_dataset': staging_dataset,
            'staging_destination_dataset': staging_dataset
        },
        sql = './sql/S_NEWS_SOURCES.sql'
    )

    transform_news_volume_spikes = BigQueryExecuteQueryOperator(
        task_id = 'transform_news_volume_spikes',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_source_dataset': staging_dataset,
            'staging_destination_dataset': staging_dataset
        },
        sql = './sql/S_NEWS_VOL_SPIKES.sql'
    )

    
    transform_prices_to_ta_ = transform_prices_to_ta(project_id, staging_dataset, 'PRICE_STAGING', 'S_ALL_TA')
    transform_esg = transform_esg_score(project_id, staging_dataset, 'ESG_SCORE_STAGING', 'S_ESG_SCORE')
    transform_fomc_to_sentiment = transform_fomc_to_sentiment(project_id, staging_dataset, 'FOMC_STATEMENT_STAGING', 'FOMC_MINUTES_STAGING', 'S_FOMC')