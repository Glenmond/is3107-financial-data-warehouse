from requests import get
import pandas as pd
import yfinance as yf
import talib as tb
from airflow.decorators import task, task_group
from google.cloud import storage
import os
from params import google_cloud_path
from datetime import datetime
from dateutil.relativedelta import relativedelta


def get_adj_close(tickers, start_date, end_date):
    returns_df = pd.DataFrame(columns=['Ticker', 'High', 'Low', 'Open', 'Close', 'Volume', 'Adj Close'])
    cannot_find = []
    for ticker in tickers:
        # retrieve stock data (includes Date, OHLC, Volume, Adjusted Close)
        try:
          # ADd interval = w / m to change timeframe
          s = yf.download(ticker, start_date, end_date)
          s['Ticker'] = ticker
          returns_df = returns_df.append(s)
        except Exception as e:
          print(e)
          cannot_find.append(ticker)
    print(cannot_find)
    return returns_df

def array_to_df(values, ticker, ta, subta=None, blob_exists=True):
    df = pd.DataFrame(values, columns=['Value'])
    df = df.dropna()
    df['Ticker'] = ticker
    df['TA'] = ta
    df['TA_2'] = subta
    df = df.rename_axis('Date').reset_index()
    return df

def blob_exists(bucket_name, filename):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    return blob.exists()

def extract_data_task_group():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    #TODO Task to check if BigQuery tables exists
    @task()
    def check_if_gcs_bucket_exists(bucket_name):
        """
        #### Check Task
        Check if bucket in Google Cloud Storage Exists for intermediate data dump
        """
        client = storage.Client()
        if client.bucket(bucket_name).exists():
            return
        else:
            client.bucket(bucket_name).create()
            return 

    @task()
    def scrape_sti_tickers():
        sti = pd.read_html('https://en.wikipedia.org/wiki/Straits_Times_Index', match='List of STI constituents')[0]
        sti['Stock Symbol'] = sti['Stock Symbol'].apply(lambda x: x.split(" ")[1] + ".SI" )
        return sti.set_index('Stock Symbol').to_dict()['Company']

    @task(task_id='extract_stock_prices')
    def extract_stock_prices(sti_tickers, bucket_name):
        """
        #### Extract prices
        Extract Stock prices in the STI Universe
        """
        tickers_list = [*sti_tickers.keys()]
        if blob_exists(bucket_name, 'prices.csv'):
            prices_df = get_adj_close(tickers_list, start_date=(datetime.today() - relativedelta(months=2)).strftime("%Y-%m-%d"), end_date=(datetime.today()).strftime("%Y-%m-%d"))
        else:    
            prices_df = get_adj_close(tickers_list, start_date=(datetime.today() - relativedelta(months=12)).strftime("%Y-%m-%d"), end_date=(datetime.today()).strftime("%Y-%m-%d"))
        prices_df['Stock'] = prices_df['Ticker'].map(sti_tickers)
        return prices_df.rename_axis('Date').reset_index()

    @task(task_id='extract_stock_info')
    def extract_stock_info(sti_tickers, bucket_name):
        if blob_exists(bucket_name, 'stock_info.csv') is False or (datetime.today().day == 1 and datetime.today().month in [1, 4, 7, 10]):
            tickers_list = [*sti_tickers.keys()]
            info_df = None
            for i in tickers_list:
                # Stock Info
                try:
                    stock = yf.Ticker(i)
                    stock_info = stock.info
                    stock_info_df = pd.DataFrame([[sti_tickers[i], i, 'SG', stock_info['industry'], stock_info['longBusinessSummary']]], columns=['Stock', 'Ticker', 'Market', 'Industry', 'Summary'])
                    if info_df is None:
                        info_df = stock_info_df
                    else:
                        info_df = pd.concat([info_df, stock_info_df])
                except Exception as e:
                    continue
            return info_df.reset_index(drop=True)
        else:
            return None

    @task(task_id='extract_stock_fundamentals')
    def extract_stock_fundamentals(sti_tickers, bucket_name):
        if blob_exists(bucket_name, 'stock_fundamentals.csv') is False or (datetime.today().day == 1 and datetime.today().month in [1, 4, 7, 10]):
            tickers_list = [*sti_tickers.keys()]
            fun_df = None
            fund_list = ['Total Revenue', 'Total Assets', 'Cash', 'Total Current Liabilities', 'Total Liab', 'Net Income', 'Total Stockholder Equity', 'Common Stock', 'Treasury Stock']
            for i in tickers_list:
                # Stock Fundamentals (Balance Sheet + Financials)
                stock = yf.Ticker(i)
                fundamentals_df = pd.concat([stock.quarterly_balance_sheet.T, stock.quarterly_financials.T], axis=1)
                for fund in fund_list:
                    if fund not in fundamentals_df.columns:
                        fundamentals_df[fund] = None
                final_fundamentals_df = fundamentals_df[['Total Revenue', 'Total Assets', 'Cash', 'Total Current Liabilities', 'Total Liab', 'Net Income', 'Total Stockholder Equity', 'Common Stock', 'Treasury Stock']]
                final_fundamentals_df['Ticker'] = i
                final_fundamentals_df['Stock'] = sti_tickers[i]
                if fun_df is None:
                    fun_df = final_fundamentals_df
                else:
                    fun_df = pd.concat([fun_df, final_fundamentals_df])
            return fun_df.rename_axis('Date').reset_index()
        else:
            return None

    @task(task_id='extract_stock_dividends')
    def extract_stock_dividends(sti_tickers):
        tickers_list = [*sti_tickers.keys()]
        div_df = None
        for i in tickers_list:
            # Dividends 
            stock = yf.Ticker(i)
            dividend_df = pd.DataFrame(stock.dividends)
            dividend_df['Ticker'] = i
            dividend_df['Stock'] = sti_tickers[i]
            if div_df is None:
                div_df = dividend_df
            else:
                dividend_df = pd.concat([div_df, dividend_df])
        return dividend_df.rename_axis('Date').reset_index()

    @task(task_id='extract_commodities')
    def extract_commodities(bucket_name):
        commodities_mapping = {
            'CL=F': 'Crude Oil',
            'BZ=F': 'Brent Oil',
            'ZC=F': 'Corn',
            'ZS=F': 'Soybean',
            'ZR=F': 'Rice',
            'KC=F': 'Coffee',
            'GC=F': 'Gold',
            'SI=F': 'Silver',
            'HG=F': 'Copper',
            'SB=F': 'Sugar'
        }
        commodities_list = [*commodities_mapping.keys()]
        if blob_exists(bucket_name, 'commodities.csv'):
            commodities_df = get_adj_close(commodities_list, start_date=(datetime.today() - relativedelta(months=2)).strftime("%Y-%m-%d"), end_date=(datetime.today()).strftime("%Y-%m-%d"))
        else:    
            commodities_df = get_adj_close(commodities_list, start_date=(datetime.today() - relativedelta(months=12)).strftime("%Y-%m-%d"), end_date=(datetime.today()).strftime("%Y-%m-%d"))
        commodities_df['Commodities'] = commodities_df['Ticker'].map(commodities_mapping)
        return commodities_df.rename_axis('Date').reset_index()

    @task(task_id='extract_exchange_rates')
    def extract_exchange_rates(bucket_name):
        exchange_rate_mapping = {
            'SGDUSD=X': 'SGD/USD',
            'SGDCNY=X': 'SGD/CNY',
            'SGDEUR=X': 'SGD/EUR',
            'SGDJPY=X': 'SGD/JPY',
            'SGDGBP=X': 'SGD/GBP'
        }
        exchange_rate_list = [*exchange_rate_mapping.keys()]
        if blob_exists(bucket_name, 'exchange_rates.csv'):
            exchange_rate_df = get_adj_close(exchange_rate_list, start_date=(datetime.today() - relativedelta(months=2)).strftime("%Y-%m-%d"), end_date=(datetime.today()).strftime("%Y-%m-%d"))
        else:    
            exchange_rate_df = get_adj_close(exchange_rate_list, start_date=(datetime.today() - relativedelta(months=12)).strftime("%Y-%m-%d"), end_date=(datetime.today()).strftime("%Y-%m-%d"))
        exchange_rate_df['Commodities'] = exchange_rate_df['Ticker'].map(exchange_rate_mapping)
        #sgd_sgd = pd.DataFrame(columns=['Ticker', 'High', 'Low', 'Open', 'Close', 'Volume', 'Adj Close'])
        return exchange_rate_df.rename_axis('Date').reset_index()

    @task(task_id='extract_sg_ir')
    def extract_sg_ir(bucket_name):
        if blob_exists(bucket_name, 'sg_ir.csv'):
            start_date = (datetime.today()- relativedelta(months=6)).strftime("%Y-%m-%d") 
            end_date = datetime.today().strftime("%Y-%m-%d") 
        else:    
            start_date = (datetime.today()- relativedelta(months=24)).strftime("%Y-%m-%d") 
            end_date = datetime.today().strftime("%Y-%m-%d") 
        url = f'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&between[end_of_day]={start_date},{end_date}'
        headers = {'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1.1; SM-G928X Build/LMY47X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36'}
        response = get(url, headers=headers)
        sg_interest_rate = pd.DataFrame.from_dict(response.json()['result']['records'])
        sg_interest_rate = sg_interest_rate.rename(columns={'end_of_day': 'Date'})
        sg_interest_rate['Date']= pd.to_datetime(sg_interest_rate['Date'])
        return sg_interest_rate

    @task(task_id='extract_us_yields')
    def extract_us_yields(bucket_name):
        yields_mapping = {
            '^TNX': 'US Treasury Yield 10 Years',
            '^TYX': 'US Treasury Yield 30 Years',
            '^FVX': 'US Treasury Yield 5 Years',
        }
        yields_list = [*yields_mapping.keys()]
        if blob_exists(bucket_name, 'commodities.csv'):
            yields_df = get_adj_close(yields_list, start_date=(datetime.today() - relativedelta(months=2)).strftime("%Y-%m-%d"), end_date=(datetime.today()).strftime("%Y-%m-%d"))
        else:    
            yields_df = get_adj_close(yields_list, start_date=(datetime.today() - relativedelta(months=12)).strftime("%Y-%m-%d"), end_date=(datetime.today()).strftime("%Y-%m-%d"))
        yields_df['Treasury_Yield'] = yields_df['Ticker'].map(yields_mapping)
        return yields_df.rename_axis('Date').reset_index()

    @task(task_id='extract_stock_ta')
    def transform_prices_ta(sti_tickers, bucket_name, df):
        """
        #### Transform task for prices
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        df = df.set_index('Date')
        blobExists = blob_exists(bucket_name, 'ta_prices.csv')
        data = pd.DataFrame(columns=['Date', 'Stock', 'TA', 'TA_2', 'Value'])
        tickers_list = [*sti_tickers.keys()]
        for i in tickers_list:
            try:
                df_ticker = df[df['Ticker'] == i].dropna()
                open, high, low, close, vol, adj_close = df_ticker['Open'], df_ticker['High'], df_ticker['Low'], df_ticker['Close'], df_ticker['Volume'], df_ticker['Adj Close']
                # SMA
                data = data.append(array_to_df(tb.SMA(adj_close, timeperiod=30), i, 'SMA', blobExists))
                # EMA
                data = data.append(array_to_df(tb.EMA(adj_close, timeperiod=30), i, 'EMA', blobExists))
                # WMA
                data = data.append(array_to_df(tb.WMA(adj_close, timeperiod=30), i, 'WMA', blobExists))
                # Bollinger Bands
                lower, middle, upper = tb.BBANDS(adj_close, timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
                data = data.append(array_to_df(lower, i, 'BBANDS', 'Lower Band', blobExists))
                data = data.append(array_to_df(lower, i, 'BBANDS', 'Middle Band', blobExists))
                data = data.append(array_to_df(lower, i, 'BBANDS', 'Upper Band', blobExists))
                # Momentum Indicators
                # MACD
                macd, macdsignal, macdhist = tb.MACD(adj_close, fastperiod=12, slowperiod=26, signalperiod=9)
                data = data.append(array_to_df(macd, i, 'MACD', 'MACD', blobExists))
                data = data.append(array_to_df(macdsignal, i, 'MACD', 'MACD Signal', blobExists))
                data = data.append(array_to_df(macdhist, i, 'MACD', 'MACD Historical', blobExists))
                # STOCH
                slowk, slowd = tb.STOCH(high, low, adj_close, fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
                data = data.append(array_to_df(slowk, i, 'STOCH', 'SlowK', blobExists))
                data = data.append(array_to_df(slowd, i, 'STOCH', 'SlowD', blobExists))   
                #RSI
                data = data.append(array_to_df(tb.RSI(adj_close, timeperiod=14), i, 'RSI', blobExists))
                # Volume Indicators
                # AD
                real = tb.AD(high, low, adj_close, vol)
                data = data.append(array_to_df(real, i, 'AD', blobExists))
                # ADOSC
                real = tb.ADOSC(high, low, adj_close, vol, fastperiod=3, slowperiod=10)
                data = data.append(array_to_df(real, i, 'ADOSC', blobExists))
                # OBV
                real = tb.OBV(adj_close, vol)
                data = data.append(array_to_df(real, i, 'OBV', blobExists))
                # Volatility Indicators
                # ATR
                real = tb.ATR(high, low, adj_close, timeperiod=14)
                data = data.append(array_to_df(real, i, 'ATR', blobExists))
                # NATR
                real = tb.NATR(high, low, adj_close, timeperiod=14)
                data = data.append(array_to_df(real, i, 'NATR', blobExists))
                # TRANGE
                real = tb.TRANGE(high, low, adj_close)
                data = data.append(array_to_df(real, i, 'TRANGE', blobExists))
            except Exception as e:
                print(e)
                print(i)
        data['Stock'] = data['Ticker'].map(sti_tickers)
        data = data.reset_index(drop=True)
        return data

    # Run Tasks 
    bucket = 'is3107_bucket_test'
    # Set Credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_path
    # Check if Bucket Exists
    check_bucket = check_if_gcs_bucket_exists(bucket)
    # Scrape STI Tickers
    tickers = scrape_sti_tickers()
    # Stock Prices 
    price_df = extract_stock_prices(tickers, bucket_name=bucket)
    # Stock Info, Fundamentals and Div
    stock_info_df = extract_stock_info(tickers, bucket_name=bucket)
    stock_fundamentals_df = extract_stock_fundamentals(tickers, bucket_name=bucket)
    stock_dividends_df = extract_stock_dividends(tickers)
    # SG Exchange Rates
    sg_exchange_rates = extract_exchange_rates(bucket_name=bucket)
    # Commodities
    commodities_df = extract_commodities(bucket_name=bucket) 
    # SG Interest Rates
    sg_ir_df = extract_sg_ir(bucket_name='is3107_bucket_test')
    # US Yields
    us_yields = extract_us_yields(bucket_name='is3107_bucket_test')
    # TA for Prices
    ta_data = transform_prices_ta(tickers, bucket, price_df)
    check_bucket >> [price_df, sg_exchange_rates, commodities_df, sg_ir_df, us_yields, stock_info_df, stock_fundamentals_df, stock_dividends_df] 
    tickers >> price_df >> ta_data
