import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

from bs4 import BeautifulSoup
import requests
import re
from fredapi import Fred
import sys

try:
    from fomc_data.FomcStatement import FomcStatement
    from fomc_data.FomcMinutes import FomcMinutes

except ModuleNotFoundError: 
    from .fomc_data.FomcStatement import FomcStatement
    from .fomc_data.FomcMinutes import FomcMinutes

batch_id = datetime.date.today().strftime("%y%m%d")
fred_api = "18fb1a5955cab2aae08b90a2ff0f6e42"
fred = Fred(api_key=fred_api)

def fetch_data(ref_date = datetime.datetime.today()):
    '''
    Fetches data from FRED API, stores it in a dataframe. 
    Takes as input a datetime of the latest day within the dataframe. 
    Default value for this is today's date.
    '''    
    # concurrent
    commodities_params = { 
        'observation_start':'2003-01-02',
        'observation_end':ref_date.strftime('%Y-%m-%d'),
        'units':'lin',
        'frequency':'m',
        'aggregation_method': 'eop',   
    }
    commodities = ('PPIACO', commodities_params)

    # one period lag 
    # one period lag means observation starts and ends one period later
    one_month_lag = ref_date + relativedelta(months=1)
    real_gdp_params = { 
        'observation_start':'2003-01-02',
        'observation_end':one_month_lag.strftime('%Y-%m-%d'),
        'units':'lin',
        'frequency':'q',
        'aggregation_method': 'eop',   
    }
    real_gdp = ('GDPC1', real_gdp_params)

    # two period lag
    # two period lag means observation starts and ends two periods later since we have to shift forward
    two_month_lag = ref_date + relativedelta(months=2)
    median_cpi_params ={ 
        'observation_start':'2003-01-02',
        'observation_end':two_month_lag.strftime('%Y-%m-%d'),
        'units':'lin',
        'frequency':'m',
        'aggregation_method': 'eop',   
    }
    median_cpi = ('MEDCPIM158SFRBCLE', median_cpi_params)

    # three period lag
    # three period lag means observation starts and ends three periods later since we have to shift forward
    three_month_lag = ref_date + relativedelta(months=3)
    em_ratio_params = { 
        'observation_start':'2003-01-02',
        'observation_end':three_month_lag.strftime('%Y-%m-%d'),
        'units':'lin',
        'frequency':'m',
        'aggregation_method': 'eop',   
    }
    em_ratio = ('EMRATIO', em_ratio_params)

    # five period lag
    five_month_lag = ref_date + relativedelta(months=5)
    med_wages_params = { 
        'observation_start':'2003-01-02',
        'observation_end': five_month_lag.strftime('%Y-%m-%d'),
        'units':'lin',
        'frequency':'q',
        'aggregation_method': 'eop',   
    }
    med_wages = ('LES1252881600Q', med_wages_params)

    # 5 period lead
    # five period lead means observation starts and ends five periods earlier since we have to shift backward
    five_month_lead = ref_date - relativedelta(months=5)
    maturity_minus_three_month_params = {
        'observation_start':'2002-08-02', 
        'observation_end':five_month_lead.strftime('%Y-%m-%d'),
        'units':'lin',
        'frequency':'m',
        'aggregation_method': 'eop',
    }
    maturity_minus_three_month = ('T10Y3M', maturity_minus_three_month_params)

    indicators = [
        commodities,
        real_gdp,
        median_cpi,
        em_ratio,
        med_wages,
        maturity_minus_three_month,
    ]

    df = pd.DataFrame()

    # target value
    fed_fund_date = ref_date-relativedelta(months=1) # get the latest available end-of-month data for this
    fed_fund_rate = fred.get_series(
        "DFF",
        **{
            "observation_start": "2003-01-02", 
            "observation_end": fed_fund_date.strftime('%Y-%m-%d'), 
            "frequency": "m",
            "aggregation_method": "eop",
        }
    )

    fed_fund_rate.index = pd.to_datetime(fed_fund_rate.index).to_period("M")
    df["target"] = fed_fund_rate.to_numpy()
    df.index = fed_fund_rate.index
    for series_id, params in indicators:
        # Get the data from FRED, convert to pandas DataFrame
        indicator = fred.get_series(series_id, **params)
        indicator = indicator.to_frame().set_axis([series_id], axis="columns")
        # fill in data with '0.0' that is presented as just '.'
        indicator[series_id] = ["0.0" if x == "." else x for x in indicator[series_id]]
        # turn the value into numeric
        indicator[series_id] = pd.to_numeric(indicator[series_id])
        indicator.index = pd.to_datetime(indicator.index).to_period("M")
        indicator = indicator.resample("M").interpolate()

        if series_id in ("PAYEMS", "T5YIE", "GDPC1"):  # align 1 lag
            indicator = indicator.shift(-1)[:-1]

        if series_id in ("MEDCPIM158SFRBCLE"):  # align 2 lag
            indicator = indicator.shift(-2)[:-2]
            indicator.rename(columns={"MEDCPIM158SFRBCLE": "MEDCPI"}, inplace=True)

        if series_id in ("EMRATIO"):  # align 3 lag
            indicator = indicator.shift(-3)[:-3]

        if series_id in ("LES1252881600Q"):  # align 5 lag
            indicator = indicator.shift(-5)[:-5]
            indicator.rename(columns={"LES1252881600Q": "MEDWAGES"}, inplace=True)

        if series_id in ("PERMIT", "AMTMNO", "DGORDER", "T10Y3M"):  # align 5 lead
            indicator = indicator.shift(5)[5:]

        # join the dataframes together
        df = pd.concat([indicator, df], axis="columns")
    return df


def download_data(docs, from_year):
    df = docs.get_contents(from_year)
    print("Shape of the downloaded data: ", df.shape)
    print("The first 5 rows of the data: \n", df.head())
    print("The last 5 rows of the data: \n", df.tail())
    docs.pickle_dump_df(filename=f"{batch_id}_{docs.content_type}" + ".pickle")


def download_fed_futures_historical(path):
    # Futures: full historical + forward data
    # download from barchart
    full_path = path + "/raw_data/historical-prices.csv"
    futures = pd.read_csv(full_path)
    futures = futures[:-1]
    futures['Exp Date'] = pd.to_datetime(futures['Exp Date'])
    futures = futures.set_index("Exp Date")    
    return futures

def download_fomc_dates():
        page = requests.get("https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm")
        soup = BeautifulSoup(page.content, 'html.parser')
        soupy = soup.find_all("div", class_="panel panel-default")
        result = []
        for s in soupy:
            year = s.find("div", class_ = "panel-heading").get_text()
            year = re.findall(r'\d+', year)[0]

            year_meetings = s.find_all("div", class_=["row fomc-meeting","fomc-meeting--shaded row fomc-meeting"])
            for meeting in year_meetings:
                meeting_month = meeting.find("div", class_="fomc-meeting__month").get_text()
                meeting_day = meeting.find("div", class_="fomc-meeting__date").get_text()

                if re.search(r"\(([^)]+)", meeting_day):
                    continue
                meeting_month = meeting_month.split("/")[0]
                meeting_day = meeting_day.split("-")[0]

                try: 
                    d = datetime.datetime.strptime(f"{meeting_day} {meeting_month} {year}", '%d %B %Y')
                except:
                    d = datetime.datetime.strptime(f"{meeting_day} {meeting_month} {year}", '%d %b %Y')
                result.append(d.strftime('%Y-%m-%d'))
        result.sort()
        df = pd.DataFrame(result)
        df.columns = ['Date']
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index("Date")

        return df

def import_sentiment_data(from_year, content_type='all', base_dir='../data/sentiment_data/extract/'):
    content_type_all = ("statement", "minutes", "all")

    if content_type not in content_type_all:
        print("Please specify the first argument from ", content_type_all)
        return

    if (from_year < 1980) or (from_year > 2022):
        print("Please specify the second argument between 1980 and 2022")
        return 

    if content_type == "all":
        fomc = FomcStatement(base_dir=base_dir)
        download_data(fomc, from_year)
        fomc = FomcMinutes(base_dir=base_dir)
        download_data(fomc, from_year)
        
    else:
        if content_type == "statement":
            docs = FomcStatement()
        elif content_type == "minutes":
            docs = FomcMinutes()

        download_data(docs, from_year)
    print("Sentiment data imported")

if __name__ == "__main__":

    pg_name = sys.argv[0]
    args = sys.argv[1:]
    content_type_all = ('statement', 'minutes', 'all')

    if (len(args) != 1) and (len(args) != 2):
        print("Usage: ", pg_name)
        print("Please specify the first argument from ", content_type_all)
        print("You can add from_year (yyyy) as the second argument.")
        print("\n You specified: ", ','.join(args))
        sys.exit(1)

    if len(args) == 1:
        from_year = 1990
    else:
        from_year = int(args[1])
    
    content_type = args[0].lower()
    if content_type not in content_type_all:
        print("Usage: ", pg_name)
        print("Please specify the first argument from ", content_type_all)
        sys.exit(1)

    if (from_year < 1980) or (from_year > 2022):
        print("Usage: ", pg_name)
        print("Please specify the second argument between 1980 and 2022")
        sys.exit(1)

    import_sentiment_data(from_year, content_type)