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
    from news_data.News import News
except ModuleNotFoundError: 
    from .fomc_data.FomcStatement import FomcStatement
    from .fomc_data.FomcMinutes import FomcMinutes
    from .news_data.News import News

batch_id = datetime.date.today().strftime("%y%m%d")
fred_api = "18fb1a5955cab2aae08b90a2ff0f6e42"
fred = Fred(api_key=fred_api)

def download_data(docs, from_year):
    if docs.content_type == "news":
        news_sources_df, news_volume_spikes_df = docs.get_contents(from_year)
        print("Shape of the news sources downloaded data: ", news_sources_df.shape)
        print("The first 5 rows of the data: \n", news_sources_df.head())
        print("The last 5 rows of the data: \n", news_sources_df.tail())

        print("Shape of the news volumes spikes downloaded data: ", news_volume_spikes_df.shape)
        print("The first 5 rows of the data: \n", news_volume_spikes_df.head())
        print("The last 5 rows of the data: \n", news_volume_spikes_df.tail())

        docs.pickle_dump_df(f"{batch_id}_{docs.content_type}_sources" + ".pickle", "news_sources_df")
        docs.pickle_dump_df(f"{batch_id}_{docs.content_type}_volume_spikes" + ".pickle", "news_volume_spikes")

    else:
        df = docs.get_contents(from_year)
        print("Shape of the downloaded data: ", df.shape)
        print("The first 5 rows of the data: \n", df.head())
        print("The last 5 rows of the data: \n", df.tail())
        docs.pickle_dump_df(filename=f"{batch_id}_{docs.content_type}" + ".pickle")


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
    content_type_all = ("statement", "minutes", "news", "all")

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
        news = News(base_dir=base_dir)
        download_data(news, from_year)
        
    else:
        if content_type == "statement":
            docs = FomcStatement()
        elif content_type == "minutes":
            docs = FomcMinutes()
        else:  # News
            docs = News()
        download_data(docs, from_year)
    print("Sentiment data imported")

if __name__ == "__main__":

    pg_name = sys.argv[0]
    args = sys.argv[1:]
    content_type_all = ('statement', 'minutes', 'news', 'all')

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