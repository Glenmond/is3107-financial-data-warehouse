#import libraries to save the data from  to dataframe
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from bs4 import BeautifulSoup
import random
import requests

from datetime import datetime
import pandas as pd
import requests


####################################################
# 1. DEFINE PYTHON FUNCTIONS
####################################################

        
def _get_user_agent() -> str:
    """Get a random User-Agent strings from a list of some recent real browsers
    Parameters
    ----------
    None
    Returns
    -------
    str
        random User-Agent strings
    """
    user_agent_strings = [
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.10; rv:86.1) Gecko/20100101 Firefox/86.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:86.1) Gecko/20100101 Firefox/86.1",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:82.1) Gecko/20100101 Firefox/82.1",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.10; rv:83.0) Gecko/20100101 Firefox/83.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:84.0) Gecko/20100101 Firefox/84.0",
    ]

    return random.choice(user_agent_strings)

def _get_html(url: str):
    """Wraps HTTP requests.get for testibility.
    Fake the user agent by changing the User-Agent header of the request
    and bypass such User-Agent based blocking scripts used by websites.
    Parameters
    ----------
    url : str
        HTML page URL
    Returns
    -------
    str
        HTML page
    """
    return requests.get(url, headers={"User-Agent": _get_user_agent()}).text

#Transform scraped data into data frame
def split_text_to_dataframe(str1):
    tmp1 = str1.split(")")
    list1 = []
    for i in tmp1:
        list1.extend(i.split(":"))
    list2 = []
    for j in list1:
        list2.extend(j.split("("))
    list2 = list2[:-1]
    for j in range(len(list2)):
        list2[j] = list2[j].replace("Fear & Greed", "").strip()
    time_list = []
    value_list = []
    text_list = []
    for i in range(len(list2)):
        if i % 3 == 0: time_list.append(list2[i])
        elif i % 3 == 1: value_list.append(list2[i])
        else: text_list.append(list2[i])

    df = pd.DataFrame({"Time":time_list, "FG_Value":value_list, "FG_Textvalue":text_list})

    return df

#Scrape data from webpage
def scrape_fear_greed_index():
    """Scrapes CNN Fear and Greed Index HTML page
    Parameters
    ----------
    None
    Returns
    -------
    BeautifulSoup
        CNN Fear And Greed Index HTML page
    """
    return BeautifulSoup(
        _get_html("https://money.cnn.com/data/fear-and-greed/"),
        "lxml",
    )

def extract_fear_and_greed_index():
    print('1 Extract Fear and Greed Index by scraping CNN data')

    text_soup_cnn = scrape_fear_greed_index()
    # Fill in fear and greed index
    index_data = (
        text_soup_cnn.findAll("div", {"class": "modContent feargreed"})[0]
        .contents[0]
        .text
    )
    return split_text_to_dataframe(index_data)
    print('Completed')

def transform_fear_and_greed_index():
    print('2 Transform Fear and Greed Index into dataframe')

    transformed_df = extract_fear_and_greed_index()
    daily_data_df = transformed_df.iloc[0:1]
    today_date = datetime.today().strftime("%Y-%m-%d")
    daily_data_df.insert(0, "Date", today_date, True)
    prev_close = transformed_df._get_value(1, 'FG_Value')
    daily_data_df['FG_Close'] = [prev_close]
    prev_close_text = transformed_df._get_value(1, 'FG_Textvalue')
    daily_data_df['FG_Closetext'] = [prev_close_text]
    return daily_data_df
    print('Completed')

def load_fear_and_greed_index():
    print('3 Load Fear and Gread Index into databse by writing into csv')
    df = transform_fear_and_greed_index()
    df.to_csv('/home/airflow/fear_greed_data.csv', index = False)
    print('Completed')

############################################
#2. DEFINE AIRFLOW DAG (SETTINGS + SCHEDULE)
############################################

default_args = {
     'owner': 'YXLuo',
     'depends_on_past': False,
     'email': ['e0420174@u.nus.com'],
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1
    }

dag = DAG( 'fear_greed_daily_ETL',
            default_args=default_args,
            description='Extract fear and greed index daily',
            catchup=False, 
            start_date= datetime(2022, 3, 23), 
            schedule_interval= '0 * * * *'  
          )  

##########################################
#3. DEFINE AIRFLOW OPERATORS
##########################################


extract_fear_and_greed_index_task = PythonOperator(task_id = 'extract_fear_and_greed_index', 
                                   python_callable = extract_fear_and_greed_index, 
                                   provide_context = True,
                                   dag= dag )


transform_fear_and_greed_index_task = PythonOperator(task_id = 'transform_fear_and_greed_index', 
                                 python_callable = transform_fear_and_greed_index,
                                 provide_context = True,
                                 dag= dag)

load_fear_and_greed_index_task = PythonOperator(task_id = 'load_frear_and_greed_index', 
                                  python_callable = load_fear_and_greed_index,
                                  provide_context = True,
                                  dag= dag)      

##########################################
#4. DEFINE OPERATORS HIERARCHY
##########################################

extract_fear_and_greed_index_task  >> transform_fear_and_greed_index_task >> load_fear_and_greed_index_task

