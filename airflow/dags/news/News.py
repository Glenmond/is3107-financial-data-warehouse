import os
import json
import time
import requests
from datetime import datetime
import dateutil
import pandas as pd
import pickle
import numpy as np
# import datetime
from ravenpackapi import RPApi

apikey = RPApi(api_key='0KWrK3jwSACDor2JHZpVFP')

news_sources = {
    'D05.SI': "43DF4D8FA22B80ECE94E8729BEDEC6C4", # dbs_general
    'U11.SI': "FD5AA5E1DBD987CE34BED42728ADCCE4", # uob_general
    'O39.SI': "4B27B6E1B37DDBF8CA6604646B42B8DD", # ocbc_general
}

news_volume_spikes = {
    'D05.SI': "B25D42AF9042C76574B87E4ABDE66B81", # dbs_news_volume_spikes
    'U11.SI': "7698BF1230711675A6ED9A53A21514A5", # uob_news_volume_spikes
    'O39.SI': "7FF80DA8AFB3C36F7DCFA2BE2D0EC364" # ocbc_news_volume_spikes
}

class News():
    def __init__(self, spikes=True, base_dir='../data/sentiment_data/extract/'):
        # Set arguments to internal variables
        self.content_type = "news"
        self.spikes = spikes # include news vol
        self.base_dir = base_dir
        self.news_df = None
        self.start = None
        self.end = None

    def download_data(self, dataset_id, start_date, end_date):
        """
        Self-improvised methods from RavenPack API
        Limitation of API: only 1 year or 10,000 worth of datasets allowed per requests
        """

        ds = apikey.get_dataset(dataset_id)
        data = ds.json(
            start_date,
            end_date,
        )

        list_of_dict = []
        for record in data:
            list_of_dict.append(record)

        return pd.DataFrame.from_dict(list_of_dict, orient='columns')
    
    def get_contents(self, start_date, end_date):
        '''Sends and parses request/response to/from NYT Archive API for given dates.'''
        
        # Set arguments to internal variables
        self.start = start_date
        self.end = end_date

        if self.spikes != True: # news spikes not True
            print("Getting articles for news..." + 'Date range: ' + str(start_date) + ' to ' + str(end_date))

            ldf = []
            for k, v in news_sources.items():
                temp_df = self.download_data(v, self.start, self.end)
                temp_df['entity_name'] = k
                ldf.append(temp_df)
            news_sources_df = pd.concat(ldf, axis=0)
            news_sources_df.reset_index(inplace=True, drop=True)

            # Assign to attributes
            self.news_df = news_sources_df
        
        else:
            print("Getting articles for news volumes spikes..." + 'Date range: ' + str(start_date) + ' to ' + str(end_date))
            
            ldf = []
            for k, v in news_volume_spikes.items():
                temp_df = self.download_data(v, self.start, self.end)
                temp_df = temp_df[(temp_df['rp_entity_id']=="ROLLUP")]
                temp_df['entity_name'] = k
                ldf.append(temp_df)
            news_volume_spikes_df = pd.concat(ldf, axis=0)
            news_volume_spikes_df.reset_index(inplace=True, drop=True)
            
            # Assign to attributes
            self.news_df = news_volume_spikes_df

        # MAPPING
        return self.news_df

    
    def pickle_dump_df(self, filename="output.pickle", filetype="news_sources_df"):
        '''
        Dump df to a pickle file
        '''
        if filetype == "news_sources_df":
            df = self.news_sources_df
        else:
            df = self.news_volume_spikes_df


        filepath = self.base_dir + filename
        print("")
        print("Writing to ", filepath)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "wb") as output_file:
            pickle.dump(df, output_file)

        