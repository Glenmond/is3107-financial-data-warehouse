import re
import threading
import pickle
import os
import pandas as pd

from abc import ABCMeta, abstractmethod

class FomcBase(metaclass=ABCMeta):
    def __init__(self, content_type, verbose, max_threads, base_dir):
        # Set arguments to internal variables
        self.content_type = content_type
        self.verbose = verbose
        self.MAX_THREADS = max_threads
        self.base_dir = base_dir

        # Initialization
        self.df = None
        self.links = None
        self.dates = None
        self.articles = None
        self.speakers = None
        self.titles = None

        # FOMC website URLs
        self.base_url = 'https://www.federalreserve.gov'
        self.calendar_url = self.base_url + '/monetarypolicy/fomccalendars.htm'

        # FOMC Chairperson's list
        self.chair = pd.DataFrame(
            data=[["Greenspan", "Alan", "1987-08-11", "2006-01-31"], 
                  ["Bernanke", "Ben", "2006-02-01", "2014-01-31"], 
                  ["Yellen", "Janet", "2014-02-03", "2018-02-03"],
                  ["Powell", "Jerome", "2018-02-05", "2022-02-05"]],
            columns=["Surname", "FirstName", "FromDate", "ToDate"])
        
    def _date_from_link(self, link):
        date = re.findall('[0-9]{8}', link)[0]

        if date[4] == '0':
            date = "{}-{}-{}".format(date[:4], date[5:6], date[6:])
        else:
            date = "{}-{}-{}".format(date[:4], date[4:6], date[6:])

        return date

    def _speaker_from_date(self, article_date):
        if self.chair.FromDate[0] < article_date and article_date < self.chair.ToDate[0]:
            speaker = self.chair.FirstName[0] + " " + self.chair.Surname[0]
        elif self.chair.FromDate[1] < article_date and article_date < self.chair.ToDate[1]:
            speaker = self.chair.FirstName[1] + " " + self.chair.Surname[1]
        elif self.chair.FromDate[2] < article_date and article_date < self.chair.ToDate[2]:
            speaker = self.chair.FirstName[2] + " " + self.chair.Surname[2]
        elif self.chair.FromDate[3] < article_date and article_date < self.chair.ToDate[3]:
            speaker = self.chair.FirstName[3] + " " + self.chair.Surname[3]
        else:
            speaker = "other"
        return speaker

    def _get_articles_multi_threaded(self):
        '''
        Get all articles using multi-threading
        '''
        if self.verbose:
            print("Getting articles - Multi-threaded...")

        self.articles = ['']*len(self.links)
        jobs = []
        # initiate and start threads:
        index = 0
        while index < len(self.links):
            if len(jobs) < self.MAX_THREADS:
                t = threading.Thread(target=self._add_article, args=(self.links[index],index,))
                jobs.append(t)
                t.start()
                index += 1
            else:    # wait for threads to complete and join them back into the main thread
                t = jobs.pop(0)
                t.join()
        for t in jobs:
            t.join()

    @abstractmethod
    def _get_links(self, from_year):
        '''
        Sets all the links for the FOMC meetings
        from the giving from_year to the current most recent year
        from_year is min(2015, from_year)
        '''
        # Implement in sub classes
        pass
    
    @abstractmethod
    def _add_article(self, link, index=None):
        '''
        Adds the related article for 1 link into the instance variable
        index is the index in the article to add to. 
        '''
        # Implement in sub classes
        pass

    def get_contents(self, from_year=1990):
        '''
        Returns a df with the date as the index.
        Save the same to internal df as well.
        '''
        self._get_links(from_year)
        self._get_articles_multi_threaded()
        dict = {
            'date': self.dates,
            'contents': self.articles,
            'speaker': self.speakers, 
            'title': self.titles
        }
        self.df = pd.DataFrame(dict).sort_values(by=['date'])
        self.df.reset_index(drop=True, inplace=True)
        return self.df

    def pickle_dump_df(self, filename="output.pickle"):
        '''
        Dump df to a pickle file
        '''
        filepath = self.base_dir + filename
        print("")
        if self.verbose:
            print("Writing to ", filepath)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "wb") as output_file:
            pickle.dump(self.df, output_file)