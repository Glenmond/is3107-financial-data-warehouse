import re
import pandas as pd
import numpy as np

from .config import negate, lmdict
from pandas.tseries.offsets import MonthEnd
from functools import reduce

class DictionaryModel:
    def __init__(self, data, start_dt):
        self.data = data  # dictionary of dataframes
        self.start_dt = start_dt
        self.docs = ["statements", "minutes"]

    def predict(self):
        """
        Predicting results for dataframe
        """
        for name in self.docs:
            df = self.classify(name)
            self.data[name] = df
            df.to_csv(f"{name}_df_dict_model.csv")
        return self.combine_df()
        
    def negated(self, word):
        """
        Checking for negated words
        """
        if word.lower() in negate:
            return True
        else:
            return False

    def tone_counter(self, dict, article):
        """
        Calculating word meaning into counts based on dictionary
        """
        pos_count = 0
        neg_count = 0
        hawk_count = 0
        dov_count = 0

        pos_words = []
        neg_words = []
        hawk_words = []
        dov_words = []

        input_words = re.findall(
            r"\b([a-zA-Z]+n\'t|[a-zA-Z]+\'s|[a-zA-Z]+)\b", article.lower()
        )

        word_count = len(input_words)

        for i in range(0, word_count):
            if input_words[i] in dict["Negative"]:
                neg_count += 1
                neg_words.append(input_words[i])

            if input_words[i] in dict["Positive"]:
                if i >= 3:
                    if (
                        self.negated(input_words[i - 1])
                        or self.negated(input_words[i - 2])
                        or self.negated(input_words[i - 3])
                    ):
                        neg_count += 1
                        neg_words.append(input_words[i] + " (with negation)")
                    else:
                        pos_count += 1
                        pos_words.append(input_words[i])
                elif i == 2:
                    if self.negated(input_words[i - 1]) or self.negated(
                        input_words[i - 2]
                    ):
                        neg_count += 1
                        neg_words.append(input_words[i] + " (with negation)")
                    else:
                        pos_count += 1
                        pos_words.append(input_words[i])
                elif i == 1:
                    if self.negated(input_words[i - 1]):
                        neg_count += 1
                        neg_words.append(input_words[i] + " (with negation)")
                    else:
                        pos_count += 1
                        pos_words.append(input_words[i])
                elif i == 0:
                    pos_count += 1
                    pos_words.append(input_words[i])

            if input_words[i] in dict["Hawkish"]:
                hawk_count += 1
                hawk_words.append(input_words[i])

            if input_words[i] in dict["Dovish"]:
                dov_count += 1
                dov_words.append(input_words[i])

        results = [
            word_count,
            pos_count,
            neg_count,
            hawk_count,
            dov_count,
            pos_words,
            neg_words,
            hawk_words,
            dov_words,
        ]

        return results

    def sentiment_calculation(self, temp):
        """
        Take in a list of values (ie, [wordcount, NPostiiveWords, NNegativeWords, NHawkishWords, NDovishWords])
        Return a classification value
        Rule Based Heuristic Approach
        1. Hawkish + Positive = Hawkish --> global economic growth continues to improve // improving wages
        2. Hawkish + Negative = Dovish --> global economy is the weakest for many years // declining wages
        3. Dovish + Positive = Dovish --> growing uncertainty // increasing slowdown
        4. Dovish + Negative = Hawkish --> decrease in uncertainty // decreasing slowdown

        """
        NPositiveWords = temp[1]
        NNegativeWords = temp[2]
        NHawkishWords = temp[3]
        NDovishWords = temp[4]

        if NHawkishWords == 0 & NDovishWords == 0:
            return 0

        elif NHawkishWords > NDovishWords:  # more hawkish
            if NPositiveWords >= NNegativeWords:
                return 1
            else:
                return -1
        else:  # NHawkishWords < NDovishWords, more dovish
            if NPositiveWords >= NNegativeWords:
                return -1
            else:
                return 1

    def aggregate_score(self, series):  # series == sentences
        """
        Aggregate sentences to document level scoring
        """
        score = []
        series_count = 0  # to check only the Hawkish vs Dovish
        for i in series:
            if i == 0:
                continue
            else:
                score.append(i)
                series_count = series_count + 1

        if series_count == 0:  # means all neutral sentiment
            return 0

        final_score = sum(score) / series_count
        return final_score

    def classify(self, name):
        """
        Utilised model to classify the results
        """
        print(f"===== predicting {name} using dictionary-based model =====".title())

        data = self.data[name]
        data["Score DB"] = 0

        sentence_mapping = {"news": "lemmatizedSentences", "statements" : "lemmatizedSentencesDB", "minutes": "lemmatizedSentencesDB"}
        col = sentence_mapping[name] # column name
        
        for index, row in data.iterrows():
            # ignore rows that are empty
            if len(row[col]) == 0:
                continue

            df = pd.DataFrame(row[col], columns=["Corpus"])
            df["Date"] = row["date"].strftime("%Y-%m-%d")
            df["Class"] = 0  # Hawkish = +1, Dovish=-1

            # initializing dataframe columns
            df["wordcount"] = 0
            df["NPositiveWords"] = 0
            df["NNegativeWords"] = 0
            df["NHawkishWords"] = 0
            df["NDovishWords"] = 0
            df = df[
                [
                    "Date",
                    "Corpus",
                    "Class",
                    "wordcount",
                    "NPositiveWords",
                    "NNegativeWords",
                    "NHawkishWords",
                    "NDovishWords",
                ]
            ]

            # remove rows with null values
            df["Corpus"].replace("", np.nan, inplace=True)
            df.dropna(subset=["Corpus"], inplace=True)
            df.reset_index(drop=True, inplace=True)

            for index2, row2 in df.iterrows():

                temp = self.tone_counter(lmdict, row2["Corpus"])
                df.loc[index2, "wordcount"] = temp[0]
                df.loc[index2, "NPositiveWords"] = temp[1]
                df.loc[index2, "NNegativeWords"] = temp[2]
                df.loc[index2, "NHawkishWords"] = temp[3]
                df.loc[index2, "NDovishWords"] = temp[4]
                df.loc[index2, "Class"] = self.sentiment_calculation(temp)

            # manipulate the data then aggregate the value back into the original dataframe
            score = self.aggregate_score(df["Class"])
            data.loc[index, "Score DB"] = score
        
        data.reset_index(inplace=True, drop=True)
        data = self.scale(data)
        return data

    def scale(self, df):
        """
        To scale the dataframe's score values
        """
        # Get Hawkish and Dovish Max
        max_hawk = df["Score DB"].max()
        min_dov = df["Score DB"].min()

        # Calculate the Ratio Level
        df["Scaled Score DB"] = df["Score DB"].apply(
            lambda x: x / max_hawk if x > 0 else -(x / min_dov)
        )
        return df

    # def update(self, curr_df, name):
    #     """
    #     Update historical dataframe with latest results and override with original dataframe
    #     Rules:
    #     1. Update maximum up to 1 year from the latest data in existing dataframe
    #     """
    #     # get last index of the curr dataframe
    #     old_df = self.data["historical"][name]

    #     start_dt = curr_df["date"].iloc[0]  # start date of new df
    #     hist_end_dt = old_df["date"].iloc[
    #         -12
    #     ]  # historical end date, maximum override 12 rows

    #     if start_dt < hist_end_dt:
    #         curr_df = curr_df[(curr_df["date"] >= hist_end_dt)]
    #         old_df = old_df[(old_df["date"] < hist_end_dt)]
    #     else:  # start_dt >= hist_end_dt
    #         old_df = old_df[(old_df["date"] < start_dt)]
        
    #     # concat values
    #     df = pd.concat([old_df, curr_df])
    #     df.reset_index(inplace=True, drop=True)

    #     # ensure missing NaN value is updated
    #     df.update(self.data["historical"][name])
    #     df = self.scale(df)

    #     # self.save_df(name, df)

    #     self.data["historical"][name] = df  # replace the historical dictionary
    
    def combine_df(self):
        """
        To combine and finalise all df results
        """
        st_df = self.data["statements"]
        mins_df = self.data["minutes"]

        st_df['date'] = st_df['date'] + MonthEnd(0)
        mins_df['date'] = mins_df['date'] + MonthEnd(0)

        st_df = st_df[['date', 'Scaled Score DB']]
        mins_df = mins_df[['date', 'Scaled Score DB']]

        # st_df = st_df[['date', 'Scaled Score', 'Scaled Score DB']]
        # mins_df = mins_df[['date', 'Scaled Score', 'Scaled Score DB']]

        # rename columns
        st_df.rename(columns = {"date": "Date", "Scaled Score DB" : "Score_Statement_DB"}, inplace=True)
        mins_df.rename(columns = {"date": "Date", "Scaled Score DB" : "Score_Minutes_DB"}, inplace=True)

        # st_df.rename(columns = {"date": "Date", "Scaled Score" : "Score_Statement_ML", "Scaled Score DB" : "Score_Statement_DB"}, inplace=True)
        # mins_df.rename(columns = {"date": "Date", "Scaled Score" : "Score_Minutes_ML", "Scaled Score DB" : "Score_Minutes_DB"}, inplace=True)


        dfs = [mins_df, st_df]
        df_final = reduce(lambda left,right: pd.merge(left,right,on='Date', how="left"), dfs)
        
        # applying ffill() method to fill the missing values
        df_final = df_final.ffill(axis = 0)
        df_final = df_final.bfill(axis = 0)

        # remove duplicates (eg, 2020-03 have 4 FOMC meetings, hence we aim to get the mean score)
        duplicate = df_final[df_final.duplicated('Date', keep=False)]
        duplicate = duplicate.groupby(['Date'], as_index=False).agg({'Score_Minutes_DB': 'mean', 'Score_Statement_DB': 'mean',})
        # duplicate = duplicate.groupby(['Date'], as_index=False).agg({'Score_Minutes_ML': 'mean', 'Score_Minutes_DB': 'mean', 'Score_Statement_ML': 'mean', 'Score_Statement_DB': 'mean',})
        df_final.drop_duplicates(subset=['Date'], keep=False, inplace=True)
        df_final = pd.concat([df_final, duplicate], ignore_index=True)
        df_final.sort_values(by=['Date'], inplace=True)

        return df_final
    
    
    def save_df(self, name, df):
        """
        Save and replace df to historical folder as pickle
        """
        print(f"===== save {name} to historical pickle =====".title())
        rename_dict = {"statements": "st", "minutes": "mins", "news": "news", "final": "final"}
        df.to_pickle(f"{self.path}/{rename_dict[name]}_df.pickle")