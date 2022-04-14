import pandas as pd
import pickle
import numpy as np
import os

from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn import metrics
from sklearn.dummy import DummyClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import LogisticRegression
from sklearn import svm, tree
from sklearn.ensemble import RandomForestClassifier, StackingClassifier
from sklearn.neighbors import KNeighborsClassifier

from xgboost import XGBClassifier

from sklearn import model_selection
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import RandomizedSearchCV

from sklearn import preprocessing
from sklearn.pipeline import Pipeline

from sklearn.preprocessing import LabelEncoder

class Backtest:
    def __init__(self, data, start_dt):
        self.data = data  # dictionary of dataframes
        self.fitted_model = data["model"]  # stacked model with svm as baseline
        self.fitted_vectorizer = data["vectorizer"]  # tfid vectorizer
        self.start_dt = start_dt
        self.docs = ["statements", "minutes"]

    def predict(self):
        """
        Predict dataset
        """
        for name in self.docs:
            df = self.classify(name)
            self.data[name] = df  # replace the historical dictionary
        return self.data

    def classify(self, name):
        """
        Utilised model to classify the results
        """
        print(f"===== predicting {name} using machine learning models =====".title())

        # Retrieve the dataframe from data dict
        data = self.data[name]

        data["Score"] = 0

        for index, row in data.iterrows():

            # Skip for empty rows
            if len(row["lemmatizedSentences"]) == 0:
                continue

            df = pd.DataFrame(row["lemmatizedSentences"], columns=["Corpus"])
            df["Date"] = row["date"].strftime("%Y-%m-%d")
            df["Class"] = 0  # Hawkish = +1, Dovish=-1
            df = df[["Date", "Corpus", "Class"]]

            df["Corpus"].replace(
                "", np.nan, inplace=True
            )  # remove rows with null values
            df.dropna(subset=["Corpus"], inplace=True)
            df.reset_index(drop=True, inplace=True)

            le = LabelEncoder()  # encode Class
            df["Class_Label"] = le.fit_transform(df["Class"])

            # Vectorise Corpus
            X_final_dtm = self.fitted_vectorizer.transform(df.Corpus)

            # Model Prediction
            result = self.fitted_model.predict(X_final_dtm)
            df["Predict"] = result

            # Aggregate the value back into the original dataframe
            NHawkishLabels = len(df[(df["Predict"] == 1)])
            NDovishLabels = len(df[(df["Predict"] == 0)])
            Score = (NHawkishLabels * 1 + NDovishLabels * -1) / (
                NHawkishLabels + NDovishLabels
            )
            data.loc[index, "Score"] = Score

        data.reset_index(inplace=True, drop=True)
        data = self.scale(data)
        return data

    def scale(self, df):
        """
        To scale the dataframe's score values
        """
        # Get Hawkish and Dovish Max
        max_hawk = df.Score.max()
        min_dov = df.Score.min()

        # Calculate the Ratio Level
        df["Scaled Score"] = df["Score"].apply(
            lambda x: x / max_hawk if x > 0 else -(x / min_dov)
        )

        return df

    def save_df(self, name, df):
        """
        Save and replace df to historical folder as pickle
        """
        print(f"===== save {name} to historical pickle =====".title())
        rename_dict = {"statements": "st", "minutes": "mins"}
        df.to_pickle(f"{self.path}/{rename_dict[name]}_df.pickle")