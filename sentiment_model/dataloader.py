import pandas as pd
import pickle
import datetime

batch_id = datetime.date.today().strftime("%y%m%d")

class DataLoader:
    def __init__(self, start_dt, path):
        self.start_dt = start_dt
        self.directory = {
            "statements": f"{path}/extract/{batch_id}_statement.pickle",
            "minutes": f"{path}/extract/{batch_id}_minutes.pickle",
            "model": f"{path}/model/model.pickle",
            "vectorizer": f"{path}/model/vectorizer.pickle",
            # historical dataframes: for updating
            "historical": {
                "statements": f"{path}/historical/st_df.pickle",
                "minutes": f"{path}/historical/mins_df.pickle",
            },
        }
        self.data = self.load_data()

    def load_data(self):
        """
        Returns dictionary of dataframes
        """

        data = {}
        data["historical"] = {
            "statements": pd.DataFrame(),
            "minutes": pd.DataFrame(),
        }  # to set the structure

        for k, v in self.directory.items():
            if k == "historical":
                for k2, v2 in self.directory["historical"].items():
                    f = open(v2, "rb")
                    df = pickle.load(f)
                    data["historical"][k2] = df
            else:
                f = open(v, "rb")
                df = pickle.load(f)
                if k == "statements" or k == "minutes":
                    df = self.filter_date(df)
                data[k] = df

        return data

    def filter_date(self, df):
        """
        Filter only relevant dates based on users specification
        """
        # Ensure in datetime format
        df["date"] = pd.to_datetime(df["date"])

        # Filter based on from_year parameters
        start_date = pd.Timestamp(self.start_dt, 1, 1)
        df = df[(df["date"] >= start_date)]
        df.reset_index(inplace=True, drop=True)

        return df