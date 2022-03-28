import datetime

from dataloader import DataLoader
from datapreprocessor import DataPreprocessor
from dictmodel import DictionaryModel
from backtest import Backtest

# from .extract.import_data import import_sentiment_data

def run_sentiment_model(path="../data/sentiment_data", extract_path ='../data/sentiment_data/extract/'):
    from_year = datetime.datetime.now().year
    # # Import sentiment data
    # import_sentiment_data(from_year, base_dir=extract_path)

    print(f"===== Running Hawkish-Dovish Index Model =====".title())
    batch_id = datetime.date.today().strftime("%y%m%d")
    dataloader = DataLoader(from_year, path)

    # Preprocessing
    datapreprecessor = DataPreprocessor(dataloader.data, batch_id, path)

    # bt = Backtest(datapreprecessor.data, from_year, path)
    # bt.predict()

    dict_based = DictionaryModel(datapreprecessor.data, from_year, path)
    dict_based.predict()

    # dict_based = DictionaryModel(bt.data, from_year, path)
    # dict_based.predict()

    print(f"===== Modelling Process Completed =====".title())


if __name__ == "__main__":
    run_sentiment_model()