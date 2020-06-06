from pandas_datareader import data as pdr
import pandas as pd
import os
import yfinance as yf
import numpy as np
from ta import add_all_ta_features
from multiprocessing import Pool, Manager
import time

historial_data_path = "./data/historical_data"
# For some reason these files are all empty


def remove_empty_files():
    for file in os.listdir(historial_data_path):
        if "^" in file:
            os.remove(os.path.join(historial_data_path, file))
        # with open(os.path.join(historial_data_path, file), "r") as csv_file:
        #     df = pd.read_csv(csv_file)
        #     if len(df.index) <= 1:
        #         os.remove(os.path.join(historial_data_path, file))


# applied
def add_mean(df):
    if "Average" not in df.columns:
        print("CALCULATING AVERAGE")
        mean = []
        for index, row in df.iterrows():
            mean.append((row["High"] + row["Low"]) / 2)
        df["Average"] = mean


def add_general_info(df, ticker):
    insert_columns = [
        "zip",
        "sector",
        "city",
        "state",
        "industry",
        "exchange",
    ]

    for column in insert_columns:
        if column not in df.columns:
            try:
                company_info = yf.Ticker(ticker).info
                df[column] = company_info[column]
                print("SUCCESS: {}".format(ticker))
            except:
                print("NO GENERAL INFO FOR ticker:{} column:{}".format(ticker, column))
                df[column] = np.nan


def add_technical_info(df, ticker):
    try:
        df = add_all_ta_features(
            df, open="Open", high="High", low="Low", close="Close", volume="Volume"
        )
    except:
        return


def run(file):
    if not file.startswith(".") and os.path.isfile(
        os.path.join(historial_data_path, file)
    ):
        print(file)
        ticker = file.split(".")[0]
        with open(os.path.join(historial_data_path, file), "r") as csv_file:
            df = pd.read_csv(csv_file)
            if len(df.index) > 1:
                # Add whatever method you want to do for historical data, all should be in their own method
                # add_technical_info(df, ticker)
                # add_mean(df)
                add_general_info(df, ticker)
                df.to_csv(os.path.join(historial_data_path, file), index=False)


if __name__ == "__main__":
    remove_empty_files()
    files_list = os.listdir(historial_data_path)
    with Pool(3) as p:
        p.map(run, files_list)
