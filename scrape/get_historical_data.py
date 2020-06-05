from pandas_datareader import data as pdr
import yfinance as yf
import pandas as pd
from multiprocessing import Pool, Manager


def get_historical_data(ticker):
    yf.pdr_override()
    ticker_historical_df = pdr.get_data_yahoo(
        ticker, period='Max')
    ticker_historical_df.to_csv(
        "./data/historical_data/" + ticker + ".csv", header=True)


tickers_df = pd.read_csv('./data/tickerList.csv', header=0)
tickers_list = tickers_df["TICKER"].tolist()
with Pool(10) as p:
    p.map(get_historical_data, tickers_list)
