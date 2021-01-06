from typing import Any
import yfinance as yf
import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import sys
from sys import platform

DataRoot = "C:\\Users\\rolan\\sdata"
LogRoot = "C:\\Users\\rolan\\sdata\\log.txt"
SYMBOLPATH = ".\HighDividends.txt"

def GetHistoryDataAndDividend(symbol):
    date = pd.Timestamp.now()
    dateStr = date.strftime("%Y-%m-%d")
    fromDateStr = "2000-01-01"
    s = yf.Ticker(symbol)
    os.makedirs(DataRoot, exist_ok = True)
    logging.info("Get stock price for " + symbol +" from " + fromDateStr)
    try:
        todayData = s.history(period=Any, interval="1d", start=fromDateStr)
        with open(DataRoot+"/" + symbol +".csv", "a") as stock:
            stock.writelines("Date," + ",".join(map(str, todayData.columns))+ "\n")
            for d, v in zip(todayData.T, todayData.values):
                stock.writelines(d.strftime("%Y-%m-%d") +", " + ",".join(map(str, v))+ "\n")
    except Exception as e:
        logging.error("Get history for" + symbol + " get exception:" + str(e))



if __name__ == '__main__':

    os.makedirs(DataRoot, exist_ok = True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LogRoot),
            logging.StreamHandler()
        ]
    )

    allSymbols = []
    with open(SYMBOLPATH) as df:
            allSymbols.extend(df.read().splitlines())
    num_cores = multiprocessing.cpu_count()
    logging.info("There are " + str(num_cores) + " CPU(s) on this computer.")
    with ThreadPoolExecutor(2 * num_cores) as executor:
       results = executor.map(GetHistoryDataAndDividend, allSymbols)
       for result in results:
           logging.info("Result:" + str(result))