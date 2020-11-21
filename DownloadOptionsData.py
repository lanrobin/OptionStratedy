import yfinance as yf
import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
from joblib import Parallel, delayed
import multiprocessing
import sys
from sys import platform


DataRoot = "/datadrive/data"
LogRoot = "/datadrive//log.txt"
Holidays = "/datadrive/github/OptionStratedy/holidays.txt"
SymbolRoot = "/datadrive/github/OptionStratedy"

def DownloadAllData(symbol, date):
    if date.dayofweek == 5 or date.dayofweek == 6:
        logging.debug("weekend, stock market is closed.")
        return
    dateStr = date.strftime("%Y-%m-%d")
    if IsHoliday(dateStr):
        logging.debug("Holiday, stock market is closed.")
        return
    s = yf.Ticker(symbol)

    #确保这个目录存在
    symbolPath = DataRoot +"/" +symbol
    os.makedirs(symbolPath, exist_ok = True)
    logging.debug("Get stock price for " + symbol +" at " + dateStr)
    try:
        todayData = s.history(period="1d", interval="1d", start=dateStr)
        with open(symbolPath +"/stock.csv", "a") as stock:
            stock.write(dateStr +",")
            stock.writelines(",".join(map(str, todayData.values[-1]))+ "\n", )
    except Exception as e:
        logging.error("Get history for" + symbol + " get exception:" + str(e))

    datePathStr = symbolPath +"/" + dateStr
    try:
        expirations = s.options
        logging.debug("Get options for " + symbol +" at " + dateStr + ". There are " + str(len(expirations)) +" expirations.")
        os.makedirs(datePathStr, exist_ok = True)
        for exp in expirations:
            chain = s.option_chain(exp)
            if not os.path.exists(datePathStr +"/README.txt"):
                with open(datePathStr +"/README.txt", "w") as readmef:
                    readmef.writelines("CALL COLUMNS:")
                    readmef.writelines(",".join(chain.calls.columns) +"\n")
                    readmef.writelines("PUT COLUMNS:")
                    readmef.writelines(",".join(chain.puts.columns) +"\n")
            with open(datePathStr +"/calls"+ exp +".csv", "w") as callf:
                for call in chain.calls.values:
                    callf.writelines(",".join(map(str, call)) + "\n")
            with open(datePathStr +"/puts" + exp +".csv", "w") as putf:
                for put in chain.puts.values:
                    putf.writelines(",".join(map(str, put)) + "\n")
        logging.debug("Got options for " + symbol +" at " + dateStr)
    except Exception as ex:
        logging.error("Get options for " + symbol +" with exception:" + str(ex))

S_Holidays_List = []

def IsHoliday(datestr):
    if len(S_Holidays_List) == 0:
        with open(Holidays) as hf:
            S_Holidays_List.extend(hf.read().splitlines())

    return datestr in S_Holidays_List

def GetAllData():
    allSymbols = []
    symbolFiles = ["DJIA.txt", "nasdaq100.txt", "SP500.txt"]
    for sf in symbolFiles:
        with open(SymbolRoot +"/" + sf) as df:
            allSymbols.extend(df.read().splitlines())
    allSymbols = list(set(allSymbols))

    num_cores = multiprocessing.cpu_count()
    logging.debug("There are " + str(num_cores) + " CPU(s) on this computer.")
    #fetchDate = pd.Timestamp.now()
    fetchDate = pd.Timestamp("2020-11-20")
    Parallel(n_jobs=num_cores)(delayed(DownloadAllData)(symbol = i, date = fetchDate) for i in allSymbols)
    #for s in allSymbols:
        #DownloadAllData(s, pd.Timestamp.now())
        #DownloadAllData("BF.B", fetchDate)


    
if __name__ == '__main__':
#    print(IsHoliday("2021-1-1"))
#    print(IsHoliday("2021-1-2"))

    if platform == "win32":
        DataRoot = "d:/data"
        LogRoot = "d:/data/log.txt"
        Holidays = "d:/github/OptionStratedy/holidays.txt"
        SymbolRoot = "D:/github/OptionStratedy"

    logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler(LogRoot),
                logging.StreamHandler()
            ]
        )
#    DownloadAllData("msft", pd.Timestamp("2020-11-20"))
    GetAllData()