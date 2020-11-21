import yfinance as yf
import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import sys
from sys import platform


DataRoot = "/datadrive/data"
LogRoot = "/datadrive//log.txt"
Holidays = "/datadrive/github/OptionStratedy/holidays.txt"
SymbolRoot = "/datadrive/github/OptionStratedy"

def DownloadAllData(symbol):
    date = pd.Timestamp("2020-11-20")
    #date = pd.Timestamp.now()
    if date.dayofweek == 5 or date.dayofweek == 6:
        logging.info("weekend, stock market is closed.")
        return
    dateStr = date.strftime("%Y-%m-%d")
    if IsHoliday(dateStr):
        logging.info("Holiday, stock market is closed.")
        return
    s = yf.Ticker(symbol)
    allDataFetched = True
    #确保这个目录存在
    symbolPath = DataRoot +"/" +symbol
    os.makedirs(symbolPath, exist_ok = True)
    logging.info("Get stock price for " + symbol +" at " + dateStr)
    try:
        todayData = s.history(period="1d", interval="1d", start=dateStr)
        with open(symbolPath +"/stock.csv", "a") as stock:
            stock.write(dateStr +",")
            stock.writelines(",".join(map(str, todayData.values[-1]))+ "\n", )
    except Exception as e:
        logging.error("Get history for" + symbol + " get exception:" + str(e))
        allDataFetched = False

    datePathStr = symbolPath +"/" + dateStr
    try:
        expirations = s.options
        logging.info("Get options for " + symbol +" at " + dateStr + ". There are " + str(len(expirations)) +" expirations.")
        os.makedirs(datePathStr, exist_ok = True)
        for exp in expirations:
            chain = s.option_chain(exp)
            if not os.path.exists(datePathStr +"/README.txt"):
                with open(datePathStr +"/README.txt", "w") as readmef:
                    readmef.writelines("CALL COLUMNS:")
                    readmef.writelines(",".join(chain.calls.columns) +"\n")
                    readmef.writelines("PUT COLUMNS:")
                    readmef.writelines(",".join(chain.puts.columns) +"\n")
                    readmef.writelines("GETDATE:" + dateStr +"\n")
            with open(datePathStr +"/calls"+ exp +".csv", "w") as callf:
                for call in chain.calls.values:
                    callf.writelines(",".join(map(str, call)) + "\n")
            with open(datePathStr +"/puts" + exp +".csv", "w") as putf:
                for put in chain.puts.values:
                    putf.writelines(",".join(map(str, put)) + "\n")
        logging.info("Got options for " + symbol +" at " + dateStr)
    except Exception as ex:
        logging.error("Get options for " + symbol +" with exception:" + str(ex))
        allDataFetched = False
    return (symbol, allDataFetched)

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
    logging.info("There are " + str(num_cores) + " CPU(s) on this computer.")
    with ThreadPoolExecutor(5 * num_cores) as executor:
       results = executor.map(DownloadAllData, allSymbols)
       for result in results:
           logging.info("Result:" + str(result))


    
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