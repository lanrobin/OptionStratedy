import yfinance as yf
import pandas as pd
import os
from loguru import logger
from joblib import Parallel, delayed
import multiprocessing

DataRoot = "/datadrive/data"
LogRoot = "/datadrive//log.txt"
Holidays = "/datadrive/github/OptionStragtedy/holidays.txt"
SymbolRoot = "/datadrive/github/OptionStragtedy"

def DownloadAllData(symbol, date):
    if date.dayofweek == 5 or date.dayofweek == 6:
        logger.debug("weekend, stock market is closed.")
        return
    dateStr = date.strftime("%Y-%m-%d")
    if IsHoliday(dateStr):
        logger.debug("Holiday, stock market is closed.")
        return
    s = yf.Ticker(symbol)

    #确保这个目录存在
    symbolPath = DataRoot +"/" +symbol
    os.makedirs(symbolPath, exist_ok = True)
    logger.debug("Get stock price for " + symbol +" at " + dateStr)
    try:
        todayData = s.history(period="1d", interval="1d", start=dateStr)
        with open(symbolPath +"/stock.csv", "a") as stock:
            stock.write(dateStr +",")
            stock.writelines(",".join(map(str, todayData.values[-1]))+ "\n", )
    except Exception as e:
        logger.error("Get history for" + symbol + " get exception:" + str(e))

    datePathStr = symbolPath +"/" + dateStr
    try:
        expirations = s.options
        logger.debug("Get options for " + symbol +" at " + dateStr + ". There are " + len(expirations) +" expirations.")
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
        logger.debug("Got options for " + symbol +" at " + dateStr)
    except Exception as ex:
        logger.error("Get options for " + symbol +" with exception:" + str(ex))

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
    logger.debug("There are " + str(num_cores) + " CPU(s) on this computer.")
    #fetchDate = pd.Timestamp.now()
    fetchDate = pd.Timestamp("2020-11-20")
    #Parallel(n_jobs=num_cores)(delayed(DownloadAllData)(i, fetchDate) for i in allSymbols)
    for s in allSymbols:
        #DownloadAllData(s, pd.Timestamp.now())
        DownloadAllData(s, fetchDate)


    
if __name__ == '__main__':
#    print(IsHoliday("2021-1-1"))
#    print(IsHoliday("2021-1-2"))
    logger.add(LogRoot, rotation="512 MB")
#    DownloadAllData("msft", pd.Timestamp("2020-11-20"))
    GetAllData()