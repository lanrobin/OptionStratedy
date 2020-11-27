import yfinance as yf
import multiprocessing
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import pandas as pd

DataRoot = "D:\\data"

def GetOneStockHistory(sym):
    date = pd.Timestamp.now()
    s = yf.Ticker(sym)
    dateStr = date.strftime("%Y-%m-%d")
    print("开始获取" + sym)
    data = s.history(interval="1d", start="2000-1-1", end=dateStr)
    with open(DataRoot +"\\" + sym +".csv", "w") as f:
        for index, item in zip(data.index.date, data.values):
            f.writelines(str(index) +",")
            f.writelines(",".join(map(str, item))+ "\n")
    print("完成获取" + sym)

with open(DataRoot +"\\symbols.txt", encoding="utf-8") as s:
    lines = s.read().splitlines()
    num_cores = multiprocessing.cpu_count()
    logging.info("There are " + str(num_cores) + " CPU(s) on this computer.")
    with ThreadPoolExecutor(5 * num_cores) as executor:
       results = executor.map(GetOneStockHistory, lines)
       for result in results:
           logging.info("Result:" + str(result))
    #for i in lines:
    #    GetOneStockHistory(i)
    print("全部搞定")