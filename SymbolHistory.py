from yahoo_historical import Fetcher
import multiprocessing
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

DataRoot = "D:\\data"

def GetOneStockHistory(sym):
    fc = Fetcher(sym, [2000, 1, 1], [2020, 11, 27])
    print("开始获取" + sym)
    with open(DataRoot +"\\" + sym +".csv", "w") as f:
        data = fc.getHistorical()
        f.writelines(",".join(map(str, data.columns)) + "\n")
        for item in data.values:
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