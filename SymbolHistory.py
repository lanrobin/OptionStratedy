from yahoo_historical import Fetcher
from joblib import Parallel, delayed
import multiprocessing

DataRoot = "D:\\data"

def GetOneStockHistory(sym):
    fc = Fetcher(sym, [2000, 1, 1], [2020, 11, 18])
    print("开始获取" + sym)
    with open(DataRoot +"\\" + sym +".csv", "w") as f:
        data = fc.getHistorical()
        for item in data.values:
            f.writelines(",".join(map(str, item))+ "\n")
    print("完成获取" + sym)

with open(DataRoot +"\\symbols.txt", encoding="utf-8") as s:
    lines = s.read().splitlines()
    num_cores = multiprocessing.cpu_count()
    Parallel.print_progress = 
    Parallel(n_jobs=num_cores)(delayed(GetOneStockHistory)(i) for i in lines)
    #for i in lines:
    #    GetOneStockHistory(i)
    print("全部搞定")