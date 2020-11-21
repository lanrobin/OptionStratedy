import pandas as pd
import math

def GetWeeklyVolatility(filePath):
    result = []
    with open(filePath) as f:
        lines = f.read().splitlines()
        startingPrice = 0
        endPrice = 0
        isTheFirstDayOfWeek = False
        lastDay = -1
        for line in lines:
            parts = line.split(',')
            date = pd.Timestamp(parts[0])

            if date.dayofweek < lastDay:
                ## 如果是下一周的开始了。就是算上一周的波动幅度了。
                v = (endPrice - startingPrice)/startingPrice * 100
                #print(v)
                result.append([v, startingPrice, endPrice])
                isTheFirstDayOfWeek = False
                lastDay = -1
                startingPrice = 0
                endPrice = 0
            ## 找到开盘价。
            if not isTheFirstDayOfWeek:
                isTheFirstDayOfWeek = True
                startingPrice = float(parts[1])
            
            lastDay = date.dayofweek
            endPrice = float(parts[4])
        if isTheFirstDayOfWeek:
            ## 计算最后一次的。
            v = (endPrice - startingPrice)/startingPrice * 100
            #print(v)
            result.append([v, startingPrice, endPrice])
    return result

def SellCoveredCall(v, atPercentage, optionPricePrecentage):
    totalValue = v[0][1] #第一次买的股票的钱。
    numberOfShare = (totalValue//v[0][1])
    optionFired = False
    for index, i in enumerate(v):
        numberOfShare = max((totalValue//v[index][1]), 1)
        if(math.isnan(numberOfShare) or numberOfShare > 300):
            print("出问题了。")
        optionPrice = i[1] * optionPricePrecentage * numberOfShare
        lostLastWeek  = 0
        firedIncreasementLastWeek = 0
        if optionFired:
            # 如果被行权了，我要需要以这周的开盘价买加股票，所以亏损的钱也要算上。
            lostLastWeek = (lastFiredPrice - i[1]) * numberOfShare
            if(math.isinf(lostLastWeek)):
                print("出问题了。")
            optionFired = False

        if(i[0] >= atPercentage):
            print("被行权了。")
            firedIncreasementLastWeek = (i[1] * atPercentage / 100) * numberOfShare
            lastFiredPrice = i[1] + firedIncreasementLastWeek
            optionFired = True
        totalValue += lostLastWeek + optionPrice + firedIncreasementLastWeek
        print("上周损失：" + str(lostLastWeek) + "，收取期权的价格:" + str(optionPrice) +",行权增量：" + str(firedIncreasementLastWeek) + "总价值:" + str(totalValue) + "持股数：" + str(numberOfShare) + "数据:" + str(v[index]))
    return totalValue



v = GetWeeklyVolatility("D:\\data\\KO.csv")
total = SellCoveredCall(v, 1, 0.012)

print("最后的总价值是：" + str(total) +",初始资金是:" + str(v[0][1]))