import pandas as pd
import math

DataRoot = "D:\\data"
SymbolName = "QQQ"

def GetWeeklyVolatility(filePath):
    result = []
    with open(filePath) as f:
        lines = f.read().splitlines()
        startingPrice = 0
        endPrice = 0
        weeklyHigh = 0
        weeklyLow = 0
        isTheFirstDayOfWeek = False
        lastDay = -1
        dateStr = ''
        dividend = 0
        for line in lines:
            parts = line.split(',')
            date = pd.Timestamp(parts[0])
            dateStr = parts[0]
            if date.dayofweek < lastDay:
                ## 如果是下一周的开始了。就是算上一周的波动幅度了。
                v = (endPrice - startingPrice)/startingPrice * 100
                #print(v)
                result.append([dateStr, v, startingPrice, endPrice, weeklyHigh, weeklyLow, dividend])
                isTheFirstDayOfWeek = False
                lastDay = -1
                startingPrice = 0
                endPrice = 0
                weeklyHigh = 0
                weeklyLow = 0
                dividend = 0

            ## 找到开盘价。
            if not isTheFirstDayOfWeek:
                isTheFirstDayOfWeek = True
                startingPrice = float(parts[1])
                weeklyHigh = float(parts[2])
                weeklyLow = float(parts[3])
            
            weeklyHigh = max(weeklyHigh, float(parts[2]))
            weeklyLow = min(weeklyLow, float(parts[3]))
            dividend += float(parts[6])
            lastDay = date.dayofweek
            endPrice = float(parts[4])
        if isTheFirstDayOfWeek:
            ## 计算最后一次的。
            v = (endPrice - startingPrice)/startingPrice * 100
            #print(v)
            result.append([dateStr, v, startingPrice, endPrice, weeklyHigh, weeklyLow, dividend])
    return result
'''

'''
def SellCoveredCall(v, atPercentage, optionPricePrecentage, initialStockCount):
    totalValue = v[0][2] * initialStockCount #第一次买的股票的钱。需要100股起。
    numberOfShare = initialStockCount
    optionExecuted = False
    lastExecutedPrice = 0 # 上周被行权时股票的价格
    executedIncreasementLastWeek = 0 # 上周被行权时股票价格与周一开盘价的差值。
    totalCash = 0  # 开始时现金是 0
    lostLastWeek = 0
    OptionOperationFee = 0
    StockOperationFee = 0
    dividendThisWeek = 0
    for i in v:
        if optionExecuted:
            # 如果上周被行权了，则要重新买回来，所以要算这周还能买多少。
            holdNumberOfShare = int(totalValue/i[2])
            if(holdNumberOfShare < initialStockCount):
                print("亏损了，需要补充现金。")
                totalCash -= (initialStockCount - holdNumberOfShare) * i[2]
                numberOfShare = initialStockCount
            else:
                newNumberOfShare = int(holdNumberOfShare/100) * 100 # 如果利润累加到100股了，就新增100股。
                totalCash = totalValue - newNumberOfShare * i[2]
                numberOfShare = newNumberOfShare
        elif totalCash/i[2] >= 100:
            # 如果现金又够买100股以上的股票了。就要买了。
            newPurchasedStock = int(int(totalCash/i[2])/100) * 100
            totalCash -= BuyStockFee(newPurchasedStock, i[2])
            numberOfShare += newPurchasedStock

        optionPrice = i[2] * optionPricePrecentage # 每股的期权价格

        # 不管有没有被行权，这都是我们的现金
        totalCash += optionPrice * numberOfShare

        # 先减去手续费
        operationFee = SellOptionFee(numberOfShare/100, optionPrice)
        totalValue -= operationFee
        totalCash -= operationFee

        # 加上股息,中国人要交10%的税。
        dividendThisWeek = float(i[6]) * numberOfShare * 0.9
        totalValue += dividendThisWeek
        totalCash += dividendThisWeek

        if optionExecuted:
            # 如果被行权了，我要需要以这周的开盘价买加股票，所以亏损的钱也要算上。
            lostLastWeek = (lastExecutedPrice - i[2])

            # 如果被行权了，还要交买股票的手续费。
            StockOperationFee = BuyStockFee(numberOfShare, i[2])
            totalValue -= StockOperationFee
            totalCash -= StockOperationFee
            optionExecuted = False

        if(i[1] >= atPercentage):
            print("被行权了。")
            executedIncreasementLastWeek = i[2] * atPercentage / 100
            lastExecutedPrice = i[2]  + executedIncreasementLastWeek # 被行权的股票价格。
            optionExecuted = True

        totalValue += (lostLastWeek + optionPrice + executedIncreasementLastWeek) * numberOfShare
        print(i[0] + ",上周损失：" + str(lostLastWeek * numberOfShare) + "，收取期权费:" + str(optionPrice * numberOfShare) +",行权增量：" + str(executedIncreasementLastWeek)
        + "总价值:" + str(totalValue) + "持股数：" + str(numberOfShare) +"手续费:" + str(OptionOperationFee + StockOperationFee) + ", 分红：" + str(dividendThisWeek))
        lostLastWeek = 0
        executedIncreasementLastWeek = 0
    return totalValue

# 参照这里 https://www.snowballsecurities.com/help/detail/commission/us
def SellOptionFee(contractCount, optionPrice):
    commission = max(0.65 * contractCount, 1.99)
    platformFree = 0.3 * contractCount
    ORF = 0.0388 * contractCount
    FINRA = 0.002 * contractCount
    SellFree = contractCount * 100 * optionPrice * 0.0000221
    return commission + platformFree + ORF + FINRA + SellFree

# 参照这里 https://www.snowballsecurities.com/help/detail/commission/us
def BuyStockFee(stockCount, stockPrice):
    platformFee = min(1, 0.004 * stockCount)
    commission = min(stockCount * stockPrice * 0.01, max(0.99, 0.003 * stockCount))
    return platformFee + commission

def WriteToFile(values, fileName):
    with open(fileName, "w") as f:
        for l in values:
            f.writelines(",".join(map(str, l)) + "\n")

v = GetWeeklyVolatility(DataRoot + "\\"+ SymbolName +".csv")

WriteToFile(v, DataRoot +"\\"+ SymbolName +"weekly.csv")

numberOfInitialShare = 1000
total = SellCoveredCall(v, 4.0, 0.0011, numberOfInitialShare)

initialValue = v[0][2] * numberOfInitialShare
increasedfolder = total/initialValue
print("最后的总价值是：" + str(total) +",初始资金是:" + str(initialValue) +"增长了" + str(increasedfolder) +"倍，20年年收益是：" + str(pow(increasedfolder, 1.0/20)))