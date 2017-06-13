"""
聚合K线，生成相应周期
"""
import time, datetime
import pandas as pd
import pickle
import os
from dbHandle import dbHandle

class AggregateTickData(object):

    def __init__(self, dfInfo, date, df):
        self.timeFilePath = os.getcwd() + '/' + 'timeSeriesFile/'
        self.barDict = {}
        self.splitDict = {}
        self.dfInfo = dfInfo
        self.df = df
        self.Symbol = 'a'
        self.db = dbHandle()
        self.timePoint = date
        self.initStart()

    def initStart(self):
        self.getTimeList()
        db = self.db.get_db("localhost", 27017, 'WIND_TICK_DB')
        names = self.db.get_all_colls(db)
        for i in names:
            self.genKData(i, self.df)

    def getTimeList(self):
        if not os.path.exists(self.timeFilePath):
            os.makedirs(self.timeFilePath)
        filePath = self.timeFilePath + 'timeSeries_' + self.Symbol + '.pickle'
        if os.path.exists(filePath) and datetime.datetime.fromtimestamp(os.path.getmtime(filePath)).replace(hour=0,minute=0,second=0,microsecond=0) == \
            datetime.datetime.today().replace(hour=0,minute=0,second=0,microsecond=0):
            with open(filePath, 'rb') as handle:
                self.splitDict = pickle.load(handle)
        else:
            self.genTimeList(self.Symbol)
            self.saveTimeList()

    def saveTimeList(self):
        with open(self.timeFilePath + 'timeSeries_' + self.Symbol + '.pickle', 'wb') as handle:
            pickle.dump(self.splitDict, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def genTimeList(self, symbol):
        cycle = [1,5,15,30,60]
        tempDict = {}
        self.splitDict[symbol] = {}
        for c in cycle:
            tempDict[c] = []
            self.splitDict[symbol][c] = []
            tp = self.dfInfo.loc[symbol]["CurrPeriod"]
            time1 = [t for i in tp.split(',') for t in i.split('-')]
            for i in zip(*([iter(time1)] * 2)):
                start = str(i[0]).strip()
                end = str(i[1]).strip()
                if '8:59' in start:
                    start = '9:00'
                tempList = pd.date_range(start, end, freq=(str(c) + 'min')).time.tolist()
                tempDict[c].extend(tempList)
                if len(tempList)%2:
                    tempDict[c].extend(pd.date_range(end, end, freq=(str(c) + 'min')).time.tolist())
            lst = list(set(tempDict[c]))
            lst.sort()
            self.splitDict[symbol][c] = lst

    def genKData(self, vtSymbol, df_data):
        self.gen1minKData(vtSymbol, df_data)
        self.genOtherKData(vtSymbol)
        self.gen1DayKData(vtSymbol)

    def gen1minKData(self, vtSymbol, df_data):
        c = 1
        self.barDict[vtSymbol] = {}
        self.barDict[vtSymbol][c] = []
        self.df["structTime"] = self.df["time"].map(lambda x:time.strptime(x, "%H%M%S%f"))
        for i in zip(*[iter(self.splitDict[vtSymbol][c][i:]) for i in range(2)]):
            start = time.strptime(str(i[0]).strip(), '%H:%M:%S')
            end = time.strptime(str(i[1]).strip(), '%H:%M:%S')
            if '9:00:00' in str(i[0]):
                start = time.strptime('8:59:00', '%H:%M:%S')
            p1 = df_data["structTime"] >= start
            p2 = df_data["structTime"] < end
            dfTemp = df_data.loc[p1 & p2]
            if not dfTemp.empty:
                self.barDict[vtSymbol][c].append(self.aggMethod(dfTemp))

        dbNew = self.db.get_db("localhost", 27017, 'WIND_1_MIN_DB')
        self.db.insert2db(dbNew, vtSymbol, self.barDict[vtSymbol][c])

    def genOtherKData(self, vtSymbol):
        cycle = [5,15,30,60]
        for c in cycle:
            self.barDict[vtSymbol][c] = []
            for i in zip(*[iter(self.splitDict[vtSymbol][c][i:]) for i in range(2)]):
                self.start1 = time.strptime(str(i[0]).strip(), '%H:%M:%S')
                self.end1 = time.strptime(str(i[1]).strip(), '%H:%M:%S')
                items = list(map(self.selectItems, self.barDict[vtSymbol][1]))
                items = list(filter(lambda x:x is not None, items))
                dfTemp = pd.DataFrame(items)
                if not dfTemp.empty:
                    self.barDict[vtSymbol][c].append(self.aggMethod(dfTemp))

            dbNew = self.db.get_db("localhost", 27017, 'WIND_' + str(c) + '_MIN_DB')
            self.db.insert2db(dbNew, vtSymbol, self.barDict[vtSymbol][c])

    def gen1DayKData(self, vtSymbol):
        c = '1Day'
        self.barDict[vtSymbol][c] = []
        items = self.barDict[vtSymbol][1]
        dfTemp = pd.DataFrame(items)
        if not dfTemp.empty:
            self.barDict[vtSymbol][c].append(self.aggMethod(dfTemp))
            dbNew = self.db.get_db("localhost", 27017, 'WIND_' + str(c) + '_MIN_DB')
            self.db.insert2db(dbNew, vtSymbol, self.barDict[vtSymbol][c])

    def selectItems(self,x):
        ti = time.strptime(x["time"].strip(), '%H%M%S%f')
        if ti >= self.start1 and ti < self.end1:
            return x

    def aggMethod(self, dfTemp):
        tempBar = {}
        tempBar["windCode"] = dfTemp.iloc[0]["windCode"]
        tempBar["code"] = dfTemp.iloc[0]["code"]
        tempBar["date"] = dfTemp.iloc[0]["date"]
        tempBar["time"] = dfTemp.iloc[0]["time"]
        tempBar["lastPrice"] = float(dfTemp.iloc[-1]["lastPrice"])
        tempBar["lastVolume"] = float(dfTemp.iloc[-1]["lastVolume"])
        tempBar["lastTurnover"] = float(dfTemp.iloc[-1]["lastTurnover"])
        tempBar["openInterest"] = float(dfTemp.iloc[-1]["openInterest"])
        tempBar["volume"] = float(sum(dfTemp["volume"]))
        tempBar["turnover"] = float(sum(dfTemp["turnover"]))
        tempBar["highPrice"] = float(max(dfTemp["highPrice"]))
        tempBar["lowPrice"] = float(min(dfTemp["lowPrice"]))
        tempBar["openPrice"] = float(dfTemp.iloc[0]["openPrice"])
        tempBar["preClosePrice"] = float(dfTemp.iloc[-1]["preClosePrice"])
        tempBar["position"] = float(dfTemp.iloc[-1]["position"])
        tempBar["prePosition"] = float(dfTemp.iloc[-1]["prePosition"])
        tempBar["askPrice1"] = float(max(dfTemp["askPrice1"]))
        tempBar["askVolume1"] = float(max(dfTemp["askVolume1"]))
        tempBar["bidPrice1"] = float(max(dfTemp["bidPrice1"]))
        tempBar["bidVolume1"] = float(max(dfTemp["bidVolume1"]))
        tempBar["askAvPrice"] = float(dfTemp["askPrice1"].mean())
        tempBar["bidAvPrice"] = float(dfTemp["bidPrice1"].mean())
        tempBar["totalAskVolume"] = float(sum(dfTemp["askVolume1"]))
        tempBar["totalBidVolume"] = float(sum(dfTemp["bidVolume1"]))
        tempBar["datetime"] = dfTemp.iloc[0]["datetime"]
        tempBar["matchItems"] = 0
        tempBar["tradeFlag"] = ''
        tempBar["bsFlag"] = ''
        tempBar["settlementPrice"] = 0
        tempBar["curDelta"] = 0
        tempBar["preSettlementPrice"] = 0
        tempBar["index"] = 0
        tempBar["stocks"] = 0
        tempBar["ups"] = 0
        tempBar["downs"] = 0
        tempBar["holdLines"] = 0
        return tempBar


