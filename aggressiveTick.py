"""
聚合K线，生成相应周期
"""
import time, datetime
import pandas as pd
import pickle
import os
from dbHandle import dbHandle
from module_mylog import gLogger

class AggregateTickData(object):

    def __init__(self, dfInfo, date, aucTime):
        self.timeFilePath = os.getcwd() + '/' + 'timeSeriesFile/'
        self.barDict = {}
        self.splitDict = {}
        self.dfInfo = dfInfo
        self.db = dbHandle()
        self.timePoint = date
        self.cycle = [1, 5, 15, 30, 60]
        self.AucTime = aucTime
        self.initStart()

    def initStart(self):
        self.getTimeList(self.cycle)
        db = self.db.get_db("localhost", 27017, 'WIND_TICK_DB')
        names = self.db.get_all_colls(db)
        for i in names:
            self.Symbol = "".join([a for a in i if a.isalpha()]).lower()
            self.df = self.db.get_specificDayItems(db, i, self.timePoint)
            self.genKData(i, self.df)

    def getTimeList(self, cycle):
        try:
            gLogger.info("start getTimeList")
            if not os.path.exists(self.timeFilePath):
                os.makedirs(self.timeFilePath)
            filePath = self.timeFilePath + 'timeSeries_' + self.Symbol + '.pickle'
            if os.path.exists(filePath) and datetime.datetime.fromtimestamp(os.path.getmtime(filePath)).replace(hour=0,minute=0,second=0,microsecond=0) == \
                datetime.datetime.today().replace(hour=0,minute=0,second=0,microsecond=0):
                gLogger.info("splitDict is load from pickle file")
                with open(filePath, 'rb') as handle:
                    self.splitDict[self.Symbol] = pickle.load(handle)
            else:
                self.genTimeList(self.Symbol, cycle)
                self.saveTimeList()
        except Exception as e:
            gLogger.exception("Exception : %s" %e)

    def saveTimeList(self):
        with open(self.timeFilePath + 'timeSeries_' + self.Symbol + '.pickle', 'wb') as handle:
            pickle.dump(self.splitDict[self.Symbol], handle, protocol=pickle.HIGHEST_PROTOCOL)

    def genTimeList(self, symbol, cycle):
        try:
            tempDict = {}
            self.splitDict[symbol] = {}
            for c in cycle:
                gLogger.info("start genTimeList, cycle = %d" %c)
                tempDict[c] = []
                self.splitDict[symbol][c] = []
                tp = self.dfInfo.loc[symbol]["CurrPeriod"]
                time1 = [t for i in tp.split(',') for t in i.split('-')]
                for i in zip(*([iter(time1)] * 2)):
                    start = str(i[0]).strip()
                    end = str(i[1]).strip()
                    if start in self.AucTime:
                        start1 = datetime.datetime.strptime(start, "%H:%M:%S") + datetime.timedelta(minutes=1)
                        start = start1.strftime("%H:%M:%S")
                    tempList = pd.date_range(start, end, freq=(str(c) + 'min')).time.tolist()
                    tempDict[c].extend(tempList)
                    tempDict[c].extend(pd.date_range(end, end, freq=(str(c) + 'min')).time.tolist())
                lst = list(set(tempDict[c]))
                lst.sort()
                self.splitDict[symbol][c] = lst
        except Exception as e:
            gLogger.exception("Exception : %s" %e)


    def genKData(self, vtSymbol, df_data):
        cycle = self.cycle[1:]
        self.gen1minKData(vtSymbol, df_data)
        self.genOtherKData(vtSymbol, cycle)
        self.gen1DayKData(vtSymbol)

    def gen1minKData(self, vtSymbol, df_data):
        try:
            gLogger.info("start gen1minKData , vtSymbol is %s" %vtSymbol)
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
        except Exception as e:
            gLogger.exception("Exception : %s" %e)

    def genOtherKData(self, vtSymbol, cycle):
        for c in cycle:
            try:
                gLogger.info("start genOtherKData cycle = %d" %c)
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
            except Exception as e:
                gLogger.exception("Exception : %s" %e)

    def gen1DayKData(self, vtSymbol):
        try:
            gLogger.info("start gen1DayKData , vtSymbol = %s" %vtSymbol)
            c = '1Day'
            self.barDict[vtSymbol][c] = []
            items = self.barDict[vtSymbol][1]
            dfTemp = pd.DataFrame(items)
            if not dfTemp.empty:
                self.barDict[vtSymbol][c].append(self.aggMethod(dfTemp))
                dbNew = self.db.get_db("localhost", 27017, 'WIND_' + str(c) + '_MIN_DB')
                self.db.insert2db(dbNew, vtSymbol, self.barDict[vtSymbol][c])
        except Exception as e:
            gLogger.exception("Exception : %s" %e)

    def selectItems(self,x):
        ti = time.strptime(x["time"].strip(), '%H%M%S%f')
        if ti >= self.start1 and ti < self.end1:
            return x

    def aggMethod(self, dfTemp):
        try:
            tempBar = {}
            tempBar["vtSymbol"] = dfTemp.iloc[0]["vtSymbol"]
            tempBar["symbol"] = dfTemp.iloc[0]["symbol"]
            tempBar["date"] = dfTemp.iloc[0]["date"]
            tempBar["time"] = dfTemp.iloc[0]["time"]
            tempBar["openInterest"] = float(dfTemp.iloc[-1]["openInterest"])
            tempBar["volume"] = float(dfTemp["lastVolume"].sum())
            tempBar["turnover"] = float(dfTemp["lastTurnover"].sum())
            tempBar["high"] = float(max(dfTemp["lastPrice"]))
            tempBar["low"] = float(min(dfTemp["lastPrice"]))
            tempBar["open"] = float(dfTemp.iloc[0]["lastPrice"])
            tempBar["close"] = float(dfTemp.iloc[-1]["lastPrice"])
            tempBar["datetime"] = dfTemp.iloc[0]["datetime"].replace(millseconds=0)
            return tempBar
        except Exception as e:
            gLogger.exception("Exception when exec aggMethod e:%s" %e)


