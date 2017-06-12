#-*- coding: utf-8 -*-
'''
数据清洗
'''

from pymongo import MongoClient
import pandas as pd
import time, datetime
import logging
import os
from dbHandle import dbHandle


LOG_FILE = os.getcwd() + '/' + 'LogFile/' + time.strftime('%Y-%m-%d',time.localtime(time.time()))  + ".log"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG)
logger = logging.getLogger(__name__)

def add_log(func):
    def newFunc(*args, **kwargs):
        logger.warning("Before %s() call on %s" % (func.__name__, time.strftime("%Y-%m-%d %H:%M:%S")))
        ret = func(*args, **kwargs)
        logger.warning("After %s() call on %s" % (func.__name__, time.strftime("%Y-%m-%d %H:%M:%S")))
        return ret
    return newFunc

class CleanData(object):

    def __init__(self, dfData, dfInfo):
        self.df = dfData
        self.date = datetime.datetime.strptime(self.df["date"][0], "%Y%m%d")
        self.dfInfo = dfInfo
        self.db = dbHandle()
        self.initCleanRegulation()

    def initList(self):
        self.removeList = []
        self.updateList = []
        self.logList = []

    def initCleanRegulation(self):
        dbNew = self.db.get_db("localhost", 27017, 'WIND_TICK_DB')
        i = self.df["code"][0]
        try:
            print ("start process collection %s........." %(i))
            logger.warning("start process collection %s........." %(i))
            self.Symbol = i.lower()
            self.initList()
            if not self.df.empty:
                self.cleanIllegalTradingTime()
                self.cleanSameTimestamp()
                self.reserveLastTickInAuc()
                self.cleanNullVolTurn()
                self.cleanNullPriceIndicator()
                self.cleanNullOpenInter()
                self.recordExceptionalPrice()

                self.delItemsFromRemove()
                self.db.insert2db(dbNew,i, self.df)
        except Exception as e:
            print ("Exception: %s" %e)
            logger.error("Exception: %s" %e)

    # def get_db(self,host,port,dbName):
    #     #建立连接
    #     client = MongoClient(host,port)
    #     db = client[dbName]
    #     return db
    #
    #
    # def insert2db(self,dbNew,coll_name):
    #     data = json.loads(self.df.T.to_json(date_format = 'iso')).values()
    #     for i in data:
    #         if isinstance(i["datetime"], str):
    #             i["datetime"] = datetime.datetime.strptime(i["datetime"], "%Y-%m-%dT%H:%M:%S.%fZ")
    #     dbNew[coll_name].insert_many(data)

    # def loadInformation(self):
    #     dfInfo = pd.read_csv(os.getcwd() + '/BasicInformation.csv')
    #     dfInfo.index = dfInfo['Symbol'].tolist()
    #     del dfInfo['Symbol']
    #     # 增加对历史周期交易时间段变更的记录
    #     dfInfo["CurrPeriod"] = dfInfo["TradingPeriod"].map(self.identifyCurrentPeriod)
    #     return dfInfo
    #
    # def identifyCurrentPeriod(self, target):
    #     if '%' in target:
    #         phase = [i for i in target.split('%')]
    #         phase.sort(reverse=True)
    #         for i in phase:
    #             startDate = datetime.datetime.strptime(i.split('||')[0], "%Y-%m-%d")
    #             if startDate <= self.date:
    #                 return i.split('||')[1]
    #             else:
    #                 continue
    #     else:
    #         return target.split('||')[1]

    @add_log
    def cleanIllegalTradingTime(self):
        """删除非交易时段数据"""
        self.df['illegalTime'] = self.df["time"].map(self.StandardizeTimePeriod)
        self.df['illegalTime'] = self.df['illegalTime'].fillna(False)
        for i,row in self.df[self.df['illegalTime'] == False].iterrows():
            self.removeList.append(i)
            logger.info('remove index = %d' %(i))
        del self.df["illegalTime"]

    @add_log
    def reserveLastTickInAuc(self):
        """保留集合竞价期间最后一个tick数据"""
        self.df["structTime"] = self.df["time"].map(lambda x: time.strptime(x, "%H%M%S%f"))
        start = time.strptime('8:59:00', '%H:%M:%S')
        end =  time.strptime('9:00:00', '%H:%M:%S')
        p1 = self.df["structTime"] >= start
        p2 = self.df["structTime"] < end
        dfTemp = self.df.loc[p1 & p2]
        dfTemp = dfTemp.sort_values(by = ["structTime"], ascending=False)
        for i in dfTemp.index.values[1:]:
            self.removeList.append(i)
            logger.info('remove index = %d' % i)

    @add_log
    def cleanSameTimestamp(self):
        """清除重复时间戳，记录"""
        dfTemp = self.df.sort_values(by=['datetime'], ascending=False)
        idList = dfTemp[dfTemp["datetime"].duplicated()].index
        for i in idList.values:
            self.removeList.append(i)
            logger.info('remove index = %d' % i)

    @add_log
    def cleanNullVolTurn(self):
        """Tick有成交，但volume和turnover为0"""
        f = lambda x: float(x)
        self.df.loc["lastVolume"] = self.df["lastVolume"].map(f)
        self.df.loc["lastTurnover"] = self.df["lastTurnover"].map(f)
        self.df.loc["volume"] = self.df["volume"].map(f)
        self.df.loc["turnover"] = self.df["turnover"].map(f)
        self.df.loc["openInterest"] = self.df["openInterest"].map(f)
        self.df.loc["lastPrice"] = self.df["lastPrice"].map(f)

        lastVol = self.df["lastVolume"] != 0.0
        lastTurn = self.df["lastTurnover"] != 0.0
        Vol = self.df["volume"] == 0.0
        Turn = self.df["turnover"] == 0.0
        openIn = self.df["openInterest"] == 0.0
        lastP = self.df["lastPrice"] != 0.0

        tu = self.dfInfo.loc[self.Symbol]["TradingUnits"]

        # lastTurn为0,lastVolume和lastPrice不为0
        dfTemp = self.df.loc[~lastTurn & lastVol & lastP]
        dfTemp.loc[:, "lastTurnover"] = dfTemp.loc[:, "lastVolume"] * dfTemp.loc[:, "lastPrice"] * float(tu)
        for i, row in dfTemp.iterrows():
            if i not in self.removeList:
                self.df.loc[i, "lastTurnover"] = row["lastTurnover"]
                self.updateList.append(i)
                logger.info('lastTurn = 0, update index = %d' % (i))

        # lastVolume为0,lastTurnover和lastPrice不为0
        dfTemp = self.df.loc[lastTurn & ~lastVol & lastP]
        dfTemp.loc[:, "lastVolume"] = dfTemp.loc[:, "lastTurnover"] / (dfTemp.loc[:, "lastPrice"] * float(tu))
        dfTemp["lastVolume"].map(lambda x: int(round(x)))
        for i, row in dfTemp.iterrows():
            if i not in self.removeList:
                self.df.loc[i, "lastVolume"] = row["lastVolume"]
                self.updateList.append(i)
                logger.info('lastVol = 0, update index = %d' % (i))

        # lastPrice为0,lastVolume和lastTurnover不为0
        dfTemp = self.df.loc[lastTurn & lastVol & ~lastP]
        dfTemp.loc[:, "lastPrice"] = dfTemp.loc[:, "lastTurnover"] / (dfTemp.loc[:, "lastVolume"] * float(tu))
        for i, row in dfTemp.iterrows():
            if i not in self.removeList:
                self.df.loc[i, "lastPrice"] = row["lastPrice"]
                self.updateList.append(i)
                logger.info('lastPrice = 0, update index = %d' % (i))

        # lastVolume和lastTurnover均不为0
        dfTemp = self.df.loc[lastVol & lastTurn & (Vol | Turn | openIn)]

        # volume、openInterest、turnover均为0，删除并记录
        if dfTemp.loc[Vol & Turn & openIn]._values.any():
            for i in dfTemp.loc[Vol & Turn & openIn].index.values:
                if i not in self.removeList:
                    self.removeList.append(i)
                    self.logList.append(i)
                    logger.info('Vol & openInterest & turn = 0, remove index = %d' % i)

        # turnover为0,lastVol不为0
        for i, row in self.df[Turn & lastVol].iterrows():
            preIndex = i - 1
            if preIndex >= 0 and i not in self.removeList:
                row["turnover"] = self.df.loc[preIndex, "turnover"] + row["lastTurnover"]
                self.df.loc[i, "turnover"] = row["turnover"]
                self.updateList.append(i)
                logger.info('Turn = 0 & lastTurn != 0, update index = %d' % (i))

        # volume为0,lastVol不为0
        for i, row in self.df[Vol & lastVol].iterrows():
            preIndex = i - 1
            if preIndex >= 0 and i not in self.removeList:
                row["volume"] = self.df.loc[preIndex, "volume"] + row["lastVolume"]
                self.df.loc[i, "volume"] = row["volume"]
                self.updateList.append(i)
                logger.info('Vol = 0 & lastVol != 0, update index = %d' % (i))

    @add_log
    def cleanNullOpenInter(self):
        """持仓量为0,用上一个填充"""
        self.paddingWithPrevious("openInterest")

    @add_log
    def cleanNullPriceIndicator(self):
        lastP = self.df["lastPrice"] == 0.0
        high = self.df["highPrice"] == 0.0
        low = self.df["lowPrice"] == 0.0
        bidP = self.df["bidPrice1"] == 0.0
        askP = self.df["askPrice1"] == 0.0
        # 如果均为0，删除
        if self.df.loc[lastP & high & low & bidP & askP]._values.any():
            for i in self.df.loc[lastP & high & low & bidP & askP].index.values:
                if i not in self.removeList:
                    self.removeList.append(i)
                    logger.info('All Price is Null, remove index = %d' % i)

        # 某些为0，填充
        self.paddingWithPrevious("lastPrice")
        self.paddingWithPrevious("highPrice")
        self.paddingWithPrevious("lowPrice")
        self.paddingWithPrevious("bidPrice1")
        self.paddingWithPrevious("askPrice1")

    @add_log
    def recordExceptionalPrice(self):
        self.estimateExceptional("lastPrice")
        self.estimateExceptional("highPrice")
        self.estimateExceptional("lowPrice")
        self.estimateExceptional("bidPrice1")
        self.estimateExceptional("askPrice1")

    def delItemsFromRemove(self):
        indexList = list(set(self.removeList))
        self.df = self.df.drop(indexList,axis=0)

    def estimateExceptional(self,field):
        dfTemp = pd.DataFrame(self.df[field])
        dfTemp["shift"] = self.df[field].shift(1)
        dfTemp["delta"] = abs(dfTemp[field] - dfTemp["shift"])
        dfTemp = dfTemp.dropna(axis=0, how='any')
        dfTemp["IsExcept"] = dfTemp["delta"] >= dfTemp["shift"] * 0.12
        for i, row in dfTemp.loc[dfTemp["IsExcept"]].iterrows():
            if i not in self.removeList:
                self.logList.append(i)
                logger.info('Field = %s, log index = %d' % (field, i))

    def paddingWithPrevious(self,field):
        for i, row in self.df.loc[self.df[field] == 0.0].iterrows():
            if i not in self.removeList:
                preIndex = i - 1
                if preIndex >= 0 and i not in self.removeList:
                    row[field] = self.df.loc[preIndex,field]
                    self.df.loc[i,field] = row[field]
                    self.updateList.append(i)
                    logger.info('Field = %s, update index = %d' % (field, i))

    def StandardizeTimePeriod(self,target):
        tar = str(int(target))
        try:
            tp = self.dfInfo.loc[self.Symbol]["CurrPeriod"]
            time1 = [t for i in tp.split(',') for t in i.split('-')]
            ms = tar[-3:]
            tar = tar[:-3]

            tar = time.strptime(tar, '%H%M%S')
            for i in zip(*([iter(time1)] * 2)):
                start = time.strptime(str(i[0]).strip(), '%H:%M')
                end = time.strptime(str(i[1]).strip(), '%H:%M')
                if self.compare_time(start,end,tar,ms):
                    return True

        except Exception as e:
            print (e)

    def compare_time(self,s1,s2,st,ms):
        """由于time类型没有millisecond，故单取ms进行逻辑判断"""
        if s2 == time.strptime('00:00', '%H:%M'):
            s2 = time.strptime('23:59:61', '%H:%M:%S')
        if st > s1 and st < s2:
            return True
        elif (st == s1 and int(ms) >= 0) or (st == s2 and int(ms) == 0):
            return True
        else:
            return False
