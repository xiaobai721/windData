#-*- coding: utf-8 -*-
'''
数据清洗
'''

from pymongo import MongoClient
import pandas as pd
import time, datetime
import json
import logging
import os


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

    def __init__(self, dfData):
        self.df = dfData
        self.dfInfo = self.loadInformation()
        self.date = datetime.datetime.strptime(str(self.df["Date"][0]), "%Y%m%d")

    def initList(self):
        self.removeList = []
        self.updateList = []
        self.logList = []

    def initCleanRegulation(self):
        dbNew = self.get_db("localhost", 27017, 'test_WIND_TICK_DB')
        i = str(self.df["Code"])
        try:
            print ("start process collection %s........." %(i))
            logger.warning("start process collection %s........." %(i))
            self.Symbol = filter(str.isalpha, str(i)).lower()
            self.initList()
            if not self.df.empty:
                self.cleanIllegalTradingTime()
                self.cleanSameTimestamp()
                self.cleanNullVolTurn()
                self.cleanNullPriceIndicator()
                self.cleanNullOpenInter()
                self.recordExceptionalPrice()

                self.delItemsFromRemove()
                self.insert2db(dbNew,i)
        except Exception as e:
            print (e)
            logger.error(e)

    def get_db(self,host,port,dbName):
        #建立连接
        client = MongoClient(host,port)
        db = client[dbName]
        return db


    def insert2db(self,dbNew,coll_name):

        data = json.loads(self.df.T.to_json(date_format = 'iso')).values()
        # for i in data:
        #     # if isinstance(i["datetime"], unicode):
        #     #     i["datetime"] = datetime.datetime.strptime(i["datetime"], "%Y-%m-%dT%H:%M:%S.%fZ")
        dbNew[coll_name].insert_many(data)

    def loadInformation(self):
        dfInfo = pd.read_csv(os.getcwd() + '/BasicInformation.csv')
        dfInfo.index = dfInfo['Symbol'].tolist()
        del dfInfo['Symbol']
        # 增加对历史周期交易时间段变更的记录
        dfInfo["CurrPeriod"] = dfInfo["TradingPeriod"].map(self.identifyCurrentPeriod)
        return dfInfo

    def identifyCurrentPeriod(self, target):
        if '%' in target:
            phase = [i for i in target.split('%')].sort(reverse=True)
            for i in phase:
                startDate = datetime.datetime.strptime(i.split('||')[0], "%Y-%m-%d")
                if startDate <= self.date:
                    return i.split('||')[1]
                else:
                    continue
        else:
            return target.split('||')[1]

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
    def cleanSameTimestamp(self):
        """清除重复时间戳，记录"""
        dfTemp = self.df.sort_values(by = ['Time'], ascending = False)
        idList = dfTemp[dfTemp["Time"].duplicated()].index
        for i in idList.values:
            self.removeList.append(i)
            logger.info('remove index = %d' % i)

    @add_log
    def cleanNullVolTurn(self):
        """Tick有成交，但volume和turnover为0"""
        f = lambda x: float(x)
        self.df.loc["Volume"] = self.df["Volume"].map(f)
        self.df.loc["Turover"] = self.df["Turover"].map(f)
        self.df.loc["AccVolume"] = self.df["AccVolume"].map(f)
        self.df.loc["AccTurover"] = self.df["AccTurover"].map(f)
        self.df.loc["Interest"] = self.df["Interest"].map(f)
        self.df.loc["Price"] = self.df["Price"].map(f)

        lastVol = self.df["Volume"] != 0.0
        lastTurn = self.df["Turover"] != 0.0
        Vol = self.df["AccVolume"] == 0.0
        Turn = self.df["AccTurover"] == 0.0
        openIn = self.df["Interest"] == 0.0
        lastP = self.df["Price"] != 0.0

        tu = self.dfInfo.loc[self.Symbol]["TradingUnits"]

        # lastTurn为0,lastVolume和lastPrice不为0
        dfTemp = self.df.loc[~lastTurn & lastVol & lastP]
        dfTemp.loc[:,"Turover"] = dfTemp.loc[:,"Volume"] * dfTemp.loc[:,"Price"] * float(tu)
        for i, row in dfTemp.iterrows():
            if i not in self.removeList:
                self.df.loc[i,"Turover"] = row["Turover"]
                self.updateList.append(i)
                logger.info('lastTurn = 0, update index = %d' % (i))

        # lastVolume为0,lastTurnover和lastPrice不为0
        dfTemp = self.df.loc[lastTurn & ~lastVol & lastP]
        dfTemp.loc[:,"Volume"] = dfTemp.loc[:,"Turover"] / (dfTemp.loc[:,"Price"] * float(tu))
        dfTemp["Volume"].map(lambda x:int(round(x)))
        for i, row in dfTemp.iterrows():
            if i not in self.removeList:
                self.df.loc[i,"Volume"] = row["Volume"]
                self.updateList.append(i)
                logger.info('lastVol = 0, update index = %d' % (i))

        # lastPrice为0,lastVolume和lastTurnover不为0
        dfTemp = self.df.loc[lastTurn & lastVol & ~lastP]
        dfTemp.loc[:,"Price"] = dfTemp.loc[:,"Turover"] / (dfTemp.loc[:,"Volume"] * float(tu))
        for i, row in dfTemp.iterrows():
            if i not in self.removeList:
                self.df.loc[i,"Price"] = row["Price"]
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
                row["AccTurover"] = self.df.loc[preIndex,"AccTurover"] + row["Turover"]
                self.df.loc[i,"AccTurover"] = row["AccTurover"]
                self.updateList.append(i)
                logger.info('Turn = 0 & lastTurn != 0, update index = %d' % (i))

        # volume为0,lastVol不为0
        for i,row in self.df[Vol & lastVol].iterrows():
            preIndex = i - 1
            if preIndex >= 0 and i not in self.removeList:
                row["AccVolume"] = self.df.loc[preIndex,"AccVolume"] + row["Volume"]
                self.df.loc[i,"AccVolume"] = row["AccVolume"]
                self.updateList.append(i)
                logger.info('Vol = 0 & lastVol != 0, update index = %d' % (i))

    @add_log
    def cleanNullOpenInter(self):
        """持仓量为0,用上一个填充"""
        self.paddingWithPrevious("Interest")

    @add_log
    def cleanNullPriceIndicator(self):
        lastP = self.df["Price"] == 0.0
        high = self.df["High"] == 0.0
        low = self.df["Low"] == 0.0
        bidP = self.df["BidPrice"] == 0.0
        askP = self.df["AskPrice"] == 0.0
        #如果均为0，删除
        if self.df.loc[lastP & high & low & bidP & askP]._values.any():
            for i in self.df.loc[lastP & high & low & bidP & askP].index.values:
                if i not in self.removeList:
                    self.removeList.append(i)
                    logger.info('All Price is Null, remove index = %d' %i)

        # 某些为0，填充
        self.paddingWithPrevious("Price")
        self.paddingWithPrevious("High")
        self.paddingWithPrevious("Low")
        self.paddingWithPrevious("BidPrice")
        self.paddingWithPrevious("AskPrice")

    @add_log
    def recordExceptionalPrice(self):
        self.estimateExceptional("Price")
        self.estimateExceptional("High")
        self.estimateExceptional("Low")
        self.estimateExceptional("BidPrice")
        self.estimateExceptional("AskPrice")

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
        tar = target
        ms = 0
        try:
            tp = self.dfInfo.loc[self.Symbol]["TradingPeriod"]
            time1 = [t for i in tp.split(',') for t in i.split('-')]
            if '.' in tar:
                ms = tar.split('.')[1]
                tar = tar.split('.')[0]

            tar = time.strptime(tar, '%H:%M:%S')
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
        elif (st == s1 and ms >= 0) or (st == s2 and int(ms) == 0):
            return True
        else:
            return False