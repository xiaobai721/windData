#-*- coding: utf-8 -*-
'''
数据清洗
'''

import pandas as pd
import time, datetime
import copy
from dbHandle import dbHandle
from module_mylog import gLogger
from parseConfig import getConfig

class CleanData(object):

    def __init__(self, dfData, dfInfo, aucTime, lock):
        self.df = dfData
        self.date = datetime.datetime.strptime(self.df["date"][0], "%Y%m%d")
        self.dfInfo = dfInfo
        self.db = dbHandle(lock)
        self.AucTime = aucTime
        self.initCleanRegulation()

    def initList(self):
        self.removeList = []
        self.updateList = []
        self.logList = []

    def initCleanRegulation(self):
        gLogger.info("start initCleanRegulation")
        dbNew = self.db.get_db(getConfig("database", "dbhost"), int(getConfig("database", "dbport")), getConfig("database", "db_tick"))
        j = self.df["vtSymbol"][0]
        try:
            if "IFC" in j or "IHC" in j or "ICC" in j or "TFC" in j:
                i = j[:2]
                self.Symbol = "".join([a for a in i if a.isalpha()]).lower()
            else:
                self.Symbol = "".join([a for a in j if a.isalpha()]).lower()
            self.initList()
            if not self.df.empty:
                self.cleanIllegalTradingTime()
                self.cleanSameTimestamp()
                self.reserveLastTickInAuc()
                self.cleanNullStartPrice()
                # self.cleanNullVolTurn()
                self.cleanNullPriceIndicator()
                self.cleanNullOpenInter()
                # self.recordExceptionalPrice()

                self.delItemsFromRemove()
                self.db.insert2db(dbNew,j, self.df)
            gLogger.info("finish clean data with %s" %j)
        except Exception as e:
            gLogger.exception("Exception: %s" %e)

    def cleanIllegalTradingTime(self):
        """删除非交易时段数据"""
        try:
            gLogger.info("start cleanIllegalTradingTime ")
            self.df['illegalTime'] = self.df["time"].map(self.StandardizeTimePeriod)
            self.df['illegalTime'] = self.df['illegalTime'].fillna(False)
            orilen = len(self.removeList)
            for i,row in self.df[self.df['illegalTime'] == False].iterrows():
                self.removeList.append(i)
            if len(self.removeList) > orilen:
                gLogger.warning("cleanIllegalTradingTime remove len = %d" %(len(self.removeList)-orilen))
            del self.df["illegalTime"]
        except Exception as e:
            gLogger.exception("Exception: %s" %e)

    def reserveLastTickInAuc(self):
        """保留集合竞价期间最后一个tick数据"""
        try:
            gLogger.info("start reserveLastTickInAuc")
            self.df["structTime"] = self.df["time"].map(lambda x: datetime.datetime.strptime(x, "%H%M%S%f"))
            orilen = len(self.removeList)
            tp = self.dfInfo.loc[self.Symbol]["CurrPeriod"]
            for st in self.AucTime:
                if st in [t.strip() for i in tp.split(',') for t in i.split('-')]:
                    start = datetime.datetime.strptime(st, '%H:%M')
                    end =  start + datetime.timedelta(minutes=1)
                    p1 = self.df["structTime"] >= start
                    p2 = self.df["structTime"] < end
                    dfTemp = self.df.loc[p1 & p2]
                    dfTemp.sort(columns = "structTime", ascending=False, inplace = True)
                    for i in dfTemp.index.values[1:]:
                        self.removeList.append(i)
            if len(self.removeList) > orilen:
                gLogger.warning("reserveLastTickInAuc remove len = %d" %(len(self.removeList)-orilen))
            del self.df["structTime"]
        except Exception as e:
            gLogger.exception("Exception : %s" %e)


    def cleanSameTimestamp(self):
        """清除重复时间戳，记录"""
        try:
            gLogger.info("start cleanSameTimestamp")
            dfTemp = copy.copy(self.df)
            dfTemp["num"] = dfTemp.index
            dfTemp = dfTemp.sort(columns = "num", ascending=False)
            idList = dfTemp[dfTemp["datetime"].duplicated()].index
            orilen = len(self.removeList)
            for i in idList.values:
                if i not in self.removeList:
                    self.removeList.append(i)
            del dfTemp["num"]
            if len(self.removeList) > orilen:
                gLogger.warning('cleanSameTimestamp remove len = %d' %(len(self.removeList)-orilen))
        except Exception as e:
            gLogger.exception("Exception : %s" %e)

    def cleanNullStartPrice(self):
        """起始价格连续为0，删除"""
        try:
            gLogger.info("start cleanNullStartPrice")
            orilen = len(self.removeList)
            self.df.sort(columns="datetime", ascending=True, inplace=True)
            for i, row in self.df.iterrows():
                if row["lastPrice"] == 0:
                    if i not in self.removeList:
                        self.removeList.append(i)
                else:
                    break
            if len(self.removeList) > orilen:
                gLogger.warning('cleanNullStartPrice remove len = %d' % (len(self.removeList) - orilen))
        except Exception as e:
            gLogger.exception("Exception : %s" % e)

    def cleanNullVolTurn(self):
        """Tick有成交，但volume和turnover为0"""
        gLogger.info("start cleanNullVolTurn")
        f = lambda x: float(x)
        self.df["lastVolume"] = self.df["lastVolume"].map(f)
        self.df["lastTurnover"] = self.df["lastTurnover"].map(f)
        self.df["volume"] = self.df["volume"].map(f)
        self.df["turnover"] = self.df["turnover"].map(f)
        self.df["openInterest"] = self.df["openInterest"].map(f)
        self.df["lastPrice"] = self.df["lastPrice"].map(f)

        lastVol = self.df["lastVolume"] != 0.0
        lastTurn = self.df["lastTurnover"] != 0.0
        Vol = self.df["volume"] == 0.0
        Turn = self.df["turnover"] == 0.0
        openIn = self.df["openInterest"] == 0.0
        lastP = self.df["lastPrice"] != 0.0

        # tu = self.dfInfo.loc[self.Symbol]["TradingUnits"]

        # lastTurn为0,lastVolume和lastPrice不为0
        # dfTemp = self.df.loc[~lastTurn & lastVol & lastP]
        # if not dfTemp.empty:
        #     gLogger.debug("process data that lastTurn is null but lastVol and lastP are not")
        #     dfTemp["lastTurnover"] = dfTemp["lastVolume"] * dfTemp["lastPrice"] * float(tu)
        #     for i, row in dfTemp.iterrows():
        #         if i not in self.removeList:
        #             self.df.loc[i, "lastTurnover"] = row["lastTurnover"]
        #             self.updateList.append(i)
        #             gLogger.debug('lastTurn = 0, update index = %d' % (i))
        #
        # # lastVolume为0,lastTurnover和lastPrice不为0
        # dfTemp = self.df.loc[lastTurn & ~lastVol & lastP]
        # if not dfTemp.empty:
        #     dfTemp["lastVolume"] = dfTemp["lastTurnover"] / (dfTemp["lastPrice"] * float(tu))
        #     dfTemp["lastVolume"].map(lambda x: int(round(x)))
        #     for i, row in dfTemp.iterrows():
        #         if i not in self.removeList:
        #             self.df.loc[i, "lastVolume"] = row["lastVolume"]
        #             self.updateList.append(i)
        #             gLogger.debug('lastVol = 0, update index = %d' % (i))
        #
        # # lastPrice为0,lastVolume和lastTurnover不为0
        # dfTemp = self.df.loc[lastTurn & lastVol & ~lastP]
        # if not dfTemp.empty:
        #     dfTemp["lastPrice"] = dfTemp["lastTurnover"] / (dfTemp["lastVolume"] * float(tu))
        #     for i, row in dfTemp.iterrows():
        #         if i not in self.removeList:
        #             self.df.loc[i, "lastPrice"] = row["lastPrice"]
        #             self.updateList.append(i)
        #             gLogger.debug('lastPrice = 0, update index = %d' % (i))

        # lastVolume和lastTurnover均不为0
        dfTemp = self.df.loc[lastVol & lastTurn & (Vol | Turn | openIn)]
        if not dfTemp.empty:
            # volume、openInterest、turnover均为0，删除并记录
            if dfTemp.loc[Vol & Turn & openIn]._values.any():
                for i in dfTemp.loc[Vol & Turn & openIn].index.values:
                    if i not in self.removeList:
                        self.removeList.append(i)
                        self.logList.append(i)
                        gLogger.debug('Vol & openInterest & turn = 0, remove index = %d' % i)

            # turnover为0,lastVol不为0
            for i, row in self.df[Turn & lastVol].iterrows():
                preIndex = i - 1
                if preIndex >= 0 and i not in self.removeList:
                    row["turnover"] = self.df.loc[preIndex, "turnover"] + row["lastTurnover"]
                    self.df.loc[i, "turnover"] = row["turnover"]
                    self.updateList.append(i)
                    gLogger.debug('Turn = 0 & lastTurn != 0, update index = %d' % (i))

            # volume为0,lastVol不为0
            for i, row in self.df[Vol & lastVol].iterrows():
                preIndex = i - 1
                if preIndex >= 0 and i not in self.removeList:
                    row["volume"] = self.df.loc[preIndex, "volume"] + row["lastVolume"]
                    self.df.loc[i, "volume"] = row["volume"]
                    self.updateList.append(i)
                    gLogger.debug('Vol = 0 & lastVol != 0, update index = %d' % (i))

    def cleanNullOpenInter(self):
        """持仓量为0,用上一个填充"""
        gLogger.info("start cleanNullOpenInter")
        self.paddingWithPrevious("openInterest")

    def cleanNullPriceIndicator(self):
        gLogger.info("start cleanNullPriceIndicator")
        lastP = self.df["lastPrice"] == 0.0
        high = self.df["highPrice"] == 0.0
        low = self.df["lowPrice"] == 0.0
        bidP = self.df["bidPrice1"] == 0.0
        askP = self.df["askPrice1"] == 0.0

        # 如果均为0，删除
        oriLen = len(self.removeList)
        if self.df.loc[lastP & high & low & bidP & askP]._values.any():
            # gLogger.debug("process data that all price indicators are null")
            for i in self.df.loc[lastP & high & low & bidP & askP].index.values:
                if i not in self.removeList:
                    self.removeList.append(i)
        if len(self.removeList) > oriLen:
            gLogger.warning('All Price is Null, remove len = %d' % (len(self.removeList)-oriLen))

        # 某些为0，填充
        self.paddingWithPrevious("lastPrice")
        self.paddingWithPrevious("highPrice")
        self.paddingWithPrevious("lowPrice")
        self.paddingWithPrevious("bidPrice1")
        self.paddingWithPrevious("askPrice1")

    def recordExceptionalPrice(self):
        # gLogger.info("start recordExceptionalPrice")
        self.estimateExceptional("lastPrice")
        self.estimateExceptional("highPrice")
        self.estimateExceptional("lowPrice")
        self.estimateExceptional("bidPrice1")
        self.estimateExceptional("askPrice1")

    def delItemsFromRemove(self):
        try:
            gLogger.info("start delItemsFromRemove")
            indexList = list(set(self.removeList))
            self.df = self.df.drop(indexList,axis=0)
        except Exception as e:
            gLogger.exception("Exception : %s" %e)

    def estimateExceptional(self,field):
        try:
            # gLogger.info("start estimateExceptional, field = %s" %field)
            dfTemp = pd.DataFrame(self.df[field])
            dfTemp["shift"] = self.df[field].shift(1)
            dfTemp["delta"] = abs(dfTemp[field] - dfTemp["shift"])
            dfTemp = dfTemp.dropna(axis=0, how='any')
            dfTemp["IsExcept"] = dfTemp["delta"] >= dfTemp["shift"] * 0.12
            for i, row in dfTemp.loc[dfTemp["IsExcept"]].iterrows():
                if i not in self.removeList:
                    self.logList.append(i)
                    gLogger.debug('Field = %s, log index = %d' % (field, i))
        except Exception as e:
            gLogger.exception("Exception : %s" %e)


    def paddingWithPrevious(self,field):
        try:
            # gLogger.info("start paddingWithPrevious, field = %s" %field)
            for i, row in self.df.loc[self.df[field] == 0.0].iterrows():
                if i not in self.removeList:
                    preIndex = i - 1
                    if preIndex >= 0 and i not in self.removeList:
                        row[field] = self.df.loc[preIndex,field]
                        self.df.loc[i,field] = row[field]
                        self.updateList.append(i)
                        gLogger.debug('Field = %s, update index = %d' % (field, i))
        except Exception as e:
            gLogger.exception("Exception : %s" %e)

    def StandardizeTimePeriod(self,target):
        tar = target
        try:
            tp = self.dfInfo.loc[self.Symbol]["CurrPeriod"]
            time1 = [t.strip() for i in tp.split(',') for t in i.split('-')]
            ms = tar[-3:]
            tar = tar[:-3]

            tar = time.strptime(tar, '%H%M%S')
            for i in zip(*([iter(time1)] * 2)):
                start = time.strptime(str(i[0]).strip(), '%H:%M')
                end = time.strptime(str(i[1]).strip(), '%H:%M')
                if self.compare_time(start,end,tar,ms):
                    return True
        except Exception as e:
            gLogger.exception("Exception when StandardizeTimePeriod e = %s time = %s" %(e,str(tar)))

    def compare_time(self,s1,s2,st,ms):
        """由于time类型没有millisecond，故单取ms进行逻辑判断"""
        try:
            if s2 == time.strptime('00:00', '%H:%M'):
                s2 = time.strptime('23:59:61', '%H:%M:%S')
            if st > s1 and st < s2:
                return True
            elif (st == s1 and int(ms) >= 0) or (st == s2 and int(ms) == 0):
                return True
            else:
                return False
        except Exception as e:
            gLogger.exception("Exception when compare_time e = %s" %e)
