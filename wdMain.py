'''
主函数
'''
import os, datetime, time
import pandas as pd
import logging.config
from loadmat import LoadMatFile
from CleanData import CleanData
from aggressiveTick import AggregateTickData
from module_mylog import gLogger

class Main(object):

    def __init__(self):
        self.root = 'E:\\windDataOriginal'
        self.dateList = []
        self.AucTime = ['08:59', '20:59', '09:29', '09:14']

    def processTickData(self):
        self.fileList = self.parseMatFile()
        self.fileList = ["E:\\windDataOriginal\\commodity\\20170531\\bb1805\\bb1805_20170531.mat"]
        for i in self.fileList:
            sym = i.split('\\')[-2]
            if "SP-" in sym or "SPC-" in sym or "IMCI" in sym :
                continue
            gLogger.info("start process tick data —— %s" %i)
            self.date = datetime.datetime.strptime(i.split('\\')[-1].split('_')[-1][:-4], '%Y%m%d')
            self.dateList.append(self.date)
            dfInfo = self.loadInformation()
            dfData = LoadMatFile(i).dfData
            CleanData(dfData, dfInfo, self.AucTime)

    def parse2CycleData(self):
        self.dateList = [datetime.datetime(2017, 5, 31, 0, 0),datetime.datetime(2017, 6, 1, 0, 0),datetime.datetime(2017, 6, 2, 0, 0)]
        for i in list(set(self.dateList)):
            gLogger.info("start parse cycle data —— %s" % i)
            self.date = i
            dfInfo = self.loadInformation()
            AggregateTickData(dfInfo, i, self.AucTime)

    def parseMatFile(self):
        fileList = []
        for x in os.walk(self.root):
            if len(x[-1]) > 0 and '.mat' in x[-1][0]:
                for j in x[-1]:
                    fileList.append(x[0] + '\\' + j)
        return fileList

    def loadInformation(self):
        dfInfo = pd.read_csv(os.getcwd() + '/BasicInformation.csv')
        dfInfo.index = dfInfo['Symbol'].tolist()
        del dfInfo['Symbol']
        # 增加对历史周期交易时间段变更的记录
        dfInfo["CurrPeriod"] = dfInfo["TradingPeriod"].map(self.identifyCurrentPeriod)
        return dfInfo

    def identifyCurrentPeriod(self, target):
        if '%' in target:
            phase = [i for i in target.split('%')]
            phase.sort(reverse=True)
            for i in phase:
                startDate = datetime.datetime.strptime(i.split('||')[0], "%Y-%m-%d")
                if startDate <= self.date:
                    return i.split('||')[1]
                else:
                    continue
        else:
            return target.split('||')[1]

if __name__ == '__main__':
    ee = Main()
    # ee.processTickData()
    ee.parse2CycleData()
