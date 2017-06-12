'''
主函数
'''
import os, datetime
import pandas as pd
from loadmat import LoadMatFile
from CleanData import CleanData
from aggressiveTick import AggregateTickData

class Main(object):

    def __init__(self):
        self.root = 'E:\\windDataOriginal'

    def processTickData(self):
        self.fileList = self.parseMatFile()
        for i in self.fileList:
            self.date = datetime.datetime.strptime(i.split('\\')[-1].split('_')[-1][:-4], '%Y%m%d')
            self.dfInfo = self.loadInformation()
            dfData = LoadMatFile(i).dfData
            CleanData(dfData, self.dfInfo)
            AggregateTickData(self.dfInfo, self.date)

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
    ee.processTickData()