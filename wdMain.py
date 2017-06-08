'''
主函数
'''
import os
from loadmat import LoadMatFile
from CleanData import CleanData
from aggressiveTick import AggregateTickData

class Main(object):

    def __init__(self):
        self.root = 'E:\\windDataOriginal'

    def parse2cycleData(self):
        self.aggTick = AggregateTickData(self.dfInfo)

    def generateTickData(self):
        self.fileList = self.parseMatFile()
        for i in self.fileList:
            self.LM = LoadMatFile(i)
            dfData = self.LM.convert2df()
            self.CD = CleanData(dfData)
            self.dfInfo = self.CD.dfInfo
            # self.CD.initCleanRegulation()
            self.parse2cycleData()

    def parseMatFile(self):
        fileList = []
        for x in os.walk(self.root):
            if len(x[-1]) > 0 and '.mat' in x[-1][0]:
                for j in x[-1]:
                    fileList.append(x[0] + '\\' + j)

        return fileList


if __name__ == '__main__':
    ee = Main()
    ee.generateTickData()
