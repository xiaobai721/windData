"""
聚合K线，生成相应周期
"""
import time
import pandas as pd
import collections

class AggregateTickData(object):

    def __init__(self, dfInfo):
        # K线对象字典
        tree = lambda: collections.defaultdict(tree)
        self.barDict = {}
        self.splitDict = tree()
        self.dfInfo = dfInfo
        self.Symbol = 'a'

        self.initStart()

    def initStart(self):
        self.genTimeList()

    def genTimeList(self):
        cycle = [1,5,10,15,30,60]
        tempDict = {}
        for c in cycle:
            tempDict[c] = []
            tp = self.dfInfo.loc[self.Symbol]["CurrPeriod"]
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
            self.splitDict[self.Symbol][c] = lst