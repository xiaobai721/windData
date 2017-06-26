'''
主函数
'''
import os, datetime, time, multiprocessing
import pandas as pd
from loadmat import LoadMatFile
from CleanData import CleanData
from agg import AggregateTickData
from module_mylog import gLogger
from parseConfig import getConfig
import mul_process_package

class Main(object):

    def __init__(self):
        self.root = getConfig("dataOriginal", "rootPath")
        self.dateList = []
        self.AucTime = ['08:59', '20:59', '09:29', '09:14']


    def processTickData(self):
        self.fileList = self.parseMatFile()
        filterList = getConfig("filter", "s").split(",")
        p = multiprocessing.Pool(int(getConfig("numOfProcesses", "numP")))
        manager = multiprocessing.Manager()
        work_queue = manager.Queue()
        done_queue = manager.Queue()
        lock = manager.Lock()
        for i in self.fileList:
            sym = i.split('\\')[-2]
            fg = True
            for s in filterList:
                if s.strip() in sym:
                    fg = False
                    break
            if fg:
                gLogger.info("start process tick data —— %s" %i)
                self.date = datetime.datetime.strptime(i.split('\\')[-1].split('_')[-1][:-4], '%Y%m%d')
                self.dateList.append(self.date)
                dfInfo = self.loadInformation()
                v = (i, sym, dfInfo)
                work_queue.put(v)
                while (work_queue.full()):
                    gLogger.critical("work queue is fill, waiting......")
                    time.sleep(1)
                if not work_queue.empty():
                    p.apply_async(self.oninit, args=(work_queue, done_queue, lock,))
                    work_queue.put('STOP')

        p.close()
        p.join()
        done_queue.put('STOP')
        for status in iter(done_queue.get_nowait, 'STOP'):
            gLogger.warning(status)

    def oninit(self, work_queue, done_queue, lock):
        for v in iter(work_queue.get_nowait, 'STOP'):
            try:
                i = v[0]
                vtSymbol = v[1]
                dfInfo = v[2]
                gLogger.info('Run task %s (%s)...' % (vtSymbol, os.getpid()))
                dfData = LoadMatFile(i, lock).dfData
                CleanData(dfData, dfInfo, self.AucTime, lock)
                done_queue.put("%s process has done!" %vtSymbol)
                time.sleep(1)
            except Exception as e:
                done_queue.put("failed on  process with %s!" %e)
        return True


    def parse2CycleData(self):
        for i in list(set(self.dateList)):
            gLogger.info("start parse cycle data —— %s" % i)
            self.date = i
            dfInfo = self.loadInformation()
            AggregateTickData(dfInfo, i, self.AucTime)

    def parseMatFile(self):
        fileList = []
        getConfig("model", "mod")
        if getConfig("model", "mod") != '1':
            gLogger.info("parse daily data!")
            strDate = datetime.datetime.today().strftime("%Y%m%d")
        else:
            strDate = ''

        for x in os.walk(self.root):
            if len(x[-1]) > 0 and '.mat' in x[-1][0] and strDate in x[-1][0]:
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
    multiprocessing.freeze_support()
    ee = Main()
    ee.processTickData()
    ee.parse2CycleData()
