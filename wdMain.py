'''
主函数
'''
import os
from loadmat import LoadMatFile
from CleanData import CleanData

class Main(object):

    def __init__(self):
        self.root = 'E:\\windDataOriginal'


    def generateTickData(self):
        self.fileList = self.parseMatFile()
        for i in self.fileList:
            self.LM = LoadMatFile(i)
            dfData = self.LM.convert2df()
            self.CD = CleanData(dfData)

    def parseMatFile(self):
        fileList = []
        for x in os.walk(self.root):
            if len(x[-1]) > 0 and '.mat' in x[-1][0]:
                pre = os.path.basename(x[0])
                for j in x[-1]:
                    fileList.append(self.root + '\\' + pre + '\\' + j)

        return fileList

