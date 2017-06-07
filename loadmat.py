# encoding: UTF-8

'''
实现读取wind行情历史数据.mat格式
'''

import scipy.io as sio
import pandas as pd

matFile = 'E:\windDataOriginal\commodity\\20170531\\al1706\\al1706_20170531.mat'
data = sio.loadmat(matFile)

colNames = data['temp'].dtype.names
seriesNames = locals()
for k,v in enumerate(data['temp'][0][0].tolist()):
    if v.ndim  == 1:
        seriesNames['SN_%s' % k] = pd.Series(v,name = colNames[k])
    elif v.ndim > 1:
        v = v.ravel()
        seriesNames['SN_%s' % k] = pd.Series(v, name=colNames[k])
    else:
        print ("index = %d, dim = %d" %(k, v.ndim))

df_data = pd.concat([a for a in colNames if 'SN_' in a], axis = 1)