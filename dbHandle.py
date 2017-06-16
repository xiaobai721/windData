'''
数据库相关操作
'''
import datetime, json
import pandas as pd
from pymongo import MongoClient
from module_mylog import gLogger



class dbHandle(object):

    def __init__(self):
        pass
    def get_db(self ,host ,port ,dbName):
        # 建立连接
        client = MongoClient(host ,port)
        db = client[dbName]
        return db

    def get_all_colls(self, db):
        return [i for i in db.collection_names()]

    def get_specificItems(self, db, coll_name, time):
        Items = db[coll_name].find({"datetime": {'$gte': time}})
        return Items

    def get_specificDayItems(self, db, coll_name, time):
        Items = db[coll_name].find({"date": time})
        return Items

    def insert2db(self ,dbNew ,coll_name, df):
        if isinstance(df, pd.DataFrame):
            data = json.loads(df.T.to_json(date_format = 'iso')).values()
            for i in data:
                if isinstance(i["datetime"], str):
                    i["datetime"] = datetime.datetime.strptime(i["datetime"], "%Y-%m-%dT%H:%M:%S.%fZ")
            dbNew[coll_name].insert_many(data)
        elif isinstance(df, list):
            dbNew[coll_name].insert_many(df)
        else:
            gLogger.error("data type trying to insert is not defined, please check!")