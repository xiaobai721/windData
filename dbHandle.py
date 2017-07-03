'''
数据库相关操作
'''
import datetime, json
import pandas as pd
from pymongo import MongoClient
from module_mylog import gLogger



class dbHandle(object):

    def __init__(self, lock):
        self.lock = lock
    def get_db(self ,host ,port ,dbName):
        # 建立连接
        client = MongoClient(host ,port)
        db = client[dbName]
        return db
    def disconnect_db(self, db):
        #断开连接
        db.close()

    def get_all_colls(self, db):
        return [i for i in db.collection_names()]

    def get_specificItems(self, db, coll_name, time):
        self.lock.acquire()
        Items = db[coll_name].find({"datetime": {'$gte': time}})
        self.lock.release()
        return Items

    def get_specificDayItems(self, db, coll_name, t):
        self.lock.acquire()
        if isinstance(t, datetime.datetime):
            t = t.strftime("%Y%m%d")
        Items = db[coll_name].find({"date": t})
        self.lock.release()
        return Items

    def insert2db(self ,dbNew ,coll_name, df):
        self.lock.acquire()
        if isinstance(df, pd.DataFrame):
            if df.empty:
                # gLogger.error("data trying to insert is empty!")
                self.lock.release()
                return
            data = json.loads(df.T.to_json(date_format = 'iso')).values()
            for i in data:
                if isinstance(i["datetime"], str):
                    i["datetime"] = datetime.datetime.strptime(i["datetime"], "%Y-%m-%dT%H:%M:%S.%fZ")
            dbNew[coll_name].insert_many(data)
            self.lock.release()
        elif isinstance(df, list):
            if len(df) == 0:
                # gLogger.error("data trying to insert is empty!")
                self.lock.release()
                return
            dbNew[coll_name].insert_many(df)
            self.lock.release()
        else:
            gLogger.error("data type trying to insert is not defined, please check!")
            self.lock.release()