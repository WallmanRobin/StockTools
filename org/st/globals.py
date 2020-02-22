# coding=utf-8

"""
rear平台环境的上下文类, 共用资源在此统一定义, 避免重复定义浪费资源, 该类还会通过反射将org.st.uitl.cfg中的配置读取函数映射为类的属性
"""
import types
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.pool import NullPool
from org.st.util import cfg
import tushare as ts

class StockContext():
    def __init__(self):
        self.reflectCfgMoudle()
        self.dbstg_engine = self.initDBStgEngine()
        self.dbseed_engine = self.initDBSeedEngine()
        ts.set_token(self.tushareToken)
        self.tushare_pro = ts.pro_api()

        self.n = 0

    def initDBStgEngine(self):
        j = self.dbStgConf
        connStr = j['type'] + '://' + j['user'] + ':' + j['password'] + '@' + j['host'] + ':' + j['port'] + '/' + j[
            'name']
        #return create_engine(connStr, pool_size=j["pool_size"], max_overflow=j["max_overflow"], pool_recycle=j["pool_recycle"], echo=True, echo_pool=True)
        return create_engine(connStr, poolclass=NullPool, echo=True)

    def initDBSeedEngine(self):
        j = self.dbSeedConf
        connStr = j['type'] + '://' + j['user'] + ':' + j['password'] + '@' + j['host'] + ':' + j['port'] + '/' + j[
            'name']
        #return create_engine(connStr, pool_size=j["pool_size"], max_overflow=j["max_overflow"], pool_recycle=j["pool_recycle"], echo=True, echo_pool=True)
        return create_engine(connStr, poolclass=NullPool, echo=True)

    def reflectCfgMoudle(self):
        l = dir(cfg)
        for e in dir(cfg):
            f = getattr(cfg, e)
            if isinstance(f, types.FunctionType):
                setattr(self, f.__name__, f())

    def addN(self):
        print("begin", str(self.n))
        self.n = self.n + 1
        print("end", str(self.n))