# coding=utf-8

"""
所有数据操作类的基类, 定义了数据库操作常用的函数
"""

from sqlalchemy import MetaData, Table
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

class BaseData:
    def __init__(self, engine):
        """
        初始化函数,生成并存储数据库引擎和会话
        :param session: 数据库链接session
        """
        self.engine = engine
        session = scoped_session(sessionmaker(bind=engine))
        self.session = session()

    def commit(self):
        """
        提交实例中现有的数据写入操作
        :return: 无返回值
        """
        self.session.commit()

    def rollback(self):
        """
        回滚实例中现有的数据写入操作
        :return: 无返回值
        """
        self.session.rollback()

    def addRecord(self, obj):
        """
        新增一条记录
        :param obj: 待写入数据库的模型数据实例
        :return:
        """
        self.session.add(obj)

    def truncRecord(self, table_name):
        metadata = MetaData(self.engine)
        table = Table(table_name, metadata, autoload=True)
        s = table.delete()
        self.session.execute(s)

    def getMetaTables(self):
        """
        映射获得当前数据库内的所有表结构信息并返回
        :return: list, [{'name':str, 'descr':str,'status':True, columns:[]}]
        """
        Base = declarative_base()
        Base.metadata.reflect(self.engine)
        tables = Base.metadata.tables
        tables = dict(tables)
        return [e for e in tables.keys()]

    def select(self, table_name,whereclause=None):
        metadata = MetaData(self.engine)
        table = Table(table_name, metadata, autoload=True)
        s = table.select(whereclause=whereclause)
        r = self.session.execute(s)
        return r.fetchall()

    def close(self):
        self.session.close()
