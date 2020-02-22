# coding=utf-8
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import logging

from org.st import ctx


class ETLData:
    def getSession(self, engine):
        session = sessionmaker(bind=engine)
        return session()

    def loadRemoteTable(self, source_engine, target_engine, source_table_name, target_table_name="", distinct=True):
        logger = logging.getLogger("tushare")
        target_session = None
        target_table_name = target_table_name if target_table_name != "" else source_table_name
        try:
            source_metadata = MetaData(source_engine)
            target_metadata = MetaData(target_engine)
            source_table = Table(source_table_name, source_metadata, autoload=True)
            target_table = Table(target_table_name, target_metadata, autoload=True)
            s = source_table.select(distinct=distinct)
            logger.debug("loadRemoteTable select sql: " + str(s))
            source_session = self.getSession(source_engine)
            r = source_session.execute(s)
            r = r.fetchall()
        except Exception as err:
            logger.error("loadRemoteTable error - " + source_table_name + " - " + target_table_name + " : " + repr(err))
            source_session.close()
            return -1
        i = target_table.insert()
        logger.debug("loadRemoteTable insert sql: " + str(i))
        target_session = self.getSession(target_engine)
        for l in r:
            try:
                target_session.execute(i, [l])
                target_session.commit()
            except Exception as err:
                logger.error(
                    "loadRemoteTable error - " + source_table_name + " - " + target_table_name + " - " + str(l) + " : " + repr(err))
        # 源数据库表需要commit释放Metadata资源，避免后面使用该表时发生死锁
        source_session.commit()
        source_session.close()
        target_session.commit()
        target_session.close()
        return 0

    def bulkLoadRemoteTable(self, source_engine, target_engine, source_table_name, target_table_name="", distinct=True):
        logger = logging.getLogger("tushare")
        target_session = None
        target_table_name = target_table_name if target_table_name != "" else source_table_name
        re = 0
        try:
            source_metadata = MetaData(source_engine)
            target_metadata = MetaData(target_engine)
            source_table = Table(source_table_name, source_metadata, autoload=True)
            target_table = Table(target_table_name, target_metadata, autoload=True)
            s = source_table.select(distinct=distinct)
            logger.debug("loadRemoteTable select sql: " + str(s))
            source_session = self.getSession(source_engine)
            r = source_session.execute(s)
            r = r.fetchall()
            source_session.close()
            i = target_table.insert()
            logger.debug("bulkLoadRemoteTable insert sql: " + str(i))
            target_session = self.getSession(target_engine)
            if len(r) > 0:
                target_session.execute(i, r)
                target_session.commit()
        except Exception as err:
            logger.error("bulkLoadRemoteTable error - " + source_table_name + " - " + target_table_name + " : " + repr(err))
            re = -1
        if target_session:
            target_session.close()
        return re

    def bulkLoadHostTable(self, source_engine, source_schema, target_schema, source_table_name, target_table_name="", distinct=True):
        logger = logging.getLogger("tushare")
        target_table_name = target_table_name if target_table_name != "" else source_table_name
        re = 0
        session = self.getSession(source_engine)
        try:
            s = "insert into " + target_schema + "." + target_table_name + " select "+ ("distinct " if distinct else " ") +"* from " +  source_schema + "." + target_table_name
            logger.debug("bulkLoadHostTable sql: " + s)
            session.execute(s)
            session.commit()
        except Exception as err:
            logger.error("bulkLoadHostTable error - " + source_schema + " " + target_schema + " " + source_table_name + "  " + target_table_name + " : " + repr(err))
            re = -1
        session.close()
        return re

    def loadLocalTable(self, engine, source_table_name, target_table_name="", distinct=False):
        logger = logging.getLogger("tushare")
        target_table_name = target_table_name if target_table_name != "" else source_table_name
        session = self.getSession(engine)
        try:
            metadata = MetaData(engine)
            source_table = Table(source_table_name, metadata, autoload=True)
            target_table = Table(target_table_name, metadata, autoload=True)
            s = source_table.select(distinct=distinct)
            logger.debug("loadLocalTable select sql: " + str(s))
            r = session.execute(s)
            r = r.fetchall()
        except Exception as err:
            logger.error("loadLocalTable error - " + source_table_name + " - " + target_table_name + " : " + repr(err))
            session.close()
            return -1
        i = target_table.insert()
        logger.debug("loadLocalTable insert sql: " + str(i))
        for l in r:
            try:
                session.execute(i, [l])
                session.commit()
            except Exception as err:
                logger.error(
                    "loadLocalTable error - " + source_table_name + " - " + target_table_name + " - " + str(l) + " : " + repr(err))
        session.close()
        return 0

    def bulkLoadLocalTable(self, engine, source_table_name, target_table_name="", distinct=False):
        logger = logging.getLogger("tushare")
        target_table_name = target_table_name if target_table_name != "" else source_table_name
        session = self.getSession(engine)
        re = 0
        try:
            metadata = MetaData(engine)
            source_table = Table(source_table_name, metadata, autoload=True)
            target_table = Table(target_table_name, metadata, autoload=True)
            s = source_table.select(distinct=distinct)
            logger.debug("bulkLoadLocalTable select sql: " + str(s))

            i = "insert into " + target_table_name + " " + str(s)
            logger.debug("bulkLoadLocalTable insert sql: " + str(i))
            session.execute(i)
            session.commit()
        except Exception as err:
            logger.error("bulkLoadLocalTable error - " + source_table_name + " - " + target_table_name + " : " + repr(err))
            re = -1
        session.close()
        return re

    def getMetaTables(self, engine):
        """
        映射获得当前数据库内的所有表结构信息并返回
        :return: list, [{'name':str, 'descr':str,'status':True, columns:[]}]
        """
        metadata = MetaData(engine)
        metadata.reflect(engine)
        tables = metadata.tables
        tables = dict(tables)
        return [e for e in tables.keys()]

    def createTableAsCopy(self, engine, source_table_name, target_table_name=""):
        logger = logging.getLogger("tushare")
        if target_table_name=="":
            target_table_name = source_table_name + "_" + ctx.dbStgConf['clone_suffix']
        try:
            metadata = MetaData(engine, reflect=True)
            source_table = Table(source_table_name, metadata)
            target_table = Table(target_table_name, metadata)
            for column in source_table.columns:
                target_table.append_column(column.copy())
            target_table.create()
            logger.debug("createTableAsCopy successed : " + source_table_name + " - " + target_table_name)
        except Exception as err:
            logger.error("createTableAsCopy error - " + source_table_name + " - " + target_table_name + " : " + repr(err))
            return None
        return target_table_name

    def dropTable(self, engine, table_name):
        logger = logging.getLogger("tushare")
        try:
            metadata = MetaData(engine)
            table = Table(table_name, metadata)
            table.drop(checkfirst=True)
            logger.debug("dropTable successed : " + table_name)
        except Exception as err:
            logger.error("dropTable error - " + table_name + " : " + repr(err))
            return -1
        return 0
