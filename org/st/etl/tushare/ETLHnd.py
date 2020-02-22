# coding=utf-8
import logging

from org.st import ctx
from org.st.etl.tushare.ETLData import ETLData


class ETLHnd:
    def loadRemoteTable(self, source_engine, target_engine, source_table_name, target_table_name="", distinct=True):
        d = ETLData()
        d.loadRemoteTable(source_engine, target_engine, source_table_name, target_table_name, distinct)

    def bulkLoadRemoteTable(self, source_engine, target_engine, source_table_name, target_table_name="", distinct=True):
        d = ETLData()
        d.bulkLoadRemoteTable(source_engine, target_engine, source_table_name, target_table_name, distinct)

    def bulkLoadHostTable(self, source_engine, source_schema, target_schema, source_table_name, target_table_name="",
                          distinct=True):
        logger = logging.getLogger("tushare")
        d = ETLData()
        r = d.bulkLoadHostTable(source_engine, source_schema, target_schema, source_table_name=source_table_name,
                                target_table_name=target_table_name, distinct=distinct)
        if r == 0:
            logger.debug("bulkLoadHostMetaTables bulkLoadHostTable - " + source_table_name + " successfuly loaded.")

    def bulkLoadHostMetaTables(self, source_engine, source_schema, target_schema, distinct=True):
        logger = logging.getLogger("tushare")
        d = ETLData()
        l = d.getMetaTables(source_engine)
        for e in l:
            r = d.bulkLoadHostTable(source_engine, source_schema, target_schema, source_table_name=e,
                                    target_table_name=e, distinct=distinct)
            if r == 0:
                logger.debug("bulkLoadHostMetaTables bulkLoadHostTable - " + e + " successfuly loaded.")

    def loadRemoteMetaTables(self, source_engine, target_engine, distinct=True):
        logger = logging.getLogger("tushare")
        d = ETLData()
        l = d.getMetaTables(source_engine)
        for e in l:
            try:
                d.loadRemoteTable(source_engine, target_engine, source_table_name=e, target_table_name=e,
                                  distinct=distinct)
                logger.debug("loadRemoteMetaTables loadRemoteTable - " + e + " successfuly loaded.")
            except Exception as err:
                logger.error("loadRemoteMetaTables loadRemoteTable - " + e + " : " + repr(err))

    def bulkLoadRemoteMetaTables(self, source_engine, target_engine, distinct=True):
        logger = logging.getLogger("tushare")
        d = ETLData()
        l = d.getMetaTables(source_engine)
        for e in l:
            r = d.bulkLoadRemoteTable(source_engine, target_engine, source_table_name=e, target_table_name=e,
                                      distinct=distinct)
            if r == 0:
                logger.debug("loadRemoteMetaTables loadRemoteTable - " + e + " successfuly loaded.")

    def bulkLoadRemoteListTables(self, source_engine, target_engine, l, distinct=True):
        logger = logging.getLogger("tushare")
        d = ETLData()
        for e in l:
            r = d.bulkLoadRemoteTable(source_engine, target_engine, source_table_name=e, target_table_name=e,
                                      distinct=distinct)
            if r == 0:
                logger.debug("loadRemoteListTables loadTable - " + e + " successfuly loaded.")

    def extractDBDupRows(self, engine):
        logger = logging.getLogger("tushare")
        d = ETLData()
        l = d.getMetaTables(engine)
        t = []
        t.extend(l)
        for e in t:
            try:
                self.extractDupRows(engine, e)
                logger.debug("extractDBDupRows extractDupRows - " + e + " successfuly loaded.")
            except Exception as err:
                logger.error("extractDBDupRows extractDupRows - " + e + " : " + repr(err))

    def extractDupRows(self, engine, table_name):
        logger = logging.getLogger("tushare")
        d = ETLData()
        target_table_name = table_name + '_' + ctx.dbStgConf['clone_suffix']
        r = d.dropTable(engine, target_table_name)
        if r != 0:
            return -1
        r = d.createTableAsCopy(engine, source_table_name=table_name, target_table_name=target_table_name)
        if not r:
            return -2
        r = d.bulkLoadLocalTable(engine, table_name, target_table_name, True)
        if r != 0:
            return -3
        r = d.dropTable(engine, table_name)
        if r != 0:
            return -4
        r = d.createTableAsCopy(engine, source_table_name=target_table_name, target_table_name=table_name)
        if not r:
            return -5
        r = d.bulkLoadLocalTable(engine, target_table_name, table_name, True)
        if r != 0:
            return -6
        r = d.dropTable(engine, target_table_name)
        if r != 0:
            return -7
        logger.debug("extractDupRows successed - " + table_name)
        return 0
