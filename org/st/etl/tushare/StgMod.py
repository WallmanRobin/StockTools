# coding: utf-8
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy import Column, INTEGER, String, text, TIMESTAMP, MetaData, Text, Float, Index
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = MetaData()

class StockBasic(Base):
    __tablename__ = 'stock_basic'

    ts_code = Column('ts_code', Text, primary_key=True)
    symbol = Column('symbol', Text)
    name = Column('name', Text)
    area = Column('area', Text)
    industry = Column('industry', Text)
    market = Column('market', Text)
    curr_type = Column('curr_type', Text)
    list_status = Column('list_status', Text)
    list_date = Column('list_date', Text)
    delist_date = Column('delist_date', Text)
    is_hs = Column('is_hs', Text)