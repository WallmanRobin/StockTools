#!/usr/bin/python
# coding=utf-8
import os, json
from os import path

def cfgUrl():
    """
    从当前目录回溯找到配置文件
    :return: str, 配置文件的路径
    """
    cfgfile = ''
    d = __file__
    for i in range(4):
        d = path.dirname(d)
    cfgfile = os.path.join(d, 'config.json')
    if not os.path.isfile(cfgfile):
        cfgfile = 'config.json'
    return cfgfile

def dbStgConf():
    """
    读取配置文件中配置的Stage数据库的配置信息
    :return: Dict, 数据库配置信息的json对象1
    """
    cfgfile = cfgUrl()
    with open(cfgfile, 'r') as f:
        return json.load(f)['database']['stage']

def dbSeedConf():
    """
    读取配置文件中配置的Ware数据库部分的配置信息
    :return: Dict, 数据库配置信息的json对象1
    """
    cfgfile = cfgUrl()
    with open(cfgfile, 'r') as f:
        return json.load(f)['database']['seed']

def tushareToken():
    """
    读取配置文件中的Tushare token密钥
    :return: str, Tushare token密钥
    """
    cfgfile = cfgUrl()
    with open(cfgfile, 'r') as f:
        return json.load(f)['tushare']['token']

def tushareSleepTime():
    """
    读取配置文件中的Tushare token密钥
    :return: str, Tushare token密钥
    """
    cfgfile = cfgUrl()
    with open(cfgfile, 'r') as f:
        return json.load(f)['tushare']['sleep_time']