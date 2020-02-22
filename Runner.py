from multiprocessing import Process

from org.st.etl.tushare import StgData
import logging.config
from os import path
from org.st.etl.tushare.StgHnd import StgHnd
from org.st.etl.tushare.ETLHnd import ETLHnd
from org.st import ctx
from org.st.util.datetime import getListFromPeriod, getWeekday, getLastPeriod
import argparse

def stgDailyPriceData(date):
    sh = StgHnd()
    sh.stgDailyPriceData(date)

def stgDailyMarketData(stockBasic, date, runPeriod):
    sh = StgHnd()
    sh.stgDailyMarketData(stockBasic, date, runPeriod)

def stgPeriodDataRun(stockBasic, period, runPeriod):
    sh = StgHnd()
    sh.stgPeriodDataRun(stockBasic, period, runPeriod)

def stgDailyTotalCover(stockBasic, runPeriod):
    sh = StgHnd()
    sh.stgDailyTotalCover(stockBasic, runPeriod)

def stgFundData(date, runPeriod):
    sh = StgHnd()
    sh.stgFundData(date, runPeriod)

def stgIndexData(date, runPeriod):
    sh = StgHnd()
    sh.stgIndexData(date, runPeriod)

def dailyRun(date, runPeriod=False, distinct=True, exclude=[]):
    sh = StgHnd()
    eh = ETLHnd()
    sh.stgStockBasic()
    stockBasic = sh.loadStockBasic(list_status="L")
    period = getLastPeriod(date)

    process_list = []
    p = Process(target=stgDailyPriceData, args=(date,))
    p.start()
    process_list.append(p)

    p = Process(target=stgDailyMarketData, args=(stockBasic, date, runPeriod,))
    p.start()
    process_list.append(p)

    p = Process(target=stgPeriodDataRun, args=(stockBasic, period, runPeriod,))
    p.start()
    process_list.append(p)

    p = Process(target=stgDailyTotalCover, args=(stockBasic, runPeriod,))
    p.start()
    process_list.append(p)

    if "fund" not in exclude:
        p = Process(target=stgFundData, args=(date, runPeriod,))
        p.start()
        process_list.append(p)

    if "index" not in exclude:
        p = Process(target=stgIndexData, args=(date, runPeriod,))
        p.start()
        process_list.append(p)

    for i in process_list:
        i.join()

    eh.bulkLoadHostMetaTables(ctx.dbstg_engine, ctx.dbStgConf['name'], ctx.dbSeedConf['name'], distinct=distinct)

    if runPeriod:
        eh.extractDBDupRows(ctx.dbseed_engine)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="StockTools parameters: yyyyMMdd [-e yyyyMMdd] [-p always/week/last] [-x fund/index] ")
    parser.add_argument("begin_date",type=str,help="Begin trade date")
    parser.add_argument("-e", "--end_date", type=str, nargs=1, default="", help="Last trade date")
    parser.add_argument("-p", "--period", type=str, nargs=1, choices=["always", "week", "last"], help="Run period stage")
    parser.add_argument("-x", "--exclude", type=str, nargs="*", choices=["fund", "index"],
                        help="Run period stage")

    args = parser.parse_args()
    st = args.begin_date if args.begin_date else ""
    et = args.end_date[0] if (args.end_date and len(args.end_date)>0) else st
    p = args.period[0] if (args.period and len(args.period)>0) else ""
    l = getListFromPeriod(st, et)
    x = args.exclude if args.exclude is not None else []

    logging.config.fileConfig(path.join(path.dirname(__file__), 'logging.conf'))

    for e in l:
        period = False
        if p=="always":
            period = True
        elif p=="week":
            d = getWeekday(e)
            if d==4:
                period = True
        elif p=="last" and e==et:
            period = True

        dailyRun(e, runPeriod=period, distinct=True, exclude=x)