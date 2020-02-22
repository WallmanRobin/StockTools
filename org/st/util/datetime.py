# coding: utf-8
from datetime import datetime, timedelta

def getLastPeriod(period):
    p = datetime.strptime(period, '%Y%m%d')
    month = (p.month - 1) - (p.month - 1) % 3 + 1
    period_start = datetime(p.year, month, 1)
    last_period_end = period_start - timedelta(days=1)
    return last_period_end.strftime('%Y%m%d')

def getLastMonth(period):
    p = datetime.strptime(period, '%Y%m%d')
    month = (p.month -1) if p.month!=1 else 12
    year = p.year if p.month!=1 else (p.year-1)
    period_start = datetime(year, month, 1)
    return period_start.strftime('%Y%m')

def getListFromPeriod(start_date, end_date, bWeekend=False):
    dt = datetime.strptime(start_date, "%Y%m%d")
    de = datetime.strptime(end_date, "%Y%m%d")
    l = []
    t = dt
    while t <= de:
        wd = t.weekday()
        if (not bWeekend) and (wd==5 or wd==6):
            t = t + timedelta(days=1)
            continue
        l.append(t.strftime("%Y%m%d"))
        t = t + timedelta(days=1)
    return l

def getStrFromDeltaDays(start_date, days):
    dt = datetime.strptime(start_date, "%Y%m%d")
    t = dt + timedelta(days=days)
    return t.strftime("%Y%m%d")

def getWeekday(date):
    dt = datetime.strptime(date, "%Y%m%d")
    return dt.weekday()

if __name__=="__main__":
    print(getListFromPeriod('20200102','20200103'))