# coding: utf-8
from datetime import datetime, timedelta
import logging
import time
from org.st import ctx
from org.st.etl.tushare.StgData import StgData
from org.st.util.db.handler.BaseHandler import BaseHandler
from org.st.util.datetime import getLastPeriod, getStrFromDeltaDays, getLastMonth

class StgHnd(BaseHandler):
    def stgDailyPriceData(self, date):
        logger = logging.getLogger("tushare")
        d = self.getData(StgData, ctx.dbstg_engine)
        i = 0
        while i < 3:
            try:
                d.stgAdjFactor(trade_date=date)
                logger.debug("stgDailyPriceData stgAdjFactor - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyPriceData stgAdjFactor - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgDailyLineData(trade_date=date)
                logger.debug("stgDailyPriceData stgDailyLineData - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyPriceData stgDailyLineData - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgDailyBasic(trade_date=date)
                logger.debug("stgDailyPriceData stgDailyBasic - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyPriceData stgDailyBasic - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgWeeklyLineData(trade_date=date)
                logger.debug("stgDailyPriceData stgWeeklyLineData - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyPriceData stgWeeklyLineData - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgMonthlyLineData(trade_date=date)
                logger.debug("stgDailyPriceData stgMonthlyLineData - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyPriceData stgMonthlyLineData - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgMoneyflow(trade_date=date)
                logger.debug("stgDailyPriceData stgMoneyflow - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyPriceData stgMoneyflow - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgStkLimit(trade_date=date)
                logger.debug("stgDailyPriceData stgStkLimit - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyPriceData stgStkLimit - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgLimitList(trade_date=date)
                logger.debug("stgDailyPriceData stgLimitList - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyPriceData stgLimitList - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgMoneyflowHsgt(trade_date=date)
                logger.debug("stgDailyPriceData stgMoneyflowHsgt - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyPriceData stgMoneyflowHsgt - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgHsgtTop10(trade_date=date)
                logger.debug("stgDailyPriceData stgHsgtTop10 - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyPriceData stgHsgtTop10 - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgHkHold(trade_date=date)
                logger.debug("stgDailyPriceData stgHkHold - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyPriceData stgHkHold - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgGgtDaily(trade_date=date)
                logger.debug("stgDailyPriceData stgGgtDaily - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyPriceData stgGgtDaily - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        d.commit()
        d.close()

    def stgDailyMarketData(self, stockBasic, date, runPeriod=False):
        logger = logging.getLogger("tushare")
        d = self.getData(StgData, ctx.dbstg_engine)
        i = 0
        while i < 3:
            try:
                d.stgGgtTop10(trade_date=date)
                logger.debug("stgDailyMarketData stgGgtTop10 - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyMarketData stgGgtTop10 - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgMargin(trade_date=date)
                logger.debug("stgDailyMarketData stgMargin - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyMarketData stgMargin - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgMarginDetail(trade_date=date)
                logger.debug("stgDailyMarketData stgMarginDetail - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyMarketData stgMarginDetail - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgTopList(trade_date=date)
                logger.debug("stgDailyMarketData stgTopList - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyMarketData stgTopList - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgTopInst(trade_date=date)
                logger.debug("stgDailyMarketData stgTopInst - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyMarketData stgTopInst - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgBlockTrade(trade_date=date)
                logger.debug("stgDailyMarketData stgBlockTrade - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgDailyMarketData stgBlockTrade - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        try:
            d.truncRecord("pledge_stat")
            d.commit()
            logger.debug("stgDailyMarketData pledge_stat truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgDailyMarketData pledge_stat truncate: " + repr(err))
        try:
            d.truncRecord("pledge_detail")
            d.commit()
            logger.debug("stgDailyMarketData pledge_detail truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgDailyMarketData pledge_detail truncate: " + repr(err))
        if runPeriod:
            for e in stockBasic:
                i = 0
                while i < 3:
                    try:
                        d.stgPledgeStat(ts_code=e.ts_code)
                        logger.debug("stgDailyMarketData stgPledgeStat - " + e.ts_code + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error("stgDailyMarketData stgPledgeStat - " + e.ts_code + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgPledgeDetail(ts_code=e.ts_code)
                        logger.debug(
                            "stgDailyMarketData stgPledgeDetail - " + e.ts_code + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error("stgDailyMarketData stgPledgeDetail - " + e.ts_code + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
            end_date = getStrFromDeltaDays(date, 90)
            start_date = getStrFromDeltaDays(date, -30)
            i = 0
            while i < 3:
                try:
                    d.stgShareFloat(start_date=start_date, end_date=end_date)
                    logger.debug(
                        "stgDailyMarketData stgShareFloat - " + start_date + " - " + end_date + " successfuly staged.")
                    i = 3
                except Exception as err:
                    i = i + 1
                    logger.error(
                        "stgDailyMarketData stgShareFloat - " + start_date + " - " + end_date + " : " + repr(err))
                    time.sleep(ctx.tushareSleepTime)
            i = 0
            while i < 3:
                try:
                    d.stgRepurchase(start_date=start_date, end_date=end_date)
                    logger.debug(
                        "stgDailyMarketData stgRepurchase - " + start_date + " - " + end_date + " successfuly staged.")
                    i = 3
                except Exception as err:
                    i = i + 1
                    logger.error(
                        "stgDailyMarketData stgRepurchase - " + start_date + " - " + end_date + " : " + repr(err))
                    time.sleep(ctx.tushareSleepTime)
            i = 0
            while i < 3:
                try:
                    d.stgStkHoldernumber(start_date=start_date, end_date=end_date)
                    logger.debug(
                        "stgDailyMarketData stgStkHoldernumber - " + start_date + " - " + end_date + " successfuly staged.")
                    i = 3
                except Exception as err:
                    i = i + 1
                    logger.error(
                        "stgDailyMarketData stgStkHoldernumber - " + start_date + " - " + end_date + " : " + repr(err))
                    time.sleep(ctx.tushareSleepTime)
            i = 0
            while i < 3:
                try:
                    d.stgStkHolderTrade(start_date=start_date, end_date=end_date)
                    logger.debug(
                        "stgDailyMarketData stgStkHolderTrade - " + start_date + " - " + end_date + " successfuly staged.")
                    i = 3
                except Exception as err:
                    i = i + 1
                    logger.error(
                        "stgDailyMarketData stgStkHolderTrade - " + start_date + " - " + end_date + " : " + repr(err))
                    time.sleep(ctx.tushareSleepTime)
        d.commit()
        d.close()

    def stgPeriodDataRun(self, stockBasic, period, runPeriod=False):
        logger = logging.getLogger("tushare")
        d = self.getData(StgData, ctx.dbstg_engine)
        try:
            d.truncRecord("income")
            d.commit()
            logger.debug("stgPeriodDataRun income truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun income truncate: " + repr(err))
        try:
            d.truncRecord("balancesheet")
            d.commit()
            logger.debug("stgPeriodDataRun balancesheet truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun balancesheet truncate: " + repr(err))
        try:
            d.truncRecord("cashflow")
            d.commit()
            logger.debug("stgPeriodDataRun cashflow truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun cashflow truncate: " + repr(err))
        try:
            d.truncRecord("forecast")
            d.commit()
            logger.debug("stgPeriodDataRun forecast truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun forecast truncate: " + repr(err))
        try:
            d.truncRecord("express")
            d.commit()
            logger.debug("stgPeriodDataRun express truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun express truncate: " + repr(err))
        try:
            d.truncRecord("fina_indicator")
            d.commit()
            logger.debug("stgPeriodDataRun fina_indicator truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun fina_indicator truncate: " + repr(err))
        try:
            d.truncRecord("fina_mainbz")
            d.commit()
            logger.debug("stgPeriodDataRun fina_mainbz truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun fina_mainbz truncate: " + repr(err))
        try:
            d.truncRecord("fina_audit")
            d.commit()
            logger.debug("stgPeriodDataRun fina_audit truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun fina_audit truncate: " + repr(err))
        try:
            d.truncRecord("top10_holders")
            d.commit()
            logger.debug("stgPeriodDataRun top10_holders truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun top10_holders truncate: " + repr(err))
        try:
            d.truncRecord("top10_floatholders")
            d.commit()
            logger.debug("stgPeriodDataRun top10_floatholders truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun top10_floatholders truncate: " + repr(err))
        try:
            d.truncRecord("disclosure_date")
            d.commit()
            logger.debug("stgPeriodDataRun disclosure_date truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun disclosure_date truncate: " + repr(err))
        try:
            d.truncRecord("dividend")
            d.commit()
            logger.debug("stgPeriodDataRun dividend truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun dividend truncate: " + repr(err))
        try:
            d.truncRecord("stk_rewards")
            d.commit()
            logger.debug("stgPeriodDataRun stk_rewards truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgPeriodDataRun stk_rewards truncate: " + repr(err))
        if runPeriod:
            for e in stockBasic:
                i = 0
                while i < 3:
                    try:
                        d.stgStkRewards(ts_code=e.ts_code, end_date=period)
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error("stgPeriodDataRun stgStkRewards" + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgIncome(ts_code=e.ts_code, period=period)
                        logger.debug(
                            "stgPeriodDataRun stgIncome - " + e.ts_code + " - " + period + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error("stgPeriodDataRun stgIncome - " + e.ts_code + " - " + period + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgBalanceSheet(ts_code=e.ts_code, period=period)
                        logger.debug(
                            "stgPeriodDataRun stgBalanceSheet - " + e.ts_code + " - " + period + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error(
                            "stgPeriodDataRun stgBalanceSheet - " + e.ts_code + " - " + period + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgCashflow(ts_code=e.ts_code, period=period)
                        logger.debug(
                            "stgPeriodDataRun stgCashflow - " + e.ts_code + " - " + period + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error("stgPeriodDataRun stgCashflow - " + e.ts_code + " - " + period + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgForecast(ts_code=e.ts_code, period=period)
                        logger.debug(
                            "stgPeriodDataRun stgForecast - " + e.ts_code + " - " + period + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error("stgPeriodDataRun stgForecast - " + e.ts_code + " - " + period + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgExpress(ts_code=e.ts_code, period=period)
                        logger.debug(
                            "stgPeriodDataRun stgExpress - " + e.ts_code + " - " + period + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error("stgPeriodDataRun stgExpress - " + e.ts_code + " - " + period + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgFinaIndicator(ts_code=e.ts_code, period=period)
                        logger.debug(
                            "stgPeriodDataRun stgFinaIndicator - " + e.ts_code + " - " + period + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error(
                            "stgPeriodDataRun stgFinaIndicator - " + e.ts_code + " - " + period + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgFinaMainbz(ts_code=e.ts_code, period=period)
                        logger.debug(
                            "stgPeriodDataRun stgFinaMainbz - " + e.ts_code + " - " + period + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error(
                            "stgPeriodDataRun stgFinaMainbz - " + e.ts_code + " - " + period + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgFinaAudit(ts_code=e.ts_code, period=period)
                        logger.debug(
                            "stgPeriodDataRun stgFinaAudit - " + e.ts_code + " - " + period + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error(
                            "stgPeriodDataRun stgFinaAudit - " + e.ts_code + " - " + period + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgTop10Holders(ts_code=e.ts_code, period=period)
                        logger.debug(
                            "stgPeriodDataRun stgTop10Holders - " + e.ts_code + " - " + period + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error(
                            "stgPeriodDataRun stgTop10Holders - " + e.ts_code + " - " + period + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgTop10FloatHolders(ts_code=e.ts_code, period=period)
                        logger.debug(
                            "stgPeriodDataRun stgTop10FloatHolders - " + e.ts_code + " - " + period + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error(
                            "stgPeriodDataRun stgTop10FloatHolders - " + e.ts_code + " - " + period + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgDisclosureDate(ts_code=e.ts_code, end_date=period)
                        logger.debug(
                            "stgPeriodDataRun stgDisclosureDate - " + e.ts_code + " - " + period + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error(
                            "stgPeriodDataRun stgDisclosureDate - " + e.ts_code + " - " + period + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
                i = 0
                while i < 3:
                    try:
                        d.stgDividend(ts_code=e.ts_code)
                        logger.debug("stgPeriodDataRun stgDividend - " + e.ts_code + " successfuly staged.")
                        i = 3
                    except Exception as err:
                        i = i + 1
                        logger.error("stgPeriodDataRun stgDividend - " + e.ts_code + " : " + repr(err))
                        time.sleep(ctx.tushareSleepTime)
            d.commit()
        d.close()

    def stgDailyTotalCover(self, stockBasic, runPeriod=False):
        logger = logging.getLogger("tushare")
        d = self.getData(StgData, ctx.dbstg_engine)
        try:
            d.truncRecord("concept")
            d.commit()
            logger.debug("stgDailyTotalCover concept truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgDailyTotalCover concept truncate: " + repr(err))
        try:
            d.truncRecord("concept_detail")
            d.commit()
            logger.debug("stgDailyTotalCover concept_detail truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgDailyTotalCover concept_detail truncate: " + repr(err))
        try:
            d.truncRecord("hs_const")
            d.commit()
            logger.debug("stgDailyTotalCover hs_const truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgDailyTotalCover hs_const truncate: " + repr(err))
        try:
            d.truncRecord("stk_managers")
            d.commit()
            logger.debug("stgDailyTotalCover stk_managers truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgDailyTotalCover stk_managers truncate: " + repr(err))
        if runPeriod:
            i = 0
            while i < 3:
                try:
                    d.stgHsConst("SH")
                    i = 3
                    logger.debug("stgDailyTotalCover stgHsConst - SH successfuly staged.")
                except Exception as err:
                    i = i + 1
                    logger.error("stgDailyTotalCover stgHsConst SH: " + repr(err))
                    time.sleep(ctx.tushareSleepTime)
            d.commit()
            i = 0
            while i < 3:
                try:
                    d.stgHsConst("SZ")
                    i = 3
                    logger.debug("stgDailyTotalCover stgHsConst - SZ successfuly staged.")
                except Exception as err:
                    i = i + 1
                    logger.error("stgDailyTotalCover stgHsConst SZ: " + repr(err))
                    time.sleep(ctx.tushareSleepTime)
            d.commit()
            s = time.strftime("%Y%m%d", time.localtime())
            end_date = getStrFromDeltaDays(s, 90)
            start_date = getStrFromDeltaDays(s, -30)
            i = 0
            while i < 3:
                try:
                    d.stgNewShare(start_date, end_date)
                    i = 3
                    logger.debug(
                        "stgDailyTotalCover stgNewShare - " + start_date + " " + end_date + " successfuly staged.")
                except Exception as err:
                    i = i + 1
                    logger.error("stgDailyTotalCover stgNewShare" + repr(err))
                    time.sleep(ctx.tushareSleepTime)
            d.commit()
            for e in stockBasic:
                i = 0
                while i < 3:
                    try:
                        d.stgStkManagers(ts_code=e.ts_code)
                        i = 3
                        logger.debug("stgDailyTotalCover stgStkManagers - " + e.ts_code + " successfuly staged.")
                    except Exception as err:
                        i = i + 1
                        logger.error("stgDailyTotalCover stgStkManagers" + repr(err))
                        time.sleep(ctx.tushareSleepTime)
            d.commit()

            i = 0
            while i < 3:
                try:
                    d.stgConcept()
                    d.commit()
                    logger.debug("stgDailyTotalCover stgConcept successfuly staged.")
                    r = d.select('concept')
                    if len(r) > 0:
                        for e in r:
                            j = 0
                            while j < 3:
                                try:
                                    d.stgConceptDetail(id=e[0])
                                    logger.debug(
                                        "stgDailyTotalCover stgConceptDetail  - " + e[0] + " successfuly staged.")
                                    j = 3
                                except Exception as err:
                                    j = j + 1
                                    logger.error(
                                        "stgDailyTotalCover stgConcept stgConceptDetail - " + e[0] + " : " + repr(err))
                                    time.sleep(ctx.tushareSleepTime)
                    i = 3
                except Exception as err:
                    i = i + 1
                    logger.error("stgDailyTotalCover stgConcept : " + repr(err))
                    time.sleep(ctx.tushareSleepTime)
            d.commit()
        d.close()

    def stgIndexData(self, date, runPeriod):
        logger = logging.getLogger("tushare")
        d = self.getData(StgData, ctx.dbstg_engine)
        # try:
        #     d.truncRecord("index_member")
        #     d.commit()
        #     logger.debug("stgIndexData index_member truncated.")
        # except Exception as err:
        #     d.rollback()
        #     logger.error("stgIndexData index_member truncate: " + repr(err))
        try:
            d.truncRecord("index_basic")
            d.commit()
            logger.debug("stgIndexData index_basic truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgIndexData index_basic truncate: " + repr(err))
        try:
            d.truncRecord("index_daily")
            d.commit()
            logger.debug("stgIndexData index_daily truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgIndexData index_daily truncate: " + repr(err))
        try:
            d.truncRecord("index_dailybasic")
            d.commit()
            logger.debug("stgIndexData index_dailybasic truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgIndexData index_dailybasic truncate: " + repr(err))
        i = 0
        while i < 3:
            try:
                d.stgIndexBasic('SSE')
                d.stgIndexBasic('SZSE')
                logger.debug("stgIndexData stgIndexBasic successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgIndexData stgIndexBasic" + repr(err))
                time.sleep(ctx.tushareSleepTime)
        d.commit()
        r = d.select('index_basic')
        for e in r:
            i = 0
            while i < 3:
                try:
                    d.stgIndexDaily(ts_code=e[0], trade_date=date)
                    i = 3
                    logger.debug("stgIndexData stgIndexDaily - " + e[0] + " " + date + " successfuly staged.")
                except Exception as err:
                    i = i + 1
                    logger.error("stgIndexData stgIndexDaily - " + e[0] + " " + date + ":" + repr(err))
                    time.sleep(ctx.tushareSleepTime)
            i = 0
            while i < 3:
                try:
                    d.stgIndexDailyBasic(ts_code=e[0], trade_date=date)
                    i = 3
                    logger.debug("stgIndexData stgIndexDailyBasic - " + e[0] + " " + date + " successfuly staged.")
                except Exception as err:
                    i = i + 1
                    logger.error("stgIndexData stgIndexDailyBasic - " + e[0] + " " + date + ":" + repr(err))
                    time.sleep(ctx.tushareSleepTime)
        d.commit()
        if runPeriod:
            i = 0
            while i < 3:
                try:
                    d.stgIndexWeekly(trade_date=date)
                    logger.debug("stgIndexData stgIndexWeekly - " + date + " successfuly staged.")
                    i = 3
                except Exception as err:
                    i = i + 1
                    logger.error("stgIndexData stgIndexWeekly - " + date + ":" + repr(err))
                    time.sleep(ctx.tushareSleepTime)
            d.commit()
            i = 0
            while i < 3:
                try:
                    d.stgIndexMonthly(trade_date=date)
                    logger.debug("stgIndexData stgIndexMonthly - " + date + " successfuly staged.")
                    i = 3
                except Exception as err:
                    i = i + 1
                    logger.error("stgIndexData stgIndexMonthly - " + date + ":" + repr(err))
                    time.sleep(ctx.tushareSleepTime)
            d.commit()
            i = 0
            while i < 3:
                try:
                    d.stgIndexWeight(trade_date=date)
                    logger.debug("stgIndexData stgIndexWeight - " + date + " successfuly staged.")
                    i = 3
                except Exception as err:
                    i = i + 1
                    logger.error("stgIndexData stgIndexWeight - " + date + ":" + repr(err))
                    time.sleep(ctx.tushareSleepTime)
            d.commit()
            # i = 0
            # while i < 3:
            #     try:
            #         d.stgIndexClassify()
            #         logger.debug("stgIndexData stgIndexClassify successfuly staged.")
            #         i = 3
            #     except Exception as err:
            #         i = i + 1
            #         logger.error("stgIndexData stgIndexClassify: " + repr(err))
            #         time.sleep(ctx.tushareSleepTime)
            # d.commit()
            # r = d.select('index_classify')
            # if len(r) > 0:
            #     for e in r:
            #         i = 0
            #         while i < 3:
            #             try:
            #                 d.stgIndexMember(index_code=e[0])
            #                 i = 3
            #                 logger.debug("stgIndexData stgIndexMember - " + e[0] +" successfuly staged.")
            #             except Exception as err:
            #                 i = i + 1
            #                 logger.error("stgIndexData stgIndexMember - " + e[0] + ":" + repr(err))
            #                 time.sleep(ctx.tushareSleepTime)
            # d.commit()
        d.close()

    def stgFundData(self, date, runPeriod):
        logger = logging.getLogger("tushare")
        d = self.getData(StgData, ctx.dbstg_engine)
        try:
            d.truncRecord("fund_portfolio")
            d.commit()
            logger.debug("stgFundData fund_portfolio truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgFundData fund_portfolio truncate: " + repr(err))
        try:
            d.truncRecord("fund_daily")
            d.commit()
            logger.debug("stgFundData fund_daily truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgFundData fund_daily truncate: " + repr(err))
        try:
            d.truncRecord("fund_adj")
            d.commit()
            logger.debug("stgFundData fund_adj truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgFundData fund_adj truncate: " + repr(err))
        try:
            d.truncRecord("fund_basic")
            d.commit()
            logger.debug("stgFundData fund_basic truncated.")
        except Exception as err:
            d.rollback()
            logger.error("stgFundData fund_basic truncate: " + repr(err))
        i = 0
        while i < 3:
            try:
                d.stgFundBasic('E')
                logger.debug("stgFundData stgFundBasic - E successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgFundData stgFundBasic - E" + repr(err))
                time.sleep(ctx.tushareSleepTime)
        d.commit()
        i = 0
        while i < 3:
            try:
                d.stgFundBasic('O')
                i = 3
                logger.debug("stgFundData stgFundBasic - O successfuly staged.")
            except Exception as err:
                i = i + 1
                logger.error("stgFundData stgFundBasic - O" + repr(err))
                time.sleep(ctx.tushareSleepTime)
        d.commit()
        i = 0
        while i < 3:
            try:
                d.stgFundDaily(trade_date=date)
                logger.debug("stgFundData stgFundDaily  - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgFundData stgFundDaily - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        i = 0
        while i < 3:
            try:
                d.stgFundAdj(trade_date=date)
                logger.debug("stgFundData stgFundAdj  - " + date + " successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgFundData stgFundAdj - " + date + " : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        d.commit()
        i = 0
        while i < 3:
            try:
                d.stgFundNav(end_date=date)
                i = 3
                logger.debug("stgFundData stgFundNav successfuly staged.")
            except Exception as err:
                i = i + 1
                logger.error("stgFundData stgFundNav : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        d.commit()
        i = 0
        while i < 3:
            try:
                d.stgFundDiv(ann_date=date)
                i = 3
                logger.debug("stgFundData stgFundDiv successfuly staged.")
            except Exception as err:
                i = i + 1
                logger.error("stgFundData stgFundDiv : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        d.commit()

        if runPeriod:
            i = 0
            while i < 3:
                try:
                    d.stgFundCompany()
                    i = 3
                    logger.debug("stgFundData stgFundCompany successfuly staged.")
                except Exception as err:
                    i = i + 1
                    logger.error("stgFundData stgFundCompany : " + repr(err))
                    time.sleep(ctx.tushareSleepTime)
            d.commit()
            r = d.select('fund_basic')
            if len(r) > 0:
                for e in r:
                    i = 0
                    while i < 3:
                        try:
                            d.stgFundPortfolio(ts_code=e[0])
                            logger.debug("stgFundData stgFundPortfolio  - " + e[0] + " successfuly staged.")
                            i = 3
                        except Exception as err:
                            i = i + 1
                            logger.error("stgFundData stgFundPortfolio - " + e[0] + " : " + repr(err))
                            time.sleep(ctx.tushareSleepTime)
            d.commit()
        d.close()

    def stgStockBasic(self):
        logger = logging.getLogger("tushare")
        d = self.getData(StgData, ctx.dbstg_engine)
        i = 0
        while i < 3:
            try:
                d.stgStockBasic()
                logger.debug("stgStockBasic successfuly staged.")
                i = 3
            except Exception as err:
                i = i + 1
                logger.error("stgStockBasic : " + repr(err))
                time.sleep(ctx.tushareSleepTime)
        d.commit()
        d.close()

    def loadStockBasic(self, ts_code="", list_status="", whereClause=""):
        d = self.getData(StgData, ctx.dbstg_engine)
        l = d.loadStockBasic(ts_code=ts_code, list_status=list_status, whereClause=whereClause)
        d.close()
        return l

    def dailyRun(self, date="", runPeriod=False):
        s = date
        if s == "":
            s = time.strftime("%Y%m%d", time.localtime())
        d = self.getData(StgData, ctx.dbstg_engine)
        d.stgStockBasic()
        d.commit()
        l = d.loadStockBasic(list_status="L")
        d.close()
        p = getLastPeriod(s)
        self.stgDailyPriceData(s)
        self.stgDailyMarketData(l, s, runPeriod)
        self.stgPeriodDataRun(l, p, runPeriod)
        self.stgDailyTotalCover(l, runPeriod)
        self.stgFundData(date, runPeriod)
        self.stgIndexData(date, runPeriod)

    def customRun(self):
        d = self.getData(StgData, ctx.dbstg_engine)
        d.stgFundPortfolio(ts_code="159801.SZ")
        d.commit()

if __name__=="__main__":
    h = StgHnd()
    h.customRun()
