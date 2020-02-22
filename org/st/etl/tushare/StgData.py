# coding=utf-8
from sqlalchemy import text
from sqlalchemy.orm import aliased

from org.st import ctx
from org.st.etl.tushare.StgMod import StockBasic
from org.st.util.db.data.BaseData import BaseData

class StgData(BaseData):
    def stgTradeCal(self, start_dt, end_dt, if_exists='append'):
        fields = 'exchange,cal_date,is_open,pretrade_date'
        if start_dt!="" and end_dt != "":
            df = ctx.tushare_pro.trade_cal(start_dt=start_dt, end_dt=end_dt,fields=fields)
        elif start_dt != "":
            df = ctx.tushare_pro.trade_cal(start_dt=start_dt, fields=fields)
        else:
            df = ctx.tushare_pro.trade_cal(fields=fields)
        df.to_sql('trade_cal', self.engine, index=False, if_exists=if_exists)

    def loadStockBasic(self, ts_code="", list_status="", whereClause=""):
        l = []
        try:
            r = aliased(StockBasic, name='r')
            qry = self.session.query(r)
            if ts_code!="":
                qry = qry.filter(r.ts_code==ts_code)
            if list_status!="":
                qry = qry.filter(r.list_status == list_status)
            if whereClause!="":
                qry = qry.filter(text(whereClause))
            l = qry.order_by(r.ts_code).all()
        except Exception as err:
            print(str(err))
        return l

    def stgDailyLineData(self, ts_code="", trade_date="", start_date="", end_date="", if_exists='replace'):
        if ts_code=="" and trade_date=="":
            return
        elif ts_code!="" and trade_date!="":
            df = ctx.tushare_pro.daily(ts_code=ts_code, trade_date=trade_date)
        elif ts_code!="":
            if start_date!="" and end_date!="":
                df = ctx.tushare_pro.daily(ts_code=ts_code, start_date=start_date , end_date=end_date)
            else:
                df = ctx.tushare_pro.daily(ts_code=ts_code)
        elif trade_date != "":
            df = ctx.tushare_pro.daily(trade_date=trade_date)
        df.to_sql('daily', self.engine, index=False, if_exists=if_exists)

    def stgWeeklyLineData(self, ts_code="", trade_date="", start_date="", end_date="", if_exists='replace'):
        if ts_code == "" and trade_date == "":
            return
        elif ts_code != "" and trade_date != "":
            df = ctx.tushare_pro.weekly(ts_code=ts_code, trade_date=trade_date)
        elif ts_code != "":
            if start_date != "" and end_date != "":
                df = ctx.tushare_pro.weekly(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.weekly(ts_code=ts_code)
        elif trade_date != "":
            df = ctx.tushare_pro.weekly(trade_date=trade_date)
        df.to_sql('weekly', self.engine, index=False, if_exists=if_exists)

    def stgMonthlyLineData(self, ts_code="", trade_date="", start_date="", end_date="", if_exists='replace'):
        if ts_code == "" and trade_date == "":
            return
        elif ts_code != "" and trade_date != "":
            df = ctx.tushare_pro.monthly(ts_code=ts_code, trade_date=trade_date)
        elif ts_code != "":
            if start_date != "" and end_date != "":
                df = ctx.tushare_pro.monthly(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.monthly(ts_code=ts_code)
        elif trade_date != "":
            df = ctx.tushare_pro.monthly(trade_date=trade_date)
        df.to_sql('monthly', self.engine, index=False, if_exists=if_exists)

    def stgStockBasic(self, if_exists='replace'):
        df = ctx.tushare_pro.stock_basic(fields='ts_code,symbol,name,area,industry,market,curr_type,is_hs,list_status,list_date,delist_date')
        df.to_sql('stock_basic', self.engine, index=False, if_exists=if_exists)

    def stgAdjFactor(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code!="":
            if trade_date!="":
                df = ctx.tushare_pro.adj_factor(ts_code=ts_code, trade_date=trade_date)
            elif start_date!="" and end_date!="":
                df = ctx.tushare_pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.adj_factor(ts_code=ts_code)
        elif trade_date!="":
            df = ctx.tushare_pro.adj_factor(trade_date=trade_date)
        df.to_sql('adj_factor', self.engine, index=False, if_exists=if_exists)

    def stgDailyBasic(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code!="":
            if trade_date!="":
                df = ctx.tushare_pro.daily_basic(ts_code=ts_code, trade_date=trade_date)
            elif start_date!="" and end_date!="":
                df = ctx.tushare_pro.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.daily_basic(ts_code=ts_code)
        elif trade_date!="":
            df = ctx.tushare_pro.daily_basic(trade_date=trade_date)
        df.to_sql('daily_basic', self.engine, index=False, if_exists=if_exists)

    def stgIncome(self, ts_code, period="", start_date="", end_date="", if_exists="append"):
        fields = 'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,update_flag'
        if period!="":
            df = ctx.tushare_pro.income(ts_code=ts_code, period=period, fields=fields)
        elif start_date!="" and end_date!="":
            df = ctx.tushare_pro.income(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
        else:
            df = ctx.tushare_pro.income(ts_code=ts_code, fields=fields)
        df.to_sql('income', self.engine, index=False, if_exists=if_exists)

    def stgBalanceSheet(self, ts_code, period="", start_date="", end_date="", if_exists="append"):
        fields = 'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,total_share,cap_rese,undistr_porfit,surplus_rese,special_rese,money_cap,trad_asset,notes_receiv,accounts_receiv,oth_receiv,prepayment,div_receiv,int_receiv,inventories,amor_exp,nca_within_1y,sett_rsrv,loanto_oth_bank_fi,premium_receiv,reinsur_receiv,reinsur_res_receiv,pur_resale_fa,oth_cur_assets,total_cur_assets,fa_avail_for_sale,htm_invest,lt_eqt_invest,invest_real_estate,time_deposits,oth_assets,lt_rec,fix_assets,cip,const_materials,fixed_assets_disp,produc_bio_assets,oil_and_gas_assets,intan_assets,r_and_d,goodwill,lt_amor_exp,defer_tax_assets,decr_in_disbur,oth_nca,total_nca,cash_reser_cb,depos_in_oth_bfi,prec_metals,deriv_assets,rr_reins_une_prem,rr_reins_outstd_cla,rr_reins_lins_liab,rr_reins_lthins_liab,refund_depos,ph_pledge_loans,refund_cap_depos,indep_acct_assets,client_depos,client_prov,transac_seat_fee,invest_as_receiv,total_assets,lt_borr,st_borr,cb_borr,depos_ib_deposits,loan_oth_bank,trading_fl,notes_payable,acct_payable,adv_receipts,sold_for_repur_fa,comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,oth_payable,acc_exp,deferred_inc,st_bonds_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,non_cur_liab_due_1y,oth_cur_liab,total_cur_liab,bond_payable,lt_payable,specific_payables,estimated_liab,defer_tax_liab,defer_inc_non_cur_liab,oth_ncl,total_ncl,depos_oth_bfi,deriv_liab,depos,agency_bus_liab,oth_liab,prem_receiv_adva,depos_received,ph_invest,reser_une_prem,reser_outstd_claims,reser_lins_liab,reser_lthins_liab,indept_acc_liab,pledge_borr,indem_payable,policy_div_payable,total_liab,treasury_share,ordin_risk_reser,forex_differ,invest_loss_unconf,minority_int,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,lt_payroll_payable,oth_comp_income,oth_eqt_tools,oth_eqt_tools_p_shr,lending_funds,acc_receivable,st_fin_payable,payables,hfs_assets,hfs_sales,update_flag'
        if period!="":
            df = ctx.tushare_pro.balancesheet(ts_code=ts_code, period=period, fields=fields)
        elif start_date!="" and end_date!="":
            df = ctx.tushare_pro.balancesheet(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
        else:
            df = ctx.tushare_pro.balancesheet(ts_code=ts_code, fields=fields)
        df.to_sql('balancesheet', self.engine, index=False, if_exists=if_exists)

    def stgCashflow(self, ts_code, period="", start_date="", end_date="", if_exists="append"):
        fields = 'ts_code,ann_date,f_ann_date,end_date,comp_type,report_type,net_profit,finan_exp,c_fr_sale_sg,recp_tax_rends,n_depos_incr_fi,n_incr_loans_cb,n_inc_borr_oth_fi,prem_fr_orig_contr,n_incr_insured_dep,n_reinsur_prem,n_incr_disp_tfa,ifc_cash_incr,n_incr_disp_faas,n_incr_loans_oth_bank,n_cap_incr_repur,c_fr_oth_operate_a,c_inf_fr_operate_a,c_paid_goods_s,c_paid_to_for_empl,c_paid_for_taxes,n_incr_clt_loan_adv,n_incr_dep_cbob,c_pay_claims_orig_inco,pay_handling_chrg,pay_comm_insur_plcy,oth_cash_pay_oper_act,st_cash_out_act,n_cashflow_act,oth_recp_ral_inv_act,c_disp_withdrwl_invest,c_recp_return_invest,n_recp_disp_fiolta,n_recp_disp_sobu,stot_inflows_inv_act,c_pay_acq_const_fiolta,c_paid_invest,n_disp_subs_oth_biz,oth_pay_ral_inv_act,n_incr_pledge_loan,stot_out_inv_act,n_cashflow_inv_act,c_recp_borrow,proc_issue_bonds,oth_cash_recp_ral_fnc_act,stot_cash_in_fnc_act,free_cashflow,c_prepay_amt_borr,c_pay_dist_dpcp_int_exp,incl_dvd_profit_paid_sc_ms,oth_cashpay_ral_fnc_act,stot_cashout_fnc_act,n_cash_flows_fnc_act,eff_fx_flu_cash,n_incr_cash_cash_equ,c_cash_equ_beg_period,c_cash_equ_end_period,c_recp_cap_contrib,incl_cash_rec_saims,uncon_invest_loss,prov_depr_assets,depr_fa_coga_dpba,amort_intang_assets,lt_amort_deferred_exp,decr_deferred_exp,incr_acc_exp,loss_disp_fiolta,loss_scr_fa,loss_fv_chg,invest_loss,decr_def_inc_tax_assets,incr_def_inc_tax_liab,decr_inventories,decr_oper_payable,incr_oper_payable,others,im_net_cashflow_oper_act,conv_debt_into_cap,conv_copbonds_due_within_1y,fa_fnc_leases,end_bal_cash,beg_bal_cash,end_bal_cash_equ,beg_bal_cash_equ,im_n_incr_cash_equ,update_flag'
        if period!="":
            df = ctx.tushare_pro.cashflow(ts_code=ts_code, period=period, fields=fields)
        elif start_date!="" and end_date!="":
            df = ctx.tushare_pro.cashflow(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
        else:
            df = ctx.tushare_pro.cashflow(ts_code=ts_code, fields=fields)
        df.to_sql('cashflow', self.engine, index=False, if_exists=if_exists)

    def stgForecast(self, ts_code, period="", start_date="", end_date="", if_exists="append"):
        fields='ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min,net_profit_max,last_parent_net,first_ann_date,summary,change_reason'
        if period!="":
            df = ctx.tushare_pro.forecast(ts_code=ts_code, period=period, fields=fields)
        elif start_date!="" and end_date!="":
            df = ctx.tushare_pro.forecast(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
        else:
            df = ctx.tushare_pro.forecast(ts_code=ts_code, fields=fields)
        df.to_sql('forecast', self.engine, index=False, if_exists=if_exists)

    def stgExpress(self, ts_code, period="", start_date="", end_date="", if_exists="append"):
        fields='ts_code,ann_date,end_date,revenue,operate_profit,total_profit,n_income,total_assets,total_hldr_eqy_exc_min_int,diluted_eps,diluted_roe,yoy_net_profit,bps,yoy_sales,yoy_op,yoy_tp,yoy_dedu_np,yoy_eps,yoy_roe,growth_assets,yoy_equity,growth_bps,or_last_year,op_last_year,tp_last_year,np_last_year,eps_last_year,open_net_assets,open_bps,perf_summary,is_audit,remark'
        if period!="":
            df = ctx.tushare_pro.express(ts_code=ts_code, period=period, fields=fields)
        elif start_date!="" and end_date!="":
            df = ctx.tushare_pro.express(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
        else:
            df = ctx.tushare_pro.express(ts_code=ts_code, fields=fields)
        df.to_sql('express', self.engine, index=False, if_exists=if_exists)

    def stgDividend(self, ts_code, ann_date="", if_exists="append"):
        fields='ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,imp_ann_date,base_date'
        if ann_date!="":
            df = ctx.tushare_pro.dividend(ts_code=ts_code, period=ann_date, fields=fields)
        else:
            df = ctx.tushare_pro.dividend(ts_code=ts_code, fields=fields)
        df.to_sql('dividend', self.engine, index=False, if_exists=if_exists)

    def stgFinaIndicator(self, ts_code, period="", start_date="", end_date="", if_exists="append"):
        fields='ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp'
        if period!="":
            df = ctx.tushare_pro.fina_indicator(ts_code=ts_code, period=period, fields=fields)
        elif start_date!="" and end_date!="":
            df = ctx.tushare_pro.fina_indicator(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
        else:
            df = ctx.tushare_pro.fina_indicator(ts_code=ts_code, fields=fields)
        df.to_sql('fina_indicator', self.engine, index=False, if_exists=if_exists)

    def stgFinaAudit(self, ts_code, period="", start_date="", end_date="", if_exists="append"):
        fields='ts_code,ann_date,end_date,audit_result,audit_fees,audit_agency,audit_sign'
        if period!="":
            df = ctx.tushare_pro.fina_audit(ts_code=ts_code, period=period, fields=fields)
        elif start_date!="" and end_date!="":
            df = ctx.tushare_pro.fina_audit(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
        else:
            df = ctx.tushare_pro.fina_audit(ts_code=ts_code, fields=fields)
        df.to_sql('fina_audit', self.engine, index=False, if_exists=if_exists)

    def stgFinaMainbz(self, ts_code, period="", start_date="", end_date="", if_exists="append"):
        fields = 'ts_code,end_date,bz_item,bz_sales,bz_profit,bz_cost,curr_type,update_flag'
        if period != "":
            df = ctx.tushare_pro.fina_mainbz(ts_code=ts_code, period=period, fields=fields)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.fina_mainbz(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
        else:
            df = ctx.tushare_pro.fina_mainbz(ts_code=ts_code, fields=fields)
        df.to_sql('fina_mainbz', self.engine, index=False, if_exists=if_exists)

    def stgDisclosureDate(self, ts_code="", end_date="", if_exists="append"):
        if end_date != "":
            df = ctx.tushare_pro.disclosure_date(ts_code=ts_code, end_date=end_date)
        else:
            df = ctx.tushare_pro.disclosure_date(ts_code=ts_code)
        df.to_sql('disclosure_date', self.engine, index=False, if_exists=if_exists)

    def stgGgtTop10(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code !="":
            if trade_date!="":
                df = ctx.tushare_pro.ggt_top10(ts_code=ts_code, trade_date=trade_date)
            elif start_date!="" and end_date!="":
                df = ctx.tushare_pro.ggt_top10(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.ggt_top10(ts_code=ts_code)
        elif trade_date!="":
            df = ctx.tushare_pro.ggt_top10(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.ggt_top10(start_date=start_date, end_date=end_date)
        df.to_sql('ggt_top10', self.engine, index=False, if_exists=if_exists)

    def stgMargin(self, trade_date="", start_date="", end_date="", if_exists="replace"):
        if trade_date!="":
            df = ctx.tushare_pro.margin(trade_date=trade_date)
        elif start_date!="" and end_date!="":
            df = ctx.tushare_pro.margin(start_date=start_date, end_date=end_date)
        df.to_sql('margin', self.engine, index=False, if_exists=if_exists)

    def stgMarginDetail(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code !="":
            if trade_date!="":
                df = ctx.tushare_pro.margin_detail(ts_code=ts_code, trade_date=trade_date)
            elif start_date!="" and end_date!="":
                df = ctx.tushare_pro.margin_detail(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.margin_detail(ts_code=ts_code)
        elif trade_date!="":
            df = ctx.tushare_pro.margin_detail(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.margin_detail(start_date=start_date, end_date=end_date)
        df.to_sql('margin_detail', self.engine, index=False, if_exists=if_exists)

    def stgTop10Holders(self, ts_code, period="", start_date="", end_date="", if_exists="append"):
        if ts_code != "":
            if period != "":
                df = ctx.tushare_pro.top10_holders(ts_code=ts_code, period=period)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.top10_holders(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.top10_holders(ts_code=ts_code)
        elif period != "":
            df = ctx.tushare_pro.top10_holders(period=period)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.top10_holders(start_date=start_date, end_date=end_date)
        df.to_sql('top10_holders', self.engine, index=False, if_exists=if_exists)

    def stgTop10FloatHolders(self, ts_code="", period="", start_date="", end_date="", if_exists="append"):
        if ts_code != "":
            if period != "":
                df = ctx.tushare_pro.top10_floatholders(ts_code=ts_code, period=period)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.top10_floatholders(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.top10_floatholders(ts_code=ts_code)
        elif period != "":
                df = ctx.tushare_pro.top10_floatholders(period=period)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.top10_holders(start_date=start_date, end_date=end_date)
        df.to_sql('top10_floatholders', self.engine, index=False, if_exists=if_exists)

    def stgTopList(self, ts_code="", trade_date="", if_exists="replace"):
        if ts_code !="" and trade_date !="":
            df = ctx.tushare_pro.top_list(ts_code=ts_code, trade_date=trade_date)
        elif trade_date!="":
            df = ctx.tushare_pro.top_list(trade_date=trade_date)
        df.to_sql('top_list', self.engine, index=False, if_exists=if_exists)

    def stgTopInst(self, ts_code="", trade_date="", if_exists="replace"):
        if ts_code !="" and trade_date !="":
            df = ctx.tushare_pro.top_inst(ts_code=ts_code, trade_date=trade_date)
        elif trade_date!="":
            df = ctx.tushare_pro.top_inst(trade_date=trade_date)
        df.to_sql('top_inst', self.engine, index=False, if_exists=if_exists)

    def stgPledgeStat(self, ts_code, if_exists="append"):
        df = ctx.tushare_pro.pledge_stat(ts_code=ts_code)
        df.to_sql('pledge_stat', self.engine, index=False, if_exists=if_exists)

    def stgPledgeDetail(self, ts_code, if_exists="append"):
        df = ctx.tushare_pro.pledge_detail(ts_code=ts_code)
        df.to_sql('pledge_detail', self.engine, index=False, if_exists=if_exists)

    def stgRepurchase(self, ann_date="", start_date="", end_date="", if_exists="replace"):
        if ann_date != "":
            df = ctx.tushare_pro.repurchase(ann_date=ann_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.repurchase(start_date=start_date, end_date=end_date)
        df.to_sql('repurchase', self.engine, index=False, if_exists=if_exists)

    def stgConcept(self, src="ts", if_exists='replace'):
        df = ctx.tushare_pro.concept(src=src)
        df.to_sql('concept', self.engine, index=False, if_exists=if_exists)

    def stgConceptDetail(self, id="", ts_code="", if_exists='append'):
        if id!="":
            df = ctx.tushare_pro.concept_detail(id=id)
        elif ts_code!="":
            df = ctx.tushare_pro.concept_detail(ts_code=ts_code)
        df.to_sql('concept_detail', self.engine, index=False, if_exists=if_exists)

    def stgShareFloat(self, ts_code="", ann_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code!="":
            if ann_date != "":
                df = ctx.tushare_pro.share_float(ts_code=ts_code, ann_date=ann_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.share_float(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.share_float(ts_code=ts_code)
        elif ann_date!="":
            df = ctx.tushare_pro.share_float(ann_date=ann_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.share_float(start_date=start_date, end_date=end_date)
        df.to_sql('share_float', self.engine, index=False, if_exists=if_exists)

    def stgBlockTrade(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code!="":
            if trade_date != "":
                df = ctx.tushare_pro.block_trade(ts_code=ts_code, ann_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.block_trade(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.block_trade(ts_code=ts_code)
        elif trade_date!="":
            df = ctx.tushare_pro.block_trade(ann_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.block_trade(start_date=start_date, end_date=end_date)
        df.to_sql('block_trade', self.engine, index=False, if_exists=if_exists)

    def stgStkHoldernumber(self, ts_code="", enddate="", start_date="", end_date="", if_exists="replace"):
        if ts_code!="":
            if enddate != "":
                df = ctx.tushare_pro.stk_holdernumber(ts_code=ts_code, ann_date=enddate)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.stk_holdernumber(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.stk_holdernumber(ts_code=ts_code)
        elif enddate!="":
            df = ctx.tushare_pro.stk_holdernumber(ann_date=enddate)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.stk_holdernumber(start_date=start_date, end_date=end_date)
        df.to_sql('stk_holdernumber', self.engine, index=False, if_exists=if_exists)

    def stgStkHolderTrade(self, ts_code="", ann_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code!="":
            if ann_date != "":
                df = ctx.tushare_pro.stk_holdertrade(ts_code=ts_code, ann_date=ann_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.stk_holdertrade(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.stk_holdertrade(ts_code=ts_code)
        elif ann_date!="":
            df = ctx.tushare_pro.stk_holdertrade(ann_date=ann_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.stk_holdertrade(start_date=start_date, end_date=end_date)
        df.to_sql('stk_holdertrade', self.engine, index=False, if_exists=if_exists)


    def stgMoneyflowHsgt(self, trade_date="", start_date="", end_date="", if_exists="replace"):
        if trade_date != "":
            df = ctx.tushare_pro.moneyflow_hsgt(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.moneyflow_hsgt(start_date=start_date, end_date=end_date)
        df.to_sql('moneyflow_hsgt', self.engine, index=False, if_exists=if_exists)

    def stgHsgtTop10(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code!="":
            if trade_date != "":
                df = ctx.tushare_pro.hsgt_top10(ts_code=ts_code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.hsgt_top10(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.hsgt_top10(ts_code=ts_code)
        elif trade_date!="":
            df = ctx.tushare_pro.hsgt_top10(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.hsgt_top10(start_date=start_date, end_date=end_date)
        df.to_sql('hsgt_top10', self.engine, index=False, if_exists=if_exists)

    def stgHkHold(self, code="", ts_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if code!="":
            if trade_date != "":
                df = ctx.tushare_pro.hk_hold(ts_code=code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.hk_hold(ts_code=code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.hk_hold(ts_code=code)
        elif ts_code!="":
            if trade_date != "":
                df = ctx.tushare_pro.hk_hold(ts_code=ts_code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.hk_hold(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.hk_hold(ts_code=ts_code)
        elif trade_date!="":
            df = ctx.tushare_pro.hk_hold(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.hk_hold(start_date=start_date, end_date=end_date)
        df.to_sql('hk_hold', self.engine, index=False, if_exists=if_exists)

    #20200214新增
    def stgHsConst(self, hs_type, if_exists="append"):
        df = ctx.tushare_pro.hs_const(hs_type=hs_type)
        df.to_sql('hs_const', self.engine, index=False, if_exists=if_exists)

    def stgStkManagers(self, ts_code, if_exists="append"):
        fields = 'ts_code,ann_date,name,gender,lev,title,edu,national,birthday,begin_date,end_date,resume'
        df = ctx.tushare_pro.stk_managers(ts_code=ts_code, fields=fields)
        df.to_sql('stk_managers', self.engine, index=False, if_exists=if_exists)

    def stgStkRewards(self, ts_code, end_date="", if_exists="append"):
        if end_date=="":
            df = ctx.tushare_pro.stk_rewards(ts_code=ts_code)
        else:
            df = ctx.tushare_pro.stk_rewards(ts_code=ts_code, end_date=end_date)
        df.to_sql('stk_rewards', self.engine, index=False, if_exists=if_exists)

    def stgNewShare(self, start_date="", end_date="", if_exists="replace"):
        if start_date != "" and end_date != "":
            df = ctx.tushare_pro.new_share(start_date=start_date, end_date=end_date)
        elif start_date != "":
            df = ctx.tushare_pro.new_share(start_date=start_date)
        elif end_date != "":
            df = ctx.tushare_pro.new_share(end_date=end_date)
        else:
            df = ctx.tushare_pro.new_share()
        df.to_sql('new_share', self.engine, index=False, if_exists=if_exists)

    def stgMoneyflow(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code != "":
            if trade_date != "":
                df = ctx.tushare_pro.moneyflow(ts_code=ts_code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.moneyflow(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.moneyflow(ts_code=ts_code)
        elif trade_date != "":
            df = ctx.tushare_pro.moneyflow(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.moneyflow(start_date=start_date, end_date=end_date)
        df.to_sql('moneyflow', self.engine, index=False, if_exists=if_exists)

    def stgStkLimit(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code != "":
            if trade_date != "":
                df = ctx.tushare_pro.stk_limit(ts_code=ts_code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.stk_limit(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.stk_limit(ts_code=ts_code)
        elif trade_date != "":
            df = ctx.tushare_pro.stk_limit(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.stk_limit(start_date=start_date, end_date=end_date)
        df.to_sql('stk_limit', self.engine, index=False, if_exists=if_exists)

    def stgLimitList(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code != "":
            if trade_date != "":
                df = ctx.tushare_pro.limit_list(ts_code=ts_code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.limit_list(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.limit_list(ts_code=ts_code)
        elif trade_date != "":
            df = ctx.tushare_pro.limit_list(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.limit_list(start_date=start_date, end_date=end_date)
        df.to_sql('limit_list', self.engine, index=False, if_exists=if_exists)

    def stgGgtDaily(self, trade_date="", start_date="", end_date="", if_exists="replace"):
        if trade_date != "":
            df = ctx.tushare_pro.ggt_daily(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.ggt_daily(start_date=start_date, end_date=end_date)
        df.to_sql('ggt_daily', self.engine, index=False, if_exists=if_exists)

    def stgGgtMonthly(self, month="", start_month="", end_month="", if_exists="replace"):
        if month != "":
            df = ctx.tushare_pro.ggt_monthly(month=month)
        elif start_month != "" and end_month != "":
            df = ctx.tushare_pro.ggt_monthly(start_month=start_month, end_month=end_month)
        df.to_sql('ggt_monthly', self.engine, index=False, if_exists=if_exists)

    def stgStkHolderTrade(self, ts_code="", ann_date="", start_date="", end_date="", if_exists="replace"):
        fields = "ts_code,ann_date,holder_name,holder_type,in_de,change_vol,change_ratio,after_share,after_ratio,avg_price,total_share,begin_date,close_date"
        if ts_code != "":
            if ann_date != "":
                df = ctx.tushare_pro.stk_holdertrade(ts_code=ts_code, ann_date=ann_date, fields=fields)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.stk_holdertrade(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
            else:
                df = ctx.tushare_pro.stk_holdertrade(ts_code=ts_code, fields=fields)
        elif ann_date != "":
            df = ctx.tushare_pro.stk_holdertrade(ann_date=ann_date, fields=fields)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.stk_holdertrade(start_date=start_date, end_date=end_date, fields=fields)
        df.to_sql('stk_holdertrade', self.engine, index=False, if_exists=if_exists)

    def stgFundBasic(self, market="", if_exists="append"):
        if market != "":
            df = ctx.tushare_pro.fund_basic(market=market)
        else:
            df = ctx.tushare_pro.fund_basic()
        df.to_sql('fund_basic', self.engine, index=False, if_exists=if_exists)

    def stgFundCompany(self, if_exists="replace"):
        df = ctx.tushare_pro.fund_company()
        df.to_sql('fund_company', self.engine, index=False, if_exists=if_exists)

    def stgFundNav(self, ts_code="", end_date="", if_exists="replace"):
        if ts_code != "":
            if end_date != "":
                df = ctx.tushare_pro.fund_nav(ts_code=ts_code, end_date=end_date)
            else:
                df = ctx.tushare_pro.fund_nav(ts_code=ts_code)
        elif end_date != "":
            df = ctx.tushare_pro.fund_nav(end_date=end_date)
        df.to_sql('fund_nav', self.engine, index=False, if_exists=if_exists)

    def stgFundDiv(self, ts_code="", ann_date="", if_exists="replace"):
        if ts_code != "":
            if ann_date != "":
                df = ctx.tushare_pro.fund_div(ts_code=ts_code, ann_date=ann_date)
            else:
                df = ctx.tushare_pro.fund_div(ts_code=ts_code)
        elif ann_date != "":
            df = ctx.tushare_pro.fund_div(ann_date=ann_date)
        df.to_sql('fund_div', self.engine, index=False, if_exists=if_exists)

    def stgFundPortfolio(self, ts_code, if_exists="append"):
        df = ctx.tushare_pro.fund_portfolio(ts_code=ts_code)
        df.to_sql('fund_portfolio', self.engine, index=False, if_exists=if_exists)

    def stgFundDaily(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="append"):
        if ts_code != "":
            if trade_date != "":
                df = ctx.tushare_pro.fund_daily(ts_code=ts_code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.fund_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.fund_daily(ts_code=ts_code)
        elif trade_date != "":
            df = ctx.tushare_pro.fund_daily(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.fund_daily(start_date=start_date, end_date=end_date)
        df.to_sql('fund_daily', self.engine, index=False, if_exists=if_exists)

    def stgFundAdj(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="append"):
        if ts_code != "":
            if trade_date != "":
                df = ctx.tushare_pro.fund_adj(ts_code=ts_code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.fund_adj(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.fund_adj(ts_code=ts_code)
        elif trade_date != "":
            df = ctx.tushare_pro.fund_adj(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.fund_adj(start_date=start_date, end_date=end_date)
        df.to_sql('fund_adj', self.engine, index=False, if_exists=if_exists)

    def stgIndexBasic(self, market, if_exists="append"):
        df = ctx.tushare_pro.index_basic(market=market)
        df.to_sql('index_basic', self.engine, index=False, if_exists=if_exists)

    def stgIndexDaily(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="append"):
        if ts_code != "":
            if trade_date != "":
                df = ctx.tushare_pro.index_daily(ts_code=ts_code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.index_daily(ts_code=ts_code)
        elif trade_date != "":
            df = ctx.tushare_pro.index_daily(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.index_daily(start_date=start_date, end_date=end_date)
        df.to_sql('index_daily', self.engine, index=False, if_exists=if_exists)

    def stgIndexWeekly(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code != "":
            if trade_date != "":
                df = ctx.tushare_pro.index_weekly(ts_code=ts_code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.index_weekly(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.index_weekly(ts_code=ts_code)
        elif trade_date != "":
            df = ctx.tushare_pro.index_weekly(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.index_weekly(start_date=start_date, end_date=end_date)
        df.to_sql('index_weekly', self.engine, index=False, if_exists=if_exists)

    def stgIndexMonthly(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if ts_code != "":
            if trade_date != "":
                df = ctx.tushare_pro.index_monthly(ts_code=ts_code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.index_monthly(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.index_monthly(ts_code=ts_code)
        elif trade_date != "":
            df = ctx.tushare_pro.index_monthly(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.index_monthly(start_date=start_date, end_date=end_date)
        df.to_sql('index_monthly', self.engine, index=False, if_exists=if_exists)

    def stgIndexWeight(self, index_code="", trade_date="", start_date="", end_date="", if_exists="replace"):
        if index_code != "":
            if trade_date != "":
                df = ctx.tushare_pro.index_weight(index_code=index_code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.index_weight(index_code=index_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.index_weight(index_code=index_code)
        elif trade_date != "":
            df = ctx.tushare_pro.index_weight(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.index_weight(start_date=start_date, end_date=end_date)
        df.to_sql('index_weight', self.engine, index=False, if_exists=if_exists)

    def stgIndexDailyBasic(self, ts_code="", trade_date="", start_date="", end_date="", if_exists="append"):
        if ts_code != "":
            if trade_date != "":
                df = ctx.tushare_pro.index_dailybasic(ts_code=ts_code, trade_date=trade_date)
            elif start_date != "" and end_date != "":
                df = ctx.tushare_pro.index_dailybasic(ts_code=ts_code, start_date=start_date, end_date=end_date)
            else:
                df = ctx.tushare_pro.index_dailybasic(ts_code=ts_code)
        elif trade_date != "":
            df = ctx.tushare_pro.index_dailybasic(trade_date=trade_date)
        elif start_date != "" and end_date != "":
            df = ctx.tushare_pro.index_dailybasic(start_date=start_date, end_date=end_date)
        df.to_sql('index_dailybasic', self.engine, index=False, if_exists=if_exists)

    def stgIndexClassify(self, if_exists="replace"):
        fields = 'index_code,industry_name,level,industry_code,src'
        df = ctx.tushare_pro.index_classify(fields=fields)
        df.to_sql('index_classify', self.engine, index=False, if_exists=if_exists)

    def stgIndexMember(self, index_code="", if_exists="append"):
        fields = 'index_code,index_name,con_code,con_name,in_date,out_date,is_new'
        if index_code!="":
            df = ctx.tushare_pro.index_member(index_code=index_code, fields=fields)
        else:
            df = ctx.tushare_pro.index_member(fields=fields)
        df.to_sql('index_member', self.engine, index=False, if_exists=if_exists)