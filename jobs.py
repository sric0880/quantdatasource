from contextlib import closing

import fire
from quantcalendar import CalendarAstock

from quantdatasource import main
from quantdatasource.api.eastmoney import EastMoneyApi
from quantdatasource.api.tqsdk import TQSDKApi
from quantdatasource.api.tushare import TushareApi
from quantdatasource.scheduler import job

from account import *

astock_tushare_output = "datasource/AStock/tushare"
astock_eastmoney_output = "datasource/AStock/eastmoney"
future_tqsdk_output = "datasource/CTPFuture/tqsdk"
future_tushare_output = "datasource/CTPFuture/tushare"


# 每周6晚上更新
@job(
    import_mod="import_db.AStock",
    import_funcs=["tushare_.cb.addition_import_cb_data"],
    calendar=CalendarAstock(),
    trigger="cron",
    id="astock_tushare_cb_data",
    name="[TushareApi]可转债转股和赎回",
    replace_existing=True,
    day_of_week=5,
    hour=23,
    misfire_grace_time=200,
)
def tushare_cb_data(dt, is_tradeday):
    api = TushareApi(tushare_token, astock_tushare_output, dt)
    api.full_download_cb_share_data(include_delist_cbs=False, replace=True)
    api.full_download_cb_call_data(include_delist_cbs=False, replace=True)


@job(
    import_mod="import_db.AStock",
    import_funcs=[
        "tushare_.index.addition_import_indexes_bars",
        "tushare_.dpjk.addition_import_dpjk_data",
    ],
    calendar=CalendarAstock(),
    trigger="cron",
    id="astock_tushare_index_dpjk",
    name="[TushareApi]大盘指数日/周/月线+大盘监控",
    replace_existing=True,
    hour=23,
    minute=58,
    misfire_grace_time=200,
)
def tushare_index_bars(dt, is_tradeday):
    if not is_tradeday:
        return
    api = TushareApi(tushare_token, astock_tushare_output, dt)
    api.addition_download_index(
        [
            ("000016.SH", "2004-01-01"),
            ("000300.SH", "2005-01-01"),
            ("000905.SH", "2007-01-01"),
            ("000852.SH", "2005-01-01"),
            ("399303.SZ", "2014-03-01"),
            ("000001.SH", "1991-01-01"),
            ("399001.SZ", "1991-04-01"),
            ("399006.SZ", "2010-06-01"),
        ]
    )


@job(
    import_mod="import_db.AStock",
    import_funcs=["tushare_.cb.addition_import_cb_daily"],
    calendar=CalendarAstock(),
    trigger="cron",
    id="astock_tushare_cb_daily",
    name="[TushareApi]可转债日线",
    replace_existing=True,
    hour=23,
    minute=59,
    misfire_grace_time=200,
)
def tushare_cb_daily(dt, is_tradeday):
    if not is_tradeday:
        return
    api = TushareApi(tushare_token, astock_tushare_output, dt)
    api.addition_download_cb_daily()


@job(
    import_mod="import_db.AStock",
    import_funcs=[
        "tushare_.finance.addition_import_finance_data",
        "tushare_.stock_basic.addition_import_stock_basic",
        "tushare_.ths_index.addition_import_ths_concepts_basic",
        "tushare_.ths_index.addition_import_ths_concepts_constituent",
        "tushare_.ths_index.addition_import_concepts_bars",
        "tushare_.stock_daily.addition_import_stock_daily_bars",
        "tushare_.lhb.addition_import_lhb",
        "tushare_.cb.addition_import_cb_basic",
    ],
    calendar=CalendarAstock(),
    trigger="cron",
    id="astock_tushare",
    name="[TushareApi]其他",
    replace_existing=True,
    hour=20,
    minute=30,
    misfire_grace_time=200,
)
def tushare_misc_data(dt, is_tradeday):
    api = TushareApi(tushare_token, astock_tushare_output, dt)
    api.addition_download_finance_data()
    if not is_tradeday:
        return
    api.full_download_stock_basic()
    api.full_download_cb_basic()
    api.full_download_ths_index()
    api.full_download_concepts_members(force_replace=True)

    api.addition_download_concepts_bars()
    api.addition_download_daily()
    api.addition_download_daily_basic()
    api.addition_download_moneyflow()
    api.addition_download_lhb()


# 增量下载研报数据
@job(
    import_mod="import_db.AStock",
    import_funcs=["eastmoney.analyst_reports.addition_import_analyst_reports"],
    calendar=CalendarAstock(),
    trigger="cron",
    id="astock_eastmoney_reports",
    name="[EastMoneyApi]更新研报",
    replace_existing=True,
    hour=23,
    minute=59,
    second=58,
    misfire_grace_time=1,
)
def eastmoney_analyst_reports(dt, is_tradeday):
    api = EastMoneyApi(astock_eastmoney_output, dt)
    api.addition_download_analyst_reports()


@job(
    import_mod="import_db.CTPFuture",
    import_funcs=["tqsdk_.future_basic.addition_import_future_basic"],
    trigger="cron",
    id="future_tqsdk_future_basic",
    name="[TQSDKApi]更新期货basic数据",
    replace_existing=True,
    hour=15,
    minute=15,
    misfire_grace_time=200,
)
def tqsdk_future_basic(dt, is_tradeday):
    if not is_tradeday:
        return
    api = TQSDKApi(tq_username, tq_psw, future_tqsdk_output, dt)
    with closing(api):
        api.full_download_future_basic()


@job(
    import_mod="",
    import_funcs=[],
    id="future_tqsdk_future_basic",
    name="[TQSDKApi|TushareApi]更新期货K线数据",
)
def tqsdk_download_future_bars(dt, is_tradeday):
    api = TushareApi(tushare_token, future_tushare_output, dt)
    api.full_download_all_future_bars()
    api = TQSDKApi(tq_username, tq_psw, future_tqsdk_output, dt)
    with closing(api):
        api.full_download_bars()


@job(
    import_mod="",
    import_funcs=[],
    id="future_tqsdk_future_basic",
    name="[TQSDKApi]更新期货价差数据(未完全实现)",
)
def tqsdk_calc_adj_factors(dt, is_tradeday):
    api = TQSDKApi(tq_username, tq_psw, future_tqsdk_output, dt)
    with closing(api):
        api.full_download_future_cont_list()
        api.full_download_future_cont_history()
        api.cal_cont_future_adjust_factors(force_replace=True)
    # 3. 第三步：导入Mongodb python -m import_db.CTPFuture.tqsdk_.adjust_factors


if __name__ == "__main__":
    fire.Fire(main)
