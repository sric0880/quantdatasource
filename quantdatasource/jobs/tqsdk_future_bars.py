import logging
from contextlib import closing

from quantdatasource.api.tushare import TushareFutureApi
from quantdatasource.jobs.account import *
from quantdatasource.jobs.scheduler import job

__all__ = ["tqsdk_future_bars"]


@job(
    service_type="datasource-all",
    id="future_tqsdk_future_bars",
    name="[TQSDKApi|TushareFutureApi]更新期货K线数据(未测试)",
)
def tqsdk_future_bars(dt, is_collect, is_import):
    tushare_api = TushareFutureApi(tushare_token, future_output, dt)
    if is_collect:
        tushare_api.full_download_all_future_bars()
    from quantdatasource.api.tqsdk import TQSDKApi

    api = TQSDKApi(tq_username, tq_psw, future_output, dt)
    with closing(api):
        if is_collect:
            api.full_download_bars()

    if is_import:
        from quantdatasource.dbimport import tdengine
        from quantdatasource.dbimport.tqsdk import klines
        from quantdatasource.dbimport.tushare import future_daily

        for df, symbol, itv, exchange, is_history in klines.read_klines(
            api.bars_history_path, api.bars_current_path
        ):
            tbname = tdengine.get_tbname(symbol + "_" + itv, stable="bars")
            existed_tables = tdengine.get_existed_tables("bars")
            if is_history and tbname in existed_tables:
                # chech table exists. don't import if it has imported.
                logging.info(f"{symbol} {itv} already exists.")
                continue
            tdengine.create_child_tables([tbname], "bars", [(symbol, itv, exchange)])
            tdengine.insert(
                df,
                tbname,
                types={
                    "dt": "timestamp",
                    "open": "float",
                    "high": "float",
                    "low": "float",
                    "close": "float",
                    "volume": "int unsigned",
                    "amount": "bigint unsigned",
                    "open_interest": "int unsigned",
                },
            )

        for df, week_df, symbol, is_history in future_daily.read_daily_and_weekly(
            tushare_api.future_daily_history_path, tushare_api.future_daily_current_path
        ):
            tbname = tdengine.get_tbname(symbol, stable="bars_daily")
            existed_tables = tdengine.get_existed_tables("bars_daily")
            if is_history and tbname in existed_tables:
                # chech table exists. don't import if it has imported.
                logging.info(f"{symbol} daily already exists.")
                continue

            tdengine.drop_tables([symbol + "_" + "w"], "bars_daily")
            tdengine.create_child_tables(
                [
                    tbname,
                    tdengine.get_tbname(
                        symbol + "_" + "w", stable="bars_daily"
                    ),
                ],
                "bars_daily",
                [(symbol, exchange), (symbol, exchange)],
            )
            tdengine.insert(df, symbol, stable="bars_daily")
            tdengine.insert(week_df, symbol + "_" + "w", stable="bars_daily")
