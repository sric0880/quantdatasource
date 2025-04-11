import logging

import duckdb as db
import pandas as pd
from quantdata import get_data_df, mongo_get_data

from quantdatasource.api.tushare import TushareApi
from quantdatasource.jobs.account import *
from quantdatasource.jobs.scheduler import job

__all__ = ["tushare_cb_data"]


# 每周6晚上更新
@job(
    service_type="datasource-astock-cb",
    trigger="cron",
    id="astock_tushare_cb_data",
    name="[TushareApi]可转债转股和赎回",
    replace_existing=True,
    day_of_week=5,
    hour=23,
    misfire_grace_time=200,
)
def tushare_cb_data(dt, is_collect, is_import):
    api = TushareApi(tushare_token, astock_output, dt)
    if is_collect:
        api.full_download_cb_share_data(include_delist_cbs=False, force_replace=True)
        api.full_download_cb_call_data(include_delist_cbs=False, force_replace=True)

    if is_import:
        from quantdatasource.dbimport import duckdb
        from quantdatasource.dbimport.tushare import cb

        for cb_info in mongo_get_data("finance", "basic_info_cbs"):
            symbol = cb_info["ts_code"]
            call_df = cb.read_cb_call(symbol, api.cb_call_path)
            share_df = cb.read_cb_share(cb_info, api.cb_share_path)
            if call_df is None and share_df is None:
                logging.info(f"{symbol} 没有 cb call and share")
                continue
            elif call_df is None:
                update_df = share_df
            elif share_df is None:
                update_df = call_df
            else:
                update_df = pd.merge_ordered(call_df, share_df, on="dt", how="outer")
            try:
                base_df: pd.DataFrame = get_data_df("bars_cb_daily", duckdb.get_tbname("_" + symbol))
                base_df.set_index("dt", inplace=True)
                update_df.set_index("dt", inplace=True)
                base_df.update(update_df)
                base_df.reset_index(inplace=True)
                df = base_df
            except db.CatalogException:
                logging.info(f"{symbol} 没有可转债日线")
                df = update_df

            duckdb.create_or_replace_table(df, "_" + symbol, "bars_cb_daily")
