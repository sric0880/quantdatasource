import logging

import duckdb as db
import pandas as pd
from quantdata import mongo_get_data

from quantdatasource.api.tushare import TushareApi
from quantdatasource.jobs import account
from quantdatasource.jobs.scheduler import job

__all__ = ["tushare_cb_data"]


# 每周6晚上更新
@job(
    trigger="cron",
    id="astock_tushare_cb_data",
    name="[TushareApi]可转债转股和赎回",
    replace_existing=True,
    day_of_week=5,
    hour=23,
    misfire_grace_time=200,
)
def tushare_cb_data(dt, is_collect, is_import):
    api = TushareApi(account.tushare_token, account.astock_output, dt)
    if is_collect:
        api.full_download_cb_share_data(include_delist_cbs=False, force_replace=True)
        api.full_download_cb_call_data(include_delist_cbs=False, force_replace=True)

    if is_import:
        from quantdatasource.dbimport import duckdb
        from quantdatasource.dbimport.tushare import cb

        dfs = []
        for cb_info in mongo_get_data("finance", "basic_info_cbs"):
            symbol = cb_info["ts_code"]
            call_df = cb.read_cb_call(symbol, api.cb_call_path)
            share_df = cb.read_cb_share(cb_info, api.cb_share_path)
            df = pd.merge_ordered(call_df, share_df, on="dt", how="outer")
            if not df.empty:
                df["symbol"] = symbol
                dfs.append(df)

        big_df = pd.concat(dfs)
        parquet_file = f"{config.config['parquet_output']}/bars_cb_data.parquet"
        big_df.to_parquet(parquet_file)
        logging.info(f"写入parquet[{parquet_file}]")
