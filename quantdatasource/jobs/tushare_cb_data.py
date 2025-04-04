from quantdata import mongo_get_data

from quantdatasource.api.tushare import TushareApi
from quantdatasource.jobs.account import *
from quantdatasource.jobs.scheduler import job

__all__ = ["tushare_cb_data"]


# 每周6晚上更新
@job(
    service_type="datasource-all",
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
        from quantdatasource.dbimport import tdengine
        from quantdatasource.dbimport.tushare import cb

        for cb_info in mongo_get_data("finance", "basic_info_cbs"):
            symbol = cb_info["ts_code"]
            tdengine.insert(
                cb.read_cb_call(symbol, api.cb_call_path),
                symbol,
                stable="bars_cb_daily",
                whole_df=False,
            )
            tdengine.insert(
                cb.read_cb_share(cb_info, api.cb_share_path),
                symbol,
                stable="bars_cb_daily",
                whole_df=False,
            )
