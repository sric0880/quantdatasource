from quantdatasource.api.tushare import TushareApi
from quantdatasource.jobs.account import *
from quantdatasource.jobs.calendar import get_astock_calendar
from quantdatasource.jobs.scheduler import job

__all__ = ["tushare_cb_daily"]


@job(
    trigger="cron",
    id="astock_tushare_cb_daily",
    name="[TushareApi]可转债日线",
    replace_existing=True,
    hour=23,
    minute=59,
    misfire_grace_time=200,
)
def tushare_cb_daily(dt, is_collect, is_import):
    calendar = get_astock_calendar()
    if not calendar.is_trading_day(dt):
        return
    api = TushareApi(tushare_token, astock_output, dt)
    if is_collect:
        api.addition_download_cb_daily()

    if is_import:
        from quantdatasource.dbimport import tdengine
        from quantdatasource.dbimport.tushare import cb

        tdengine.insert_multi_tables(
            cb.addition_read_cb_daily(
                dt, api.cb_daily_bars_addition_path, api.basic_cb_path
            ),
            "bars_cb_daily",
            whole_df=False,
            reorder_cols=False,
        )
