# 同花顺热股榜
from quantdatasource.api.ths import THSApi
from quantdatasource.dbimport import mongodb
from quantdatasource.jobs.account import *
from quantdatasource.jobs.calendar import get_astock_calendar
from quantdatasource.jobs.scheduler import job

__all__ = ["ths_hot_stocks"]


@job(
    trigger="cron",
    id="astock_ths_hot_stocks",
    name="[THSApi]更新同花顺热股",
    replace_existing=True,
    hour=15,
    minute=0,
    misfire_grace_time=200,
)
def ths_hot_stocks(dt, is_collect, is_import):
    calendar = get_astock_calendar()
    if not calendar.is_trading_day(dt):
        return
    api = THSApi(astock_output, dt)
    if is_collect:
        api.addition_download_hot_stocks()

    if is_import:
        from quantdatasource.dbimport.ths import hot_stocks

        mongodb.insert_many(
            hot_stocks.addition_read_hot_stocks(dt, api.hot_stocks_addition_path),
            "finance",
            "hot_stocks_ths",
            drop=False,
        )
