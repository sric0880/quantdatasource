import pathlib

from quantdatasource.api.tushare import TushareApi
from quantdatasource.jobs import account
from quantdatasource.jobs.calendar import get_astock_calendar
from quantdatasource.jobs.scheduler import job

__all__ = ["tushare_index_bars"]


@job(
    trigger="cron",
    id="astock_tushare_index",
    name="[TushareApi]大盘指数日/周/月线",
    replace_existing=True,
    hour=23,
    minute=58,
    misfire_grace_time=200,
)
def tushare_index_bars(dt, is_collect, is_import):
    calendar = get_astock_calendar()
    if not calendar.is_trading_day(dt):
        return
    api = TushareApi(account.tushare_token, account.astock_output, dt)

    index_codes = [
        ("000016.SH", "2004-01-01"),
        ("000300.SH", "2005-01-01"),
        ("000905.SH", "2007-01-01"),
        ("000852.SH", "2005-01-01"),
        ("399303.SZ", "2014-03-01"),
        ("000001.SH", "1991-01-01"),
        ("399001.SZ", "1991-04-01"),
        ("399006.SZ", "2010-06-01"),
    ]
    if is_collect:
        api.addition_download_index(index_codes)

    if is_import:
        from quantdatasource.dbimport import duckdb
        from quantdatasource.dbimport.tushare import index

        daily = index.addition_read_index(api.index_daily_addition_path, "1D")
        weekly = index.addition_read_index(api.index_week_addition_path, "w")
        monthly = index.addition_read_index(api.index_month_addition_path, "mon")
        parquet_output = pathlib.Path(f"{config.config['parquet_output']}/bars_index")
        parquet_output.mkdir(parents=True, exist_ok=True)
        if daily is not None:
            daily_path = parquet_output/"daily"
            daily_path.mkdir(parents=True, exist_ok=True)
            duckdb.save_multi_tables(daily, daily_path/f"{dt.date().isoformat()}.parquet")
        if weekly is not None:
            daily_path = parquet_output/"weekly"
            daily_path.mkdir(parents=True, exist_ok=True)
            duckdb.save_multi_tables(weekly, daily_path/f"{dt.date().isoformat()}.parquet")
        if monthly is not None:
            daily_path = parquet_output/"monthly"
            daily_path.mkdir(parents=True, exist_ok=True)
            duckdb.save_multi_tables(monthly, daily_path/f"{dt.date().isoformat()}.parquet")
