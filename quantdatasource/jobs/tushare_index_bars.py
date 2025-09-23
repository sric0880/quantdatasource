import logging
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
    api = TushareApi(account.tushare_token, account.raw_astock_output, dt)

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
        from quantdatasource.dbimport.tushare import index

        daily = index.addition_read_index(api.index_daily_addition_path, "1D")
        weekly = index.addition_read_index(api.index_week_addition_path, "w")
        monthly = index.addition_read_index(api.index_month_addition_path, "mon")
        output_dir = pathlib.Path(account.astock_output).joinpath("bars_index")
        if daily is not None:
            d_path = output_dir/"daily"
            d_path.mkdir(parents=True, exist_ok=True)
            file_path = d_path/f"{dt.date().isoformat()}.parquet"
            daily.to_parquet(file_path)
            logging.info(f"写入[{file_path}]")
        if weekly is not None:
            w_path = output_dir/"weekly"
            w_path.mkdir(parents=True, exist_ok=True)
            file_path = w_path/f"{dt.date().isoformat()}.parquet"
            weekly.to_parquet(file_path)
            logging.info(f"写入[{file_path}]")
        if monthly is not None:
            m_path = output_dir/"monthly"
            m_path.mkdir(parents=True, exist_ok=True)
            file_path = m_path/f"{dt.date().isoformat()}.parquet"
            monthly.to_parquet(file_path)
            logging.info(f"写入[{file_path}]")
