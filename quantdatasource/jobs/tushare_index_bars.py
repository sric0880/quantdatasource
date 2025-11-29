import logging
import pathlib
import pandas as pd

from quantdatasource.api.tushare import TushareApi
from quantdatasource.jobs import account
from quantdatasource.jobs.calendar import get_astock_calendar
from quantdatasource.jobs.scheduler import job

__all__ = ["tushare_index_bars"]


def _append(df: pd.DataFrame, out: pathlib.Path):
    for row in df.itertuples():
        o = out / f"{row.symbol}.parquet"
        if not o.exists():
            logging.error(f"指数日线 {o} not found")
            continue
        df = pd.read_parquet(o)
        if row.dt.to_numpy() in df["dt"].to_list():
            logging.warning(f"{row.dt} is in {o}")
            continue
        one = pd.DataFrame([row])
        one = one.drop(columns=["Index", "symbol"])
        df = pd.concat([df, one], ignore_index=True)
        df = df.sort_values("dt")
        df.to_parquet(o, index=False)
        logging.info(f"更新[{o}]")


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

        daily = index.addition_read_index(api.index_daily_addition_path)
        weekly = index.addition_read_index(api.index_week_addition_path)
        monthly = index.addition_read_index(api.index_month_addition_path)
        output_dir = pathlib.Path(account.astock_output).joinpath("bars_index")
        if daily is not None:
            d_path = output_dir / "daily"
            d_path.mkdir(parents=True, exist_ok=True)
            _append(daily, d_path)
        if weekly is not None:
            w_path = output_dir / "week"
            w_path.mkdir(parents=True, exist_ok=True)
            _append(weekly, w_path)
        if monthly is not None:
            m_path = output_dir / "mon"
            m_path.mkdir(parents=True, exist_ok=True)
            _append(monthly, m_path)
