from quantdatasource.api.eastmoney import EastMoneyApi
from quantdatasource.dbimport import mongodb
from quantdatasource.jobs.account import *
from quantdatasource.jobs.calendar import get_astock_calendar
from quantdatasource.jobs.scheduler import job

__all__ = ["eastmoney_analyst_reports"]


# 增量下载研报数据
@job(
    trigger="cron",
    id="astock_eastmoney_reports",
    name="[EastMoneyApi]更新研报",
    replace_existing=True,
    hour=23,
    minute=59,
    second=58,
    misfire_grace_time=1,
)
def eastmoney_analyst_reports(dt, is_collect, is_import):
    api = EastMoneyApi(astock_output, dt)
    if is_collect:
        api.addition_download_analyst_reports()

    if is_import:
        from quantdatasource.dbimport.eastmoney import analyst_reports

        cal = get_astock_calendar()
        mongodb.insert_many(
            analyst_reports.addition_read_analyst_reports(
                api.analyst_reports_addition_path, cal
            ),
            "finance",
            "analyst_reports",
            ignore_nan=True,
            drop=False,
        )
