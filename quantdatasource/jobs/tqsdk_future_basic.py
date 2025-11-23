from contextlib import closing

from quantdatasource.jobs import account, data_saver
from quantdatasource.jobs.calendar import get_ctpfuture_calendar
from quantdatasource.jobs.scheduler import job

__all__ = ["tqsdk_future_basic"]


@job(
    trigger="cron",
    id="future_tqsdk_future_basic",
    name="[TQSDKApi]更新期货basic数据",
    replace_existing=True,
    hour=15,
    minute=15,
    misfire_grace_time=200,
)
def tqsdk_future_basic(dt, is_collect, is_import):
    calendar = get_ctpfuture_calendar()
    if not calendar.is_trading_day(dt):
        return
    from quantdatasource.api.tqsdk import TQSDKApi

    api = TQSDKApi(account.tq_username, account.tq_psw, account.raw_future_output, dt)
    with closing(api):
        if is_collect:
            api.full_download_future_basic()

    if is_import:
        from quantdatasource.dbimport.tqsdk import future_basic

        data_saver.mongo_insert_many(
            future_basic.read_future_basic(api.future_basic_path),
            "finance_ctpfuture",
            "basic_info_futures",
        )
        data_saver.mongo_insert_many(
            future_basic.read_future_products_basic(api.product_basic_path),
            "finance_ctpfuture",
            "basic_info_products",
        )
