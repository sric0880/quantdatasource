from contextlib import closing

from quantdatasource.dbimport import mongodb
from quantdatasource.jobs import account
from quantdatasource.jobs.calendar import get_ctpfuture_calendar
from quantdatasource.jobs.scheduler import job

__all__ = ["ctp_download_contracts"]


def _download_subprocess(account):
    from quantdatasource.api.ctp import SimpleCtpApi

    api = SimpleCtpApi(config, account.future_output)
    with closing(api):
        api.full_download_contracts()


@job(
    service_type="datasource-mongo",
    # trigger="cron", # 速度太慢了，暂时关闭
    id="future_ctp_contracts",
    name="[CTP]更新期货合约手续费和保证金",
    replace_existing=True,
    hour=20,
    minute=35,
    misfire_grace_time=200,
)
def ctp_download_contracts(dt, is_collect, is_import):
    calendar = get_ctpfuture_calendar()
    if not calendar.is_trading_day(dt):
        return

    ctp_accounts = config.config["ctp_accounts"]

    if is_collect:
        # 目前只有一个账户，不用多进程
        # from multiprocessing import Pool

        # with Pool(len(account.ctp_accounts)) as pool:
        #     pool.map(_download_subprocess, account.ctp_accounts)
        for acc in account.ctp_accounts:
            _download_subprocess(acc)

    if is_import:
        from pathlib import Path

        from quantdatasource.dbimport.ctp import contracts

        for acc in account.ctp_accounts:
            filename = f"contracts_{acc['BrokerID']}"
            mongodb.insert_many(
                contracts.read_contracts(Path(account.future_output, f"{filename}.json")),
                "finance_ctpfuture",
                filename,
            )
