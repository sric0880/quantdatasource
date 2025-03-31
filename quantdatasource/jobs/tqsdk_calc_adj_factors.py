from contextlib import closing

from quantdatasource.dbimport import mongodb
from quantdatasource.jobs.account import *
from quantdatasource.jobs.calendar import get_ctpfuture_calendar
from quantdatasource.jobs.scheduler import job

__all__ = ["tqsdk_calc_adj_factors"]


@job(
    service_type="datasource-mongo",
    id="future_tqsdk_calc_adj_factors",
    name="[TQSDKApi]更新期货价差数据(未测试)",
)
def tqsdk_calc_adj_factors(dt, is_collect, is_import):
    from quantdatasource.api.tqsdk import TQSDKApi

    api = TQSDKApi(tq_username, tq_psw, future_output, dt)
    with closing(api):
        if is_collect:
            api.full_download_future_cont_list()
            api.full_download_future_cont_history()
            api.cal_cont_future_adjust_factors(force_replace=True)

    if is_import:
        from quantdatasource.dbimport.tqsdk import adjust_factors

        cal = get_ctpfuture_calendar()
        mongodb.insert_many(
            adjust_factors.read_adjust_factors(api.adjust_factors_filepath, cal),
            "finance_ctpfuture",
            "adjust_factors",
        )

        coll = mongodb.get_conn_mongodb()["finance_ctpfuture"]["adjust_factors"]

        def move_adjust_factors(symbol, price_diff):
            # 由于某些品种调整之后，价格为负数，需要将价差因子整体往下平移。price_diff未负，表示向下平移，为正，表示向上平移
            adjs = coll.find({"symbol": symbol})
            for a in adjs:
                coll.update_one(
                    {"_id": a["_id"]},
                    {"$set": {"adjust_factor": a["adjust_factor"] + price_diff}},
                )

        move_adjust_factors("ru", -10000)
