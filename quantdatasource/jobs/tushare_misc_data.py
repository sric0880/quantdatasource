import logging
import pathlib

import pandas as pd
from quantcalendar import pydt_from_second

from quantdatasource.api.tushare import TushareApi
from quantdatasource.jobs import account, data_saver
from quantdatasource.jobs.calendar import get_astock_calendar
from quantdatasource.jobs.scheduler import job

__all__ = ["tushare_misc_data"]


def _find_last_end_date(end_date):
    m = end_date.month - 3
    if m == 0:
        return None
    if m == 3:
        d = 31
    else:
        d = 30
    return pd.Timestamp(year=end_date.year, month=m, day=d)


def _finance_diff(data1, data2):
    non_data_cols = [
        "_id",
        "ts_code",
        "ann_date",
        "f_ann_date",
        "end_date",
        "comp_type",
        "year",
    ]
    ret = {}
    for key in data1.keys():
        if key in non_data_cols:
            ret[key] = data1[key]
            continue
        d1 = data1[key]
        if pd.isna(d1):
            continue
        elif key not in data2:
            ret[key] = data1[key]
        else:
            ret[key] = data1[key] - data2[key]
    return ret


def _mongo_import_finance_data(row, symbol, tablename):
    conn = data_saver.get_conn_mongodb()
    table = conn["finance"][tablename]
    end_date = row["end_date"]
    update_result = table.update_one(
        {
            "ts_code": symbol,
            "f_ann_date": row["f_ann_date"],
            "end_date": end_date,
        },
        {"$set": row},
        upsert=True,
    )

    if update_result.upserted_id is not None:
        logging.info(f"导入 {tablename} {symbol} {end_date} 财报数据完成")
    elif update_result.modified_count > 0:
        logging.info(f"替换 {tablename} {symbol} {end_date} 财报数据完成")

    if tablename != "finance_balancesheet":
        quart_table = conn["finance"][tablename + "_q"]
        last_end_date = _find_last_end_date(end_date)
        if last_end_date is None:
            # 1季报直接导入单季报
            quart_table.update_one(
                {
                    "ts_code": symbol,
                    "f_ann_date": row["f_ann_date"],
                    "end_date": end_date,
                },
                {"$set": row},
                upsert=True,
            )
            logging.info(f"直接导入一季报 {tablename}_q {symbol} {end_date} 完成")
        else:
            # 找到最新的一期财报
            p_last_doc = (
                table.find({"ts_code": symbol, "end_date": last_end_date})
                .sort([("f_ann_date", -1)])
                .limit(1)
            )  # 降序
            try:
                last_doc = p_last_doc.next()
                doc_q = _finance_diff(row, last_doc)
                quart_table.update_one(
                    {
                        "ts_code": symbol,
                        "f_ann_date": row["f_ann_date"],
                        "end_date": end_date,
                    },
                    {"$set": doc_q},
                    upsert=True,
                )
                logging.info(f"导入单季报 {tablename}_q {symbol} {end_date} 完成")
            except:
                pass


@job(
    trigger="cron",
    id="astock_tushare",
    name="[TushareApi]其他",
    replace_existing=True,
    hour=20,
    minute=30,
    misfire_grace_time=200,
)
def tushare_misc_data(dt, is_collect, is_import):
    api = TushareApi(account.tushare_token, account.raw_astock_output, dt)
    if is_collect:
        api.addition_download_finance_data()

    if is_import:
        from quantdatasource.dbimport.tushare import finance

        for row, symbol in finance.addition_read_finance_data(
            dt, api.finance_income_addition_path
        ):
            _mongo_import_finance_data(row, symbol, "finance_income")

        for row, symbol in finance.addition_read_finance_data(
            dt, api.finance_balancesheet_addition_path
        ):
            _mongo_import_finance_data(row, symbol, "finance_balancesheet")

        for row, symbol in finance.addition_read_finance_data(
            dt, api.finance_cashflow_addition_path
        ):
            _mongo_import_finance_data(row, symbol, "finance_cashflow")

    calendar = get_astock_calendar()
    if not calendar.is_trading_day(dt):
        return

    if is_collect:
        api.full_download_stock_basic()
        api.full_download_cb_basic()
        api.full_download_ths_index()
        api.full_download_concepts_members(force_replace=True)

        api.addition_download_concepts_bars()
        api.addition_download_daily()
        api.addition_download_daily_basic()
        api.addition_download_moneyflow()
        api.addition_download_lhb()

    if is_import:
        from quantdatasource.dbimport.tushare import (cb, lhb, stock,
                                                      stock_utils, ths_index)

        stock_basic_df = stock.read_basic(api.basic_stock_path)
        data_saver.mongo_insert_many(stock_basic_df, "finance", "basic_info_stocks")

        concepts_basic_df = ths_index.read_ths_concepts_basic(
            api.ths_index_a_concepts_path
        )
        data_saver.mongo_insert_many(
            concepts_basic_df,
            "finance",
            "basic_info_ths_concepts",
        )
        conn = data_saver.get_conn_mongodb()
        all_ths_index_df = pd.DataFrame(conn["finance"]["constituent_ths_index"].find())
        addition_constituent_rows = ths_index.addition_read_ths_concepts_constituent(
            dt, all_ths_index_df, api.ths_concepts_members_path
        )
        if addition_constituent_rows:
            conn["finance"]["constituent_ths_index"].insert_many(
                addition_constituent_rows
            )
        else:
            logging.info("同花顺概念股成分没有增量改变")

        output_dir = pathlib.Path(account.astock_output)
        ths_index_daily_out = output_dir / "bars_ths_index_daily"
        ths_index_daily_out.mkdir(parents=True, exist_ok=True)
        ths_index_df = ths_index.addition_read_concepts_bars(
            api.ths_daily_bars_addition_path, concepts_basic_df
        )
        ths_index_file_path = ths_index_daily_out / f"{dt.date().isoformat()}.parquet"
        ths_index_df.to_parquet(ths_index_file_path)
        logging.info(f"写入[{ths_index_file_path}]")

        chinese_names = dict(zip(stock_basic_df["symbol"], stock_basic_df["name"]))
        daily_bars = stock.addition_read_stock_daily_bars(
            dt,
            api.daily_bars_addition_path,
            api.daily_basic_addition_path,
            api.moneyflow_addition_path,
            chinese_names,
        )
        stock_daily_out = output_dir / "bars_stock_daily"
        stock_daily_out.mkdir(parents=True, exist_ok=True)
        stock_daily_file_path = stock_daily_out / f"{dt.date().isoformat()}.parquet"
        daily_bars.to_parquet(stock_daily_file_path)
        logging.info(f"写入[{stock_daily_file_path}]")

        yesterday = pydt_from_second(calendar.get_tradedays_lte(dt, 2)[0])
        yesterday_daily_bars = pd.read_feather(
            stock_daily_out / f"{yesterday.date().isoformat()}.parquet"
        )
        adjust_factors_collection = conn["finance"]["adjust_factors"]
        adj_factors = stock_utils.cal_adjust_factors(daily_bars, yesterday_daily_bars)
        for symbol, adj in adj_factors.items():
            lst = []
            for one in adjust_factors_collection.find({"symbol": symbol}):
                one["adjust_factor"] *= adj
                print(one)
                lst.append(one)
            lst.append({"symbol": symbol, "adjust_factor": 1.0, "tradedate": dt})
            adjust_factors_collection.delete_many({"symbol": symbol})
            adjust_factors_collection.insert_many(lst)

        lhb_collection = conn["finance"]["lhb"]
        lhb_data = lhb.addition_read_lhb(
            api.lhb_addition_path, api.lhb_inst_addition_path
        )
        if lhb_data:
            lhb_collection.insert_many(lhb_data)
            logging.info(f"写入MongoDB[finance][lhb]")
        else:
            logging.error(f"写入MongoDB[finance][lhb]为空")

        cb_basic_df = cb.read_basic(api.basic_cb_path)
        data_saver.mongo_insert_many(
            cb_basic_df,
            "finance",
            "basic_info_cbs",
            ignore_nan=True,
        )
