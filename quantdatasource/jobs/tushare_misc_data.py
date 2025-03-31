import logging

import pandas as pd
from quantdata.databases._tdengine import get_data

from quantdatasource.api.tushare import TushareApi
from quantdatasource.dbimport import mongodb
from quantdatasource.jobs.account import *
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
    conn = mongodb.get_conn_mongodb()
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
    service_type="datasource-all",
    trigger="cron",
    id="astock_tushare",
    name="[TushareApi]其他",
    replace_existing=True,
    hour=20,
    minute=30,
    misfire_grace_time=200,
)
def tushare_misc_data(dt, is_collect, is_import):
    api = TushareApi(tushare_token, astock_output, dt)
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
        from quantdatasource.dbimport import tdengine
        from quantdatasource.dbimport.tushare import (
            cb,
            lhb,
            stock,
            stock_utils,
            ths_index,
        )

        stock_basic_df = stock.read_basic(api.basic_stock_path)
        mongodb.insert_many(stock_basic_df, "finance", "basic_info_stocks")

        # 创建子表
        bars_tablenames = []
        bars_tags = []
        daily_stock_bars_tablenames = []
        daily_bars_tags = []
        for row in stock_basic_df.itertuples():
            symbol = row.symbol
            for interval in ["1D", "w", "mon"]:
                if interval == "1D":
                    daily_stock_bars_tablenames.append(
                        tdengine.get_tbname(symbol, stable="bars_stock_daily")
                    )
                    daily_bars_tags.append((symbol,))
                else:
                    bars_tablenames.append(
                        tdengine.get_tbname(f"{symbol}_{interval}", stable="bars")
                    )
                    bars_tags.append((symbol, interval))
        tdengine.create_child_tables(
            daily_stock_bars_tablenames, "bars_stock_daily", daily_bars_tags
        )
        tdengine.create_child_tables(bars_tablenames, "bars", bars_tags)

        concepts_basic_df = ths_index.read_ths_concepts_basic(
            api.ths_index_a_concepts_path
        )
        mongodb.insert_many(
            concepts_basic_df,
            "finance",
            "basic_info_ths_concepts",
        )
        conn = mongodb.get_conn_mongodb()
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

        bars_tablenames = []
        bars_tags = []
        for row in concepts_basic_df.itertuples():
            symbol = row.symbol
            bars_tablenames.append(
                tdengine.get_tbname(symbol, stable="bars_ths_index_daily")
            )
            bars_tags.append((symbol,))
        tdengine.create_child_tables(bars_tablenames, "bars_ths_index_daily", bars_tags)
        tdengine.insert_multi_tables(
            ths_index.addition_read_concepts_bars(
                api.ths_daily_bars_addition_path, concepts_basic_df
            ),
            "bars_ths_index_daily",
        )

        chinese_names = dict(zip(stock_basic_df["symbol"], stock_basic_df["name"]))
        daily_df = pd.read_csv(api.daily_bars_addition_path, index_col=0)
        to_create_symbols = tdengine.not_exist_symbols(
            daily_df["ts_code"].to_list(), "bars_stock_daily"
        )
        logging.info(f"需要新建的表：{to_create_symbols}")
        tdengine.create_child_tables(
            [
                tdengine.get_tbname(tb, stable="bars_stock_daily")
                for tb in to_create_symbols
            ],
            "bars_stock_daily",
            [(symbol,) for symbol in to_create_symbols],
        )
        tdengine.create_child_tables(
            [tdengine.get_tbname(f"{tb}_w", stable="bars") for tb in to_create_symbols],
            "bars",
            [(symbol, "w") for symbol in to_create_symbols],
        )
        tdengine.create_child_tables(
            [
                tdengine.get_tbname(f"{tb}_mon", stable="bars")
                for tb in to_create_symbols
            ],
            "bars",
            [(symbol, "mon") for symbol in to_create_symbols],
        )
        daily_bars, dr_symbols, market_stats = stock.addition_read_stock_daily_bars(
            dt,
            api.daily_bars_addition_path,
            api.daily_basic_addition_path,
            api.moneyflow_addition_path,
            chinese_names,
            conn["finance"],
        )
        tdengine.insert_multi_tables(daily_bars, "bars_stock_daily")
        logging.info(f"  股票详情日线导入完成")

        adjust_factors_collection = conn["finance"]["adjust_factors"]
        for symbol in dr_symbols:
            close_df = get_data(
                symbol,
                stable="bars_stock_daily",
                fields=["dt", "close", "preclose"],
            )
            adj_df = stock_utils.cal_adjust_factors(symbol, close_df)
            adjust_factors_collection.delete_many({"symbol": symbol})
            adjust_factors_collection.insert_many(adj_df.to_dict(orient="records"))
        tdengine.insert_one(market_stats, "market_stats")
        logging.info(f"  市场统计数据导入完成")
        stock_utils.calc_bars_stock_week_and_month_and_import_to_tdengine(
            adjust_factors_collection, dr_symbols
        )

        lhb_collection = conn["finance"]["lhb"]
        lhb_collection.insert_many(lhb.addition_read_lhb(api.lhb_addition_path, api.lhb_inst_addition_path))
        logging.info(f"写入MongoDB[finance][lhb]")

        cb_basic_df = cb.read_basic(api.basic_cb_path)
        mongodb.insert_many(
            cb_basic_df,
            "finance",
            "basic_info_cbs",
            ignore_nan=True,
        )
        # 创建子表
        bars_tablenames = []
        bars_tags = []
        for row in cb_basic_df.itertuples():
            symbol = row.ts_code
            bars_tablenames.append(tdengine.get_tbname(symbol, stable="bars_cb_daily"))
            bars_tags.append((symbol,))
        tdengine.create_child_tables(bars_tablenames, "bars_cb_daily", bars_tags)


@job(
    service_type="datasource-all",
    id="astock_tushare_daily_fillup",
    name="[TushareApi]补全历史A股日线[Only Import](未测试)",
)
def tushare_daily_bars(dt, is_collect, is_import):
    api = TushareApi(tushare_token, astock_output, dt)

    from quantdatasource.dbimport import tdengine
    from quantdatasource.dbimport.tushare import stock, stock_utils

    stock_basic_df = stock.read_basic(api.basic_stock_path)
    conn = mongodb.get_conn_mongodb()
    # 如果要补充历史某天的数据，调用如下函数，所有股的前复权价格都要重算
    chinese_names = dict(zip(stock_basic_df["symbol"], stock_basic_df["name"]))
    daily_bars, dr_symbols, market_stats = stock.addition_read_stock_daily_bars(
        dt,
        api.daily_bars_addition_path,
        api.daily_basic_addition_path,
        api.moneyflow_addition_path,
        chinese_names,
        conn["finance"],
    )
    tdengine.insert_multi_tables(daily_bars, "bars_stock_daily")
    logging.info(f"  股票详情日线导入完成")

    adjust_factors_collection = conn["finance"]["adjust_factors"]
    for symbol in stock_utils.get_all_symbols():
        close_df = get_data(
            symbol,
            stable="bars_stock_daily",
            fields=["dt", "close", "preclose"],
        )
        adj_df = stock_utils.cal_adjust_factors(symbol, close_df)
        adjust_factors_collection.delete_many({"symbol": symbol})
        adjust_factors_collection.insert_many(adj_df.to_dict(orient="records"))

    stock_utils.calc_all_bars_stock_week_and_month_and_import_to_tdengine(
        adjust_factors_collection
    )

    market_df = stock_utils.calc_market_stats(stock_basic_df)
    tdengine.insert(market_df, "market_stats")
