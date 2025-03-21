import copy
import logging
import os
from collections import defaultdict
from datetime import timedelta
from decimal import ROUND_HALF_UP, Decimal

import numpy as np
import pandas as pd
from quantdata.databases._tdengine import get_data, get_data_last_row


def my_round(a):
    return float(a.quantize(Decimal("1.00"), rounding=ROUND_HALF_UP))


def to_decimal(a):
    return Decimal(a).quantize(Decimal("1.00"), rounding=ROUND_HALF_UP)


def apply_yiziban(row):
    if row["open"] == row["high"] == row["low"] == row["close"]:
        return 2
    else:
        return 1


def _set_updown_perctg(symbol, df):
    df["updown_perctg"] = 0.1
    if symbol.startswith("3"):  # 创业板
        df.loc[df.index > pd.Timestamp(2020, 8, 23), "updown_perctg"] = 0.2
    elif symbol.startswith("68"):  # 科创板
        df["updown_perctg"] = 0.2
    elif symbol.endswith("BJ"):  # 北交所
        df["updown_perctg"] = 0.3
    df.loc[df["name"].astype(str).str.contains("ST"), "updown_perctg"] = 0.05
    df.loc[df["name"].astype(str).str.contains("退"), "updown_perctg"] = 0.05
    # 尚未进行股权分置改革以S开头，2007年1月8日起，日涨跌幅调整为上下5%
    df.loc[
        (df["name"].astype(str).str.contains("S"))
        & (df.index > pd.Timestamp(2007, 1, 7)),
        "updown_perctg",
    ] = 0.05
    # print(df['updown_perctg'])


def maxupordown_status(symbol, price, kline):
    # 计算涨跌停价格
    stock_name = kline["name"]
    updown_perctg = 0.1
    if symbol.startswith("3"):  # 创业板
        updown_perctg = 0.2
    elif symbol.startswith("68"):  # 科创板
        updown_perctg = 0.2
    elif symbol.endswith("BJ"):  # 北交所
        updown_perctg = 0.3
    if "ST" in stock_name:
        updown_perctg = 0.05
    if "退" in stock_name:
        updown_perctg = 0.05

    preclose = kline["preclose"]

    preclose = to_decimal(preclose)
    updown_perctg = to_decimal(updown_perctg)
    yiziban = apply_yiziban(kline)
    change = preclose * updown_perctg
    upper_price = preclose + change
    lower_price = preclose - change
    upper_price = my_round(upper_price)
    lower_price = my_round(lower_price)
    upper_diff = 1 if abs(price - upper_price) < 0.001 else 0
    lower_diff = -1 if abs(price - lower_price) < 0.001 else 0
    return (upper_diff + lower_diff) * yiziban


def cal_adjust_factors(symbol, close_df):
    # 重新计算复权因子
    # 计算复权因子(前复权)
    logging.info(f"计算 {symbol} 前复权因子")
    adj_prices = close_df["preclose"] / close_df["close"].shift(1)
    adjust_factors = adj_prices.iloc[::-1].cumprod().iloc[::-1].shift(-1, fill_value=1)
    adj_df = pd.DataFrame(
        {"adjust_factor": adjust_factors, "tradedate": close_df["dt"]}
    )
    adj_df = adj_df.drop_duplicates(subset=["adjust_factor"])
    adj_df["symbol"] = symbol
    return adj_df


def get_all_symbols():
    _symbols = get_data("bars_stock_daily", fields=["DISTINCT symbol"], use_df=False)
    return _symbols["symbol"]


def fill_up_moneyflow(output):
    # 补充资金流
    # run source get_null_moneyflow.sql to get null_moneyflow.csv
    nulls_df = pd.read_csv(os.path.join("sql", "null_moneyflow.csv"))
    nulls_df["dt"] = pd.to_datetime(nulls_df["dt"]) - pd.Timedelta(value=8, unit="h")
    _cache_mf_dfs = {}
    for row in nulls_df.itertuples():
        dt = row.dt.tz_localize(tz="UTC")
        dt_str = dt.strftime("%Y%m%d")
        moneyflow_df = _cache_mf_dfs.get(dt_str, None)
        if moneyflow_df is None:
            _cache_mf_dfs[dt_str] = moneyflow_df = pd.read_csv(
                os.path.join(output, "moneyflow", f"{dt_str}.csv"), index_col=0
            )
            moneyflow_df["trade_date"] = pd.to_datetime(
                moneyflow_df["trade_date"], format="%Y%m%d"
            )
            # print(moneyflow_df)
        _symbol_moneyflow_df = moneyflow_df.loc[moneyflow_df["ts_code"] == row.symbol]
        if _symbol_moneyflow_df.empty:
            pass
            # print(f"{dt_str}， {row.symbol} 无资金流数据")
        else:
            logging.info(f"-- {dt_str} 重新导入 {row.symbol} 资金流数据完毕")
            # 补充资金流数据
            _moneyflow_data = _symbol_moneyflow_df.iloc[0]
            new_kline = {
                "dt": dt,
                "vip_buy_amt": _moneyflow_data.buy_lg_amount,
                "vip_sell_amt": _moneyflow_data.sell_lg_amount,
                "inst_buy_amt": _moneyflow_data.buy_elg_amount,
                "inst_sell_amt": _moneyflow_data.sell_elg_amount,
                "mid_buy_amt": _moneyflow_data.buy_md_amount,
                "mid_sell_amt": _moneyflow_data.sell_md_amount,
                "indi_buy_amt": _moneyflow_data.buy_sm_amount,
                "indi_sell_amt": _moneyflow_data.sell_sm_amount,
            }
            new_kline["vip_net_flow_in"] = (
                new_kline["vip_buy_amt"] - new_kline["vip_sell_amt"]
            )
            new_kline["inst_net_flow_in"] = (
                new_kline["inst_buy_amt"] - new_kline["inst_sell_amt"]
            )
            new_kline["mid_net_flow_in"] = (
                new_kline["mid_buy_amt"] - new_kline["mid_sell_amt"]
            )
            new_kline["indi_net_flow_in"] = (
                new_kline["indi_buy_amt"] - new_kline["indi_sell_amt"]
            )
            new_kline["master2_net_flow_in"] = (
                new_kline["mid_net_flow_in"]
                + new_kline["vip_net_flow_in"]
                + new_kline["inst_net_flow_in"]
            )
            new_kline["master_net_flow_in"] = (
                new_kline["vip_net_flow_in"] + new_kline["inst_net_flow_in"]
            )
            new_kline["total_sell_amt"] = (
                new_kline["mid_sell_amt"]
                + new_kline["indi_sell_amt"]
                + new_kline["vip_sell_amt"]
                + new_kline["inst_sell_amt"]
            )
            new_kline["total_buy_amt"] = (
                new_kline["mid_buy_amt"]
                + new_kline["indi_buy_amt"]
                + new_kline["vip_buy_amt"]
                + new_kline["inst_buy_amt"]
            )
            new_kline["net_flow_in"] = (
                new_kline["total_buy_amt"] - new_kline["total_sell_amt"]
            )
            yield pd.DataFrame([new_kline])


def calc_market_stats(stock_basic_df):
    #  统计全市场指标
    # 还是之前读取pkl那一套
    all_market_maxupordown = defaultdict(list)
    all_market_lb_counts = defaultdict(dict)
    for row in stock_basic_df.itertuples():
        symbol = row.symbol
        print(symbol)
        daily_file = os.path.join("datas/AStock", "daily", f"{symbol}.pkl")
        if not os.path.exists(daily_file):
            logging.warning(f"没有日线{symbol}数据，可能还未上市")
            continue
        df = pd.read_pickle(daily_file)

        # 全市场统计
        for row in df.itertuples():
            stock_name = row.stock_name
            if "ST" in stock_name or "退" in stock_name:
                continue
            dt = row.Index
            _lb_up_count = row.lb_up_count
            _maxupordown_status = row.maxupordown
            all_market_maxupordown[dt].append(_maxupordown_status)
            if _lb_up_count > 0:
                c = all_market_lb_counts[dt].setdefault(_lb_up_count, 0)
                c += 1
                all_market_lb_counts[dt][_lb_up_count] = c

    # 全市场统计汇总
    rows = []
    for dt, maxupordown_list in all_market_maxupordown.items():
        all_stock_number = len(maxupordown_list)
        count_of_uplimit = 0
        count_of_downlimit = 0
        count_of_yiziup = 0
        count_of_yizidown = 0
        for status in maxupordown_list:
            if status > 0:
                count_of_uplimit += 1
            if status < 0:
                count_of_downlimit += 1
            if status == 2:
                count_of_yiziup += 1
            if status == -2:
                count_of_yizidown += 1
        ratio_of_uplimit = count_of_uplimit / all_stock_number
        ratio_of_downlimit = count_of_downlimit / all_stock_number
        ratio_of_yiziup = count_of_yiziup / all_stock_number
        ratio_of_yizidown = count_of_yizidown / all_stock_number
        row = [
            dt,
            count_of_uplimit,
            count_of_downlimit,
            count_of_yiziup,
            count_of_yizidown,
            ratio_of_uplimit,
            ratio_of_downlimit,
            ratio_of_yiziup,
            ratio_of_yizidown,
        ]
        lb_counts = all_market_lb_counts[dt]
        row.extend([lb_counts.get(i, 0) for i in range(1, 13)])
        rows.append(row)
    market_df = pd.DataFrame(
        rows,
        columns=[
            "dt",
            "count_of_uplimit",
            "count_of_downlimit",
            "count_of_yiziup",
            "count_of_yizidown",
            "ratio_of_uplimit",
            "ratio_of_downlimit",
            "ratio_of_yiziup",
            "ratio_of_yizidown",
            "lb1",
            "lb2",
            "lb3",
            "lb4",
            "lb5",
            "lb6",
            "lb7",
            "lb8",
            "lb9",
            "lb10",
            "lb11",
            "lb12",
        ],
    )
    market_df = market_df.sort_values(by="dt")
    market_df = market_df.astype(np.int32, errors="ignore")
    market_df = market_df.astype(
        {
            "ratio_of_uplimit": "float32",
            "ratio_of_downlimit": "float32",
            "ratio_of_yiziup": "float32",
            "ratio_of_yizidown": "float32",
        }
    )
    # print(market_df)
    # print(market_df.info())
    return market_df


def calc_bars_stock_week_and_month_and_import_to_tdengine(
    adjust_factors_collection, symbols=[]
):
    from quantdatasource.dbimport import tdengine

    # 当更新日线完成之后，将通过日线生成日线、周线和月线 (前复权)
    week_insert_datas = []
    month_insert_datas = []
    for symbol in get_all_symbols():
        _week_tbname = symbol + "_w"
        _month_tbname = symbol + "_mon"
        if symbol in symbols:
            # 删除之前的表
            logging.info(f"{symbol} 今日除权，需要重新计算前复权日线、周线、月线")
            _d = get_data(
                symbol,
                stable="bars_stock_daily",
                fields=["dt", "open", "high", "low", "close", "volume", "amount"],
                use_df=True,
            )
            if _d.empty:
                continue
            adjs = adjust_factors_collection.find(
                {"symbol": symbol},
                projection={"tradedate": 1, "adjust_factor": 1, "_id": 0},
            )
            adj_df = pd.DataFrame(adjs)
            if not adj_df.empty:
                adj_df = adj_df.rename(columns={"tradedate": "dt"})
                adj_df["dt"] = adj_df["dt"].dt.tz_localize("UTC")
                _d = pd.merge_ordered(left=_d, right=adj_df, on="dt", how="left")
                _d["adjust_factor"] = _d["adjust_factor"].fillna(method="ffill")
                _d["open"] = _d["open"] * _d["adjust_factor"]
                _d["high"] = _d["high"] * _d["adjust_factor"]
                _d["low"] = _d["low"] * _d["adjust_factor"]
                _d["close"] = _d["close"] * _d["adjust_factor"]
            _week_df = _daily_to_week(_d)
            _month_df = _daily_to_month(_d)
            tbnames = [_week_tbname, _month_tbname]
            tdengine.drop_tables(tbnames, "bars")
            tdengine.create_child_tables(
                [tdengine.get_tbname(tbname, stable="bars") for tbname in tbnames],
                "bars",
                [(symbol, "w"), (symbol, "mon")],
            )
            tdengine.insert(_week_df, _week_tbname, stable="bars")
            logging.info(f"-- 重新导入 {symbol} 周线完毕")
            tdengine.insert(_month_df, _month_tbname, stable="bars")
            logging.info(f"-- 重新导入 {symbol} 月线完毕")
            daily_df = _d[["dt", "open", "high", "low", "close"]]
            daily_df = daily_df.rename(
                columns={
                    "open": "open_",
                    "high": "high_",
                    "low": "low_",
                    "close": "close_",
                }
            )
            tdengine.insert(daily_df, symbol, stable="bars_stock_daily", whole_df=False)
            logging.info(f"-- 重新导入 {symbol} 日线前复权价格完毕")
            adjs.close()
        else:
            last_row = get_data_last_row(
                symbol,
                stable="bars_stock_daily",
                fields=["dt", "open", "high", "low", "close", "volume", "amount"],
            )

            last_week_row = get_data_last_row(
                _week_tbname,
                stable="bars",
                fields=["dt", "open", "high", "low", "close", "volume", "amount"],
            )
            if (
                last_week_row is not None
                and last_row["dt"].isocalendar()[1]
                == last_week_row["dt"].isocalendar()[1]
            ):
                tdengine.drop_row(
                    _week_tbname,
                    last_week_row["dt"] - timedelta(hours=8),
                    stable="bars",
                )
                tdengine.drop_row(_week_tbname, last_week_row["dt"], stable="bars")
                last_week_row["dt"] = last_row["dt"]
                last_week_row["high"] = max(last_week_row["high"], last_row["high"])
                last_week_row["low"] = min(last_week_row["low"], last_row["low"])
                last_week_row["close"] = last_row["close"]
                last_week_row["volume"] += last_row["volume"]
                last_week_row["amount"] += last_row["amount"]
            else:
                last_week_row = copy.copy(last_row)
            last_week_row["tablename"] = _week_tbname
            week_insert_datas.append(last_week_row)

            last_month_row = get_data_last_row(
                _month_tbname,
                stable="bars",
                fields=["dt", "open", "high", "low", "close", "volume", "amount"],
            )
            if (
                last_month_row is not None
                and last_row["dt"].month == last_month_row["dt"].month
            ):
                tdengine.drop_row(
                    _month_tbname,
                    last_month_row["dt"] - timedelta(hours=8),
                    stable="bars",
                )
                tdengine.drop_row(_month_tbname, last_month_row["dt"], stable="bars")
                last_month_row["dt"] = last_row["dt"]
                last_month_row["high"] = max(last_month_row["high"], last_row["high"])
                last_month_row["low"] = min(last_month_row["low"], last_row["low"])
                last_month_row["close"] = last_row["close"]
                last_month_row["volume"] += last_row["volume"]
                last_month_row["amount"] += last_row["amount"]
            else:
                last_month_row = copy.copy(last_row)
            last_month_row["tablename"] = _month_tbname
            month_insert_datas.append(last_month_row)

    for _datas in [week_insert_datas, month_insert_datas]:
        _df = pd.DataFrame(_datas)
        if _df.empty:
            continue
        _df = _df.astype(
            {
                "open": "float32",
                "high": "float32",
                "low": "float32",
                "close": "float32",
                "volume": "int64",
                "amount": "float64",
            }
        )
        tdengine.insert_multi_tables(_df, "bars")
    logging.info("前复权周线、月线导入完毕")


def calc_all_bars_stock_week_and_month_and_import_to_tdengine(
    adjust_factors_collection,
):
    from quantdatasource.dbimport import tdengine

    # 当更新日线完成之后，将通过日线生成日线、周线和月线 (前复权)
    for symbol in get_all_symbols():
        _week_tbname = symbol + "_w"
        _month_tbname = symbol + "_mon"
        # 删除之前的表
        logging.info(f"{symbol} 重新计算前复权日线、周线、月线")
        _d = get_data(
            symbol,
            stable="bars_stock_daily",
            fields=["dt", "open", "high", "low", "close", "volume", "amount"],
            use_df=True,
        )
        if _d.empty:
            continue
        adjs = adjust_factors_collection.find(
            {"symbol": symbol},
            projection={"tradedate": 1, "adjust_factor": 1, "_id": 0},
        )
        adj_df = pd.DataFrame(adjs)
        if not adj_df.empty:
            adj_df = adj_df.rename(columns={"tradedate": "dt"})
            adj_df["dt"] = adj_df["dt"].dt.tz_localize("UTC")
            _d = pd.merge_ordered(left=_d, right=adj_df, on="dt", how="left")
            _d["adjust_factor"] = _d["adjust_factor"].fillna(method="ffill")
            _d["open"] = _d["open"] * _d["adjust_factor"]
            _d["high"] = _d["high"] * _d["adjust_factor"]
            _d["low"] = _d["low"] * _d["adjust_factor"]
            _d["close"] = _d["close"] * _d["adjust_factor"]
        _week_df = _daily_to_week(_d)
        _month_df = _daily_to_month(_d)
        tbnames = [_week_tbname, _month_tbname]
        tdengine.drop_tables(tbnames, "bars")
        tdengine.create_child_tables(
            [tdengine.get_tbname(tbname, stable="bars") for tbname in tbnames],
            "bars",
            [(symbol, "1D"), (symbol, "w"), (symbol, "mon")],
        )
        tdengine.insert(_week_df, _week_tbname, stable="bars")
        tdengine.insert(_month_df, _month_tbname, stable="bars")
        daily_df = _d[["dt", "open", "high", "low", "close"]]
        daily_df = daily_df.rename(
            columns={"open": "open_", "high": "high_", "low": "low_", "close": "close_"}
        )
        tdengine.insert(daily_df, symbol, stable="bars_stock_daily", whole_df=False)
        logging.info(f"-- 重新导入 {symbol} 日线前复权价格完毕")
        adjs.close()
    logging.info("前复权周线、月线导入完毕")


def _daily_to_week(df: pd.DataFrame):
    # 合成周线和月线
    week_df = df.groupby(pd.Grouper(key="dt", freq="W")).agg(
        {
            "dt": "last",
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "amount": "sum",
        }
    )
    week_df = week_df.dropna(axis=0)
    week_df = week_df.reset_index(drop=True)
    week_df = week_df.astype({"amount": "float64"})
    return week_df


def _daily_to_month(df: pd.DataFrame):
    month_df = df.groupby(pd.Grouper(key="dt", freq="M")).agg(
        {
            "dt": "last",
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "amount": "sum",
        }
    )
    month_df = month_df.dropna(axis=0)
    month_df = month_df.reset_index(drop=True)
    month_df = month_df.astype({"amount": "float64"})
    return month_df
