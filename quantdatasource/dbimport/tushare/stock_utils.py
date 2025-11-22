import logging
import math
from decimal import ROUND_HALF_UP, Decimal

import pandas as pd


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


def cal_adjust_factors(daily_bars, yesterday_daily_bars):
    # 重新计算复权因子
    # 计算复权因子(前复权)
    ret = {}
    df = pd.concat([yesterday_daily_bars, daily_bars])
    for symbol, sd in df.groupby(by="symbol"):
        if len(sd) < 2:
            continue
        adj_preclose = sd["preclose"].iloc[1]
        preclose = sd["close"].iloc[0]
        if not math.isclose(adj_preclose, preclose, abs_tol=0.001):
            adj = adj_preclose / preclose
            logging.info(f"计算 {symbol} 前复权因子 {adj}")
            ret[symbol] = adj
    return ret


# def calc_market_stats(stock_basic_df):
#     #  统计全市场指标
#     all_market_maxupordown = defaultdict(list)
#     all_market_lb_counts = defaultdict(dict)
#     for row in stock_basic_df.itertuples():
#         symbol = row.symbol
#         print(symbol)
#         daily_file = os.path.join("datas/AStock", "daily", f"{symbol}.pkl")
#         if not os.path.exists(daily_file):
#             logging.warning(f"没有日线{symbol}数据，可能还未上市")
#             continue
#         df = pd.read_pickle(daily_file)

#         # 全市场统计
#         for row in df.itertuples():
#             stock_name = row.stock_name
#             if "ST" in stock_name or "退" in stock_name:
#                 continue
#             dt = row.Index
#             _lb_up_count = row.lb_up_count
#             _maxupordown_status = row.maxupordown
#             all_market_maxupordown[dt].append(_maxupordown_status)
#             if _lb_up_count > 0:
#                 c = all_market_lb_counts[dt].setdefault(_lb_up_count, 0)
#                 c += 1
#                 all_market_lb_counts[dt][_lb_up_count] = c

#     # 全市场统计汇总
#     rows = []
#     for dt, maxupordown_list in all_market_maxupordown.items():
#         all_stock_number = len(maxupordown_list)
#         count_of_uplimit = 0
#         count_of_downlimit = 0
#         count_of_yiziup = 0
#         count_of_yizidown = 0
#         for status in maxupordown_list:
#             if status > 0:
#                 count_of_uplimit += 1
#             if status < 0:
#                 count_of_downlimit += 1
#             if status == 2:
#                 count_of_yiziup += 1
#             if status == -2:
#                 count_of_yizidown += 1
#         ratio_of_uplimit = count_of_uplimit / all_stock_number
#         ratio_of_downlimit = count_of_downlimit / all_stock_number
#         ratio_of_yiziup = count_of_yiziup / all_stock_number
#         ratio_of_yizidown = count_of_yizidown / all_stock_number
#         row = [
#             dt,
#             count_of_uplimit,
#             count_of_downlimit,
#             count_of_yiziup,
#             count_of_yizidown,
#             ratio_of_uplimit,
#             ratio_of_downlimit,
#             ratio_of_yiziup,
#             ratio_of_yizidown,
#         ]
#         lb_counts = all_market_lb_counts[dt]
#         row.extend([lb_counts.get(i, 0) for i in range(1, 13)])
#         rows.append(row)
#     market_df = pd.DataFrame(
#         rows,
#         columns=[
#             "dt",
#             "count_of_uplimit",
#             "count_of_downlimit",
#             "count_of_yiziup",
#             "count_of_yizidown",
#             "ratio_of_uplimit",
#             "ratio_of_downlimit",
#             "ratio_of_yiziup",
#             "ratio_of_yizidown",
#             "lb1",
#             "lb2",
#             "lb3",
#             "lb4",
#             "lb5",
#             "lb6",
#             "lb7",
#             "lb8",
#             "lb9",
#             "lb10",
#             "lb11",
#             "lb12",
#         ],
#     )
#     market_df = market_df.sort_values(by="dt")
#     market_df = market_df.astype(np.int32, errors="ignore")
#     market_df = market_df.astype(
#         {
#             "ratio_of_uplimit": "float32",
#             "ratio_of_downlimit": "float32",
#             "ratio_of_yiziup": "float32",
#             "ratio_of_yizidown": "float32",
#         }
#     )
#     # print(market_df)
#     # print(market_df.info())
#     return market_df


def apply_adjust_factor(dir, symbol, adj):
    df = pd.read_feather(dir / f"{symbol}.parquet")
    df["open"] = df["open"] * adj
    df["high"] = df["high"] * adj
    df["low"] = df["low"] * adj
    df["close"] = df["close"] * adj
    return df
