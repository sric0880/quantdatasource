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
