import logging
from ast import literal_eval

import pandas as pd


def read_future_basic(future_basic_path):
    logging.info("期货合约基本信息导入完毕")
    df = pd.read_csv(future_basic_path)
    df = df.drop(
        columns=[
            "ins_class",
            "instrument_name",
            "underlying_symbol",
            "strike_price",
            "exchange_id",
            "product_id",
            "expired",
            "last_exercise_datetime",
            "exercise_month",
            "exercise_year",
            "option_class",
            "trading_time_day",
            "trading_time_night",
        ]
    )
    df["instrument_id"] = df["instrument_id"].map(lambda x: x[x.rindex(".") + 1 :])
    df = df.astype({"instrument_id": "string"})
    df = df.rename(columns={"instrument_id": "_id"})
    return df


exchange_map = {
    "CZCE": "ZCE",  # 郑州商品交易所
    "SHFE": "SHF",  # 上海期货交易所
    "DCE": "DCE",  # 大连商品交易所
    "CFFEX": "CFX",  # 中国金融期货交易所
    "INE": "INE",  # 上海国际能源交易所
    "GFEX": "GFE",  # 广州期货交易所
}


def read_future_products_basic(product_basic_path):
    logging.info("读取期货品种基本信息")
    df = pd.read_csv(product_basic_path, index_col=0)
    df.index.name = "_id"
    df = df.reset_index()
    df["exchange"] = df["exchange"].map(exchange_map)
    df["cont_symbols"] = df["cont_symbols"].apply(
        lambda x: literal_eval(x) if not pd.isna(x) else []
    )
    return df
