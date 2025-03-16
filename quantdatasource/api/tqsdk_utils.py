import json
import logging
import os
import zipfile
from functools import cache
from pathlib import Path

import numpy as np
import pandas as pd

cannot_download_symbols = set(
    [
        "CZCE.ZC305"
        "CZCE.LR301"
        "CZCE.RI209"
        "CZCE.ZC304"
        "CZCE.PM301"
        "CZCE.PM011"
        "CZCE.LR101"
        "CZCE.LR203"
        "CZCE.RI305"
        "CZCE.RI211"
        "CZCE.LR303"
        "CZCE.RI103"
        "CZCE.PM303"
        "CZCE.CY102"
        "CZCE.JR303"
        "CZCE.ZC302"
        "CZCE.JR209"
        "CZCE.LR201"
        "CZCE.WH305"
        "CZCE.CY012"
        "CZCE.RI107"
        "CZCE.PM305"
        "CZCE.PM203"
        "CZCE.PM211"
        "CZCE.LR109"
        "CZCE.LR105"
        "CZCE.RI011"
        "CZCE.RI203"
        "CZCE.LR305"
        "CZCE.RI303"
        "CZCE.JR305"
        "CZCE.RI301"
        "CZCE.ZC303"
        "CZCE.LR211"
        "CZCE.RI109"
        "CZCE.LR103"
        "CZCE.LR111"
        "CZCE.RI205"
        "CZCE.RI105"
        "CZCE.CY103"
        "CZCE.CY011"
        "CZCE.PM205"
    ]
)


def correct_cont_history(cont_history):
    cont_history.loc[
        cont_history["date"] == pd.Timestamp(year=2021, month=4, day=12, tz="UTC"),
        "KQ.m@DCE.c",
    ] = "DCE.c2105"
    cont_history.loc[
        cont_history["date"] == pd.Timestamp(year=2021, month=12, day=6, tz="UTC"),
        "KQ.m@DCE.c",
    ] = "DCE.c2201"
    cont_history.loc[
        cont_history["date"] == pd.Timestamp(year=2022, month=4, day=6, tz="UTC"),
        "KQ.m@DCE.c",
    ] = "DCE.c2205"


def split_future_cont_list(cont_list_file):
    """
    将主力连续合约列表分成多个文件，用于多进程下载
    """
    with open(cont_list_file, "r") as f:
        contlist = json.load(f)
    _filters = [
        "SR",
        "au",
        "sp",
        "b",
        "AP",
        "ni",
        "v",
        "eb",
        "jd",
        "ss",
        "zn",
        "al",
        "OI",
        "SM",
        "UR",
        "m",
        "y",
        "l",
        "pg",
        "PK",
        "a",
        "jm",
        "FG",
        "rb",
        "bu",
        "CF",
        "MA",
        "j",
        "ag",
        "ru",
        "fu",
        "lh",
        "pp",
        "cu",
        "sn",
        "hc",
        "SA",
        "eg",
        "RM",
    ]
    contlist = [s for s in contlist if s[s.rindex(".") + 1 :] in _filters]
    split_arr = np.array_split(contlist, 8)
    for i, arr in enumerate(split_arr):
        with open(Path(cont_list_file.parent, f"cont_list_{i}.json"), "w") as f:
            json.dump(list(arr), f)


def get_close_price_diff(symbol, pre_symbol, dt):
    # print(symbol, pre_symbol, dt)
    # TODO: get_data from database
    if pd.isna(pre_symbol):
        return 0
    try:
        ds = get_data(
            symbol,
            stable="bars_ctpfuture_daily",
            fields=["dt", "close"],
            till_dt=dt,
            use_df=False,
        )
        pre_ds = get_data(
            pre_symbol,
            stable="bars_ctpfuture_daily",
            fields=["dt", "close"],
            till_dt=dt,
            use_df=False,
        )
    except:
        return 0
    dts = ds["dt"]
    pre_dts = pre_ds["dt"]
    i = -1
    pre_i = -1
    while True:
        if (len(dts) + i < 0) or (len(pre_dts) + pre_i < 0):
            return 0
        dt = dts[i]
        pre_dt = pre_dts[pre_i]
        if dt == pre_dt:
            break
        if dt > pre_dt:
            i -= 1
        else:
            pre_i -= 1
    return ds["close"][i] - pre_ds["close"][pre_i]


def to_tushare_symbol(symbol):
    dot = symbol.index(".")
    exchange = symbol[:dot]
    symbol = symbol[dot + 1 :]
    if exchange == "CZCE":
        expire_dt = symbol[2:]
        i = 1
        if expire_dt[0] in ["1", "0", "2", "3", "4"]:
            i = 2
        symbol = f"{symbol[:2]}{i}{expire_dt}"
    return symbol


def next_month(year, month):
    if month == 12:
        return year + 1, 1
    else:
        return year, month + 1


def is_file_empty(filename):
    if os.path.exists(filename):
        size = os.path.getsize(filename)
        return size < 1000
    return False


def zip_file(filename, zipfilename):
    zip = zipfile.ZipFile(zipfilename, "w", zipfile.ZIP_DEFLATED)
    zip.write(filename)
    zip.close()


@cache
def _get_stock_basic_df(output):
    from .tushare import TushareApi

    basic_stock_path = Path(output).parent.joinpath(TushareApi.dir, "stock_basic.csv")
    if not basic_stock_path.exists():
        logging.error(
            f"{basic_stock_path}不存在，必须先调用 TushareApi.full_download_stock_basic"
        )
        return None
    df = pd.read_csv(basic_stock_path, index_col=0)
    return df


def stock_is_on_list(symbol, year, month, output):
    # 是否是上市期间
    i = symbol.index(".")
    ex = symbol[:i]
    ex = "SH" if ex == "SSE" else "SZ"
    code = symbol[i + 1 :] + "." + ex
    df = _get_stock_basic_df(output)
    if df is None:
        return False
    code_df = df.loc[df["ts_code"] == code]
    if code_df.empty:
        logging.error(f"{symbol} 不在stock_basic列表里，查不到上市和退市时间")
        return False
    data = code_df.iloc[0]
    ipoyear, ipomonth = data["list_date"].year, data["list_date"].month
    if year < ipoyear or (year == ipoyear and month < ipomonth):
        return False
    outyear, outmonth = data["delist_date"].year, data["delist_date"].month
    if year > outyear or (year == outyear and month > outmonth):
        return False
    return True
