# 大盘指数数据
import logging

import pandas as pd


def addition_read_index(filepath, periodname):
    if not filepath.exists():
        logging.info(f"读取大盘指数 没有 {filepath}")
        return None
    df: pd.DataFrame = pd.read_csv(filepath, index_col=0)
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df = df.reset_index(drop=True)
    df = df.rename(columns={"trade_date": "dt", "vol": "volume"})
    df = df.astype({"volume": "int64"})
    df["tablename"] = df["ts_code"] + "_" + periodname
    df = df.drop(columns=["pre_close", "change", "pct_chg", "ts_code"])
    return df
