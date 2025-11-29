# 大盘指数数据
import logging

import pandas as pd


def addition_read_index(filepath):
    if not filepath.exists():
        logging.info(f"读取大盘指数 没有 {filepath}")
        return None
    df: pd.DataFrame = pd.read_csv(filepath, index_col=0)
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df = df.reset_index(drop=True)
    df = df.rename(columns={"trade_date": "dt", "vol": "volume", "ts_code": "symbol"})
    df = df.astype(
        {
            "volume": "uint64",
            "amount": "uint64",
            "close": "float32",
            "open": "float32",
            "high": "float32",
            "low": "float32",
            "dt": "datetime64[ms]",
        }
    )
    df = df.drop(columns=["pre_close", "change", "pct_chg"])
    return df
