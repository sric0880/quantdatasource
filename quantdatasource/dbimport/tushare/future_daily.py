import logging
import re
from datetime import *

import pandas as pd


def _daily_to_week(df):
    # 合成周线和月线
    week_df = df.groupby(pd.Grouper(key="dt", freq="W")).agg(
        {
            "dt": "last",
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "settle": "last",
            "volume": "sum",
            "amount": "sum",
            "open_interest": "last",
        }
    )
    week_df = week_df.dropna(axis=0)
    week_df = week_df.reset_index(drop=True)
    week_df = week_df.astype({"open_interest": "int32"})
    # print(week_df)
    # print(week_df.info())
    return week_df


def read_daily_and_weekly(bars_history_path, bars_current_path):
    logging.info(f"读取期货K线")
    for basepath, is_history in [(bars_history_path, True), (bars_current_path, False)]:
        for csv in basepath.iterdir():
            if csv.suffix != ".csv":
                continue
            ret = re.match(r"(\w+).(\w+).csv", csv)
            symbol, exchange = ret.group(1), ret.group(2)
            # 只有 CZCE 郑州商品交易所 名称大写
            if exchange != "ZCE" and exchange != "CFX":
                symbol = symbol.lower()
            # if symbol != 'ag2308':
            #     continue
            logging.info((exchange, symbol))
            df = pd.read_csv(csv, index_col=0, dtype={"trade_date": "string"})
            if df.empty:
                logging.error(f"期货K线 {csv} 数据为空")
                continue

            df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
            df = df.drop(
                columns=[
                    "ts_code",
                    "oi_chg",
                    "change1",
                    "change2",
                    "pre_close",
                    "pre_settle",
                ]
            )
            df = df.rename(
                columns={"trade_date": "dt", "oi": "open_interest", "vol": "volume"}
            )
            df["amount"] = df["amount"].fillna(0)
            df["amount"] *= 10000
            df = df.dropna(axis=0)
            df = df.astype(
                {
                    "open": "float32",
                    "high": "float32",
                    "low": "float32",
                    "close": "float32",
                    "volume": "int32",
                    "amount": "int64",
                    "open_interest": "int32",
                    "settle": "float32",
                }
            )
            df = df[
                [
                    "dt",
                    "open",
                    "high",
                    "low",
                    "close",
                    "settle",
                    "volume",
                    "amount",
                    "open_interest",
                ]
            ]
            df = df.iloc[::-1]
            if df.empty:
                logging.error(f"期货K线 {csv} 字段存在空值")
                continue
            # print(df)
            # print(df.info())
            week_df = _daily_to_week(df)
            yield df, week_df, symbol, is_history
