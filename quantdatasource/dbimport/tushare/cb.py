import logging
from pathlib import Path

import pandas as pd

intervals = ["1D"]


def read_basic(basic_cb_path):
    logging.info("读取可转债基本信息")
    return pd.read_csv(
        basic_cb_path,
        dtype={
            "ts_code": str,
            "list_date": str,
            "delist_date": str,
            "maturity_date": str,
            "value_date": str,
            "conv_start_date": str,
            "conv_end_date": str,
            "conv_stop_date": str,
        },
        index_col=0,
    )


def read_cb_daily(symbol, cb_daily_path):
    cb_daily_csv = Path(cb_daily_path, f"{symbol}.csv")
    df = pd.read_csv(cb_daily_csv)
    if df.empty:
        logging.warning(f"读取可转债日线 {symbol} 为空")
        return
    logging.info(f"读取可转债日线 {symbol}")
    df = df.iloc[::-1]
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df = df.rename(
        columns={"trade_date": "dt", "vol": "volume", "pre_close": "preclose"}
    )
    df = df.fillna(0)
    dtypes = {
        "volume": "int32",
        "amount": "int64",
    }
    df = df.astype(dtypes)
    df = df[
        [
            "dt",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "preclose",
            "change",
            "pct_chg",
            "cb_value",
            "cb_over_rate",
            "bond_over_rate",
            "bond_value",
        ]
    ]
    return df


def addition_read_cb_daily(dt, cb_daily_addition_path, basic_cb_path):
    tradedt = dt.strftime("%Y%m%d")
    if not cb_daily_addition_path.exists():
        logging.error(f"增量读取可转债日线 {tradedt} {cb_daily_addition_path} 不存在")
        return
    basic_df = read_basic(basic_cb_path)
    logging.info(f"增量读取可转债日线 {tradedt}")
    df: pd.DataFrame = pd.read_csv(cb_daily_addition_path, index_col=0)
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").astype("datetime64[ms]")
    df = df.reset_index(drop=True)
    df = df.rename(
        columns={
            "trade_date": "dt",
            "vol": "volume",
            "ts_code": "symbol",
            "pre_close": "preclose",
        }
    )
    dtypes = {
        "symbol": "str",
        "open": "float32",
        "high": "float32",
        "low": "float32",
        "close": "float32",
        "volume": "uint32",
        "amount": "uint64",
        "preclose": "float32",
        "change": "float32",
        "pct_chg": "float32",
        "cb_value": "float32",
        "cb_over_rate": "float32",
        "bond_over_rate": "float32",
        "bond_value": "float32",
    }
    df = df.astype(dtypes)
    df = df.fillna(0)
    # tags_lst = list(zip(df['ts_code'], periodname))
    df = df.loc[df["symbol"].isin(basic_df["ts_code"])]
    df = df[
        [
            "symbol",
            "dt",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "preclose",
            "change",
            "pct_chg",
            "cb_value",
            "cb_over_rate",
            "bond_over_rate",
            "bond_value",
        ]
    ]
    return df


def read_cb_call(symbol, cb_call_path):
    fields = ["dt", "call_price", "call_price_tax", "is_call", "call_type"]
    cb_call_csv = Path(cb_call_path, f"{symbol}.csv")
    if not cb_call_csv.exists():
        logging.error(f"可转债赎回数据 {cb_call_csv} not found")
        return pd.DataFrame(columns=fields)
    df = pd.read_csv(cb_call_csv)
    if df.empty:
        # logging.warning(f"读取可转债赎回数据 {symbol} 为空")
        return pd.DataFrame(columns=fields)
    df["call_type"] = df["call_type"].map({"强赎": 2, "到赎": 1})
    df["is_call"] = df["is_call"].map(
        {
            "已满足强赎条件": 1,
            "公告提示强赎": 2,
            "公告实施强赎": 3,
            "公告到期赎回": 4,
            "公告不强赎": 5,
        }
    )
    df["ann_date"] = pd.to_datetime(df["ann_date"], format="%Y%m%d")
    df = df.iloc[::-1]
    df = df.rename(columns={"ann_date": "dt"})
    df = df[fields]
    df = df.fillna(0)
    return df


def read_cb_share(symbol_basic_info, cb_share_path):
    symbol = symbol_basic_info["ts_code"]
    fields = ["dt", "convert_price", "remain_size"]
    if "list_date" not in symbol_basic_info:
        logging.warning(
            f"读取可转债转股数据 {symbol} 基本信息中无list_date，推测无日线行情"
        )
        return pd.DataFrame(columns=fields)
    first_conv_price = symbol_basic_info.get("first_conv_price", 0)
    list_date = pd.to_datetime(symbol_basic_info["list_date"], format="%Y%m%d")
    issue_size = symbol_basic_info["issue_size"]
    cb_share_csv = Path(cb_share_path, f"{symbol}.csv")
    df = pd.read_csv(cb_share_csv)
    first_day_df = pd.DataFrame(
        {
            "dt": [list_date],
            "convert_price": first_conv_price,
            "remain_size": issue_size,
        }
    )
    if not df.empty:
        df["publish_date"] = pd.to_datetime(df["publish_date"], format="%Y-%m-%d")
        df = df.iloc[::-1]
        df = df.rename(columns={"publish_date": "dt"})
        df = df[fields]
        df = pd.concat([first_day_df, df])
        df = df.drop_duplicates(subset=["dt"], keep="last")
    else:
        df = first_day_df
    df = df.astype(
        {
            "remain_size": "int64",
        }
    )
    df = df.fillna(0)
    df = df.reset_index(drop=True)
    return df
