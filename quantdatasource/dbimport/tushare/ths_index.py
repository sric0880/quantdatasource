# 同花顺指数（概念板块、行业、地域、特色指数）
import logging
from datetime import datetime

import pandas as pd


def read_ths_concepts_basic(csv):
    logging.info("读取同花顺概念板块基本信息")
    df = pd.read_csv(
        csv, dtype={"list_date": str, "ts_code": str, "name": str}, index_col=0
    )
    df["count"] = df["count"].fillna(0)
    df["count"] = df["count"].astype("int32")
    df = df.rename(columns={"ts_code": "symbol"})
    return df


def read_ths_concepts_constituent(ths_concepts_members_path):
    logging.info("读取同花顺概念成分股数据")
    dt = datetime.today()
    data = []
    for members_csv in ths_concepts_members_path.iterdir():
        members_df = pd.read_csv(members_csv)
        for row in members_df.itertuples():
            data.append((datetime(dt.year, dt.month, dt.day), row.code, 1, row.ts_code))
    df = pd.DataFrame(data, columns=["tradedate", "stock_code", "op", "index_code"])
    df["tradedate"] = pd.to_datetime(df["tradedate"])
    df = df.astype({"stock_code": "string", "op": "int8", "index_code": "string"})
    # print(_df)
    # print(_df.info())
    return df


def _get_current_constituent_of_index(df):
    stocks = set()
    for row in df.itertuples():
        if row.op == 1:
            stocks.add(row.stock_code)
        else:
            try:
                stocks.remove(row.stock_code)
            except KeyError:
                pass
    return stocks


def addition_read_ths_concepts_constituent(
    dt, all_ths_index_df, ths_concepts_members_path
):
    logging.info("增量读取同花顺概念成分股数据")
    addition_rows = []
    for members_csv in ths_concepts_members_path.iterdir():
        symbol = members_csv.name.replace(".csv", "")
        ths_idx_df = all_ths_index_df.loc[all_ths_index_df["index_code"] == symbol]
        stocks = _get_current_constituent_of_index(ths_idx_df)
        members_df = pd.read_csv(members_csv, index_col=0)
        if members_df.empty:
            logging.warning(f"无法增量导入概念成分股：{members_csv} 为空")
            continue
        latest_members = set(members_df["code"].to_list())
        add_symbols = latest_members - stocks
        del_symbols = stocks - latest_members
        for _stock in add_symbols:
            logging.info(f"{_stock} 纳入指数 {symbol} 成分，添加")
            addition_rows.append(
                {
                    "tradedate": datetime(dt.year, dt.month, dt.day),
                    "stock_code": _stock,
                    "op": 1,
                    "index_code": symbol,
                }
            )
        for _stock in del_symbols:
            logging.info(f"{_stock} 剔除指数 {symbol} 成分股，删除")
            addition_rows.append(
                {
                    "tradedate": datetime(dt.year, dt.month, dt.day),
                    "stock_code": _stock,
                    "op": 0,
                    "index_code": symbol,
                }
            )
    return addition_rows


def read_concepts_bars(ths_daily_bars_path):
    logging.info("全量读取同花顺概念日线")
    for csvfile in ths_daily_bars_path.iterdir():
        if csvfile.suffix != ".csv":
            continue
        df: pd.DataFrame = pd.read_csv(csvfile, index_col=0)
        df = df.iloc[::-1]
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
        df = df.drop(columns=["pre_close", "ts_code"])
        df = df.rename(columns={"trade_date": "date", "vol": "volume"})
        df = df.set_index("date", drop=True)
        df = df.fillna(0)
        dtypes = {
            "open": "float32",
            "high": "float32",
            "low": "float32",
            "close": "float32",
            "avg_price": "float32",
            "change": "float32",
            "pct_change": "float32",
            "volume": "int32",
            "turnover_rate": "float32",
        }
        df = df.astype(dtypes)
        # print(df)
        # print(df.info())
        yield df


def addition_read_concepts_bars(csv, concepts_basic_df):
    logging.info("增量读取同花顺概念日线")
    if not csv.exists():
        logging.error(f"同花顺概念日线没有 {csv} 数据")
        return
    df: pd.DataFrame = pd.read_csv(csv, index_col=0)
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df = df.reset_index(drop=True)
    df = df.rename(
        columns={"trade_date": "dt", "vol": "volume", "ts_code": "tablename"}
    )
    df = df.fillna(0)
    df = df.astype({"volume": "int64"})
    # tags_lst = list(zip(df['ts_code'], periodname))
    df = df.drop(columns=["pre_close"])
    df = df.loc[df["tablename"].isin(concepts_basic_df["ts_code"])]
    return df
