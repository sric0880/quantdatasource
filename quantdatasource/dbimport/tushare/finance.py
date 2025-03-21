# 三大报表数据
import logging
from datetime import timedelta
from pathlib import Path

import pandas as pd


def _drop_duplicates_of_finance_data(df: pd.DataFrame):
    # BUG: 有些财报的f_ann_date有问题，比如000428.SZ的资产负债表，20091231的年报，是20110225发布的，被当作废弃处理了
    # 同一天发布的多次财报，取最新的(update_flag==1)
    updated_df = df.loc[
        (df.duplicated(subset=["ts_code", "f_ann_date", "end_date"], keep=False))
        & (df["update_flag"] == 1)
    ]
    df = df.drop_duplicates(subset=["ts_code", "f_ann_date", "end_date"], keep=False)
    df = pd.concat([df, updated_df])
    df = df.sort_values(by="f_ann_date", ascending=False)
    # 同一天发布的多次财报，可能有多个update_flag==1，那么就取第一个
    df = df.drop_duplicates(subset=["ts_code", "f_ann_date", "end_date"], keep="first")
    # 按实际发布日期排序，如果财报报告期不是递增，那么删除不递增的财报
    df = df.sort_values(by=["f_ann_date", "end_date"])
    df["last_end_date"] = df["end_date"].shift(1, fill_value=df["end_date"].iloc[0])
    df = df.loc[df["end_date"] >= df["last_end_date"]]
    df = df.reset_index(drop=True)
    df = df.drop(columns=["update_flag", "last_end_date"])
    return df


def _process_finance_data(df):
    df["ann_date"] = pd.to_datetime(df["ann_date"], format="%Y%m%d")
    df["f_ann_date"] = pd.to_datetime(df["f_ann_date"], format="%Y%m%d")
    df["end_date"] = pd.to_datetime(df["end_date"], format="%Y%m%d")
    df = df.drop(columns=["end_type", "report_type"])
    dtypes = {
        "ts_code": "string",
        "comp_type": "int8",
        # 'report_type': 'int8',
        # 'end_type': 'int8',
        "update_flag": "int8",
    }
    df = df.astype(dtypes)
    # ？？财报有重复的情况，保留update_flag==1的财报
    df = _drop_duplicates_of_finance_data(df)
    return df


def _to_finance_data_q(df: pd.DataFrame):
    # 将报告期转换为单季度报表
    df["year"] = df["end_date"].dt.year
    res_df = []
    non_data_cols = [
        "ts_code",
        "ann_date",
        "f_ann_date",
        "end_date",
        "comp_type",
        "year",
    ]
    for _, _df in df.groupby(by=["year"]):
        for col in _df.columns:
            if col in non_data_cols:
                continue
            _df[col] = _df[col] - _df[col].shift(1).fillna(0)
        res_df.append(_df)
    if res_df:
        df = pd.concat(res_df)
        df = df.sort_values(by="end_date", ascending=False).reset_index(drop=True)
        df = df.drop(columns=["year"])
    return df


def read_finance_data(finance_path, report_type):
    # 导入财报报表
    for csvfile in finance_path.iterdir():
        if csvfile.is_file():
            symbol = csvfile.name.replace(".csv", "")
            df = pd.read_csv(csvfile, index_col=0)
            if df.empty:
                logging.info(f"{report_type} {symbol} 为空")
                continue
            logging.info(f"读取财报 {report_type} {symbol}")
            df = _process_finance_data(df)
            df_q = None
            if report_type != "balancesheet":
                df_q = _to_finance_data_q(df)
            yield df, df_q, symbol


def addition_read_finance_data(dt, addition_finance_path):
    logging.info("增量读取财报数据")
    enddate = dt.strftime("%Y%m%d")
    trade_yesterday = (dt - timedelta(days=1)).strftime("%Y%m%d")
    for tradedt in [trade_yesterday, enddate]:
        addition_file = Path(addition_finance_path, f"{tradedt}.csv")
        if not addition_file.exists():
            logging.error(f"  {addition_file} 不存在")
            continue
        fdf: pd.DataFrame = pd.read_csv(addition_file, index_col=0)
        if fdf.empty:
            logging.info(f"  财报 {addition_file} 为空")
            continue
        fdf = _process_finance_data(fdf)
        for row in fdf.itertuples():
            symbol = row.ts_code
            if symbol.startswith("A"):  # 还未上市
                continue
            row_dict = row._asdict()
            row_dict.pop("Index")
            # print(symbol)
            # print(row_dict)
            yield row_dict, symbol
