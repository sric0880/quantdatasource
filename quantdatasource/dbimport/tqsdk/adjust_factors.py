import logging

import pandas as pd


def read_adjust_factors(adjust_factors_filepath, cal):
    logging.info("读取价差因子")
    adjust_df = pd.read_csv(adjust_factors_filepath, index_col=0)
    adjust_df["symbol"] = adjust_df["symbol"].map(lambda x: x[:-4])
    adjust_df = adjust_df.rename(
        columns={"date": "tradedate", "factor": "adjust_factor"}
    )
    adjust_df = adjust_df[["symbol", "tradedate", "adjust_factor"]]
    adjust_df["tradedate"] = pd.to_datetime(adjust_df["tradedate"])

    # TODO: test
    adjust_df["tradedate"] = (
        adjust_df["tradedate"]
        .map(
            lambda date: pd.Timestamp(
                cal.get_tradeday_last(date.to_pydatetime()) + 13 * 3600, freq="s"
            )
        )
        .tz_convert("Asia/Shanghai")
    )
    # 原始版本：
    # last_trade_days_index = (
    #     trade_cal_df["_id"].searchsorted(adjust_df["tradedate"], side="left") - 1
    # )
    # adjust_df["tradedate"] = adjust_df.index.map(
    #     lambda x: trade_cal_df["_id"]
    #     .iloc[last_trade_days_index[x]]
    #     .replace(hour=13)
    #     .tz_convert("Asia/Shanghai")
    # )
    # print(adjust_df)
    return adjust_df
