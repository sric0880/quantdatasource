"""
龙虎榜
"""

import logging

import pandas as pd


def addition_read_lhb(lhb_path, lhb_inst_path):
    logging.info("读取龙虎榜")
    df: pd.DataFrame = pd.read_csv(lhb_path)
    df = df.rename(columns={"ts_code": "symbol"})
    many_reason_df = df.loc[df.duplicated(subset=["symbol"], keep=False)]
    df = df.drop_duplicates(subset=["symbol"], keep="last")
    data = df.to_dict(orient="records")
    data_dct = {d["symbol"]: d for d in data}
    for symbol, sub_mrd in many_reason_df.groupby(by=["symbol"]):
        data_dct[symbol]["reason"] = sub_mrd["reason"].to_list()
    if lhb_inst_path.exists():
        inst_df = pd.read_csv(lhb_inst_path)
        inst_df = inst_df.fillna(0)
        for symbol, sub_inst_df in inst_df.groupby(by=["ts_code"]):
            _buy_inst_df = sub_inst_df.loc[sub_inst_df["side"] == 0]  # 买入
            _buy_inst_df = _buy_inst_df.drop_duplicates(
                subset=["exalter"], keep="first"
            )
            _sell_inst_df = sub_inst_df.loc[sub_inst_df["side"] == 1]  # 卖出
            _sell_inst_df = _sell_inst_df.drop_duplicates(
                subset=["exalter"], keep="first"
            )
            _buy_inst_df = _buy_inst_df.rename(
                columns={"sell": "sell_amount", "buy": "buy_amount"}
            )
            _sell_inst_df = _sell_inst_df.rename(
                columns={"sell": "sell_amount", "buy": "buy_amount"}
            )
            data_dct[symbol]["buy_top5_inst"] = _buy_inst_df[
                [
                    "exalter",
                    "buy_amount",
                    "sell_amount",
                    "buy_rate",
                    "sell_rate",
                    "net_buy",
                ]
            ].to_dict("records")
            data_dct[symbol]["sell_top5_inst"] = _sell_inst_df[
                [
                    "exalter",
                    "buy_amount",
                    "sell_amount",
                    "buy_rate",
                    "sell_rate",
                    "net_buy",
                ]
            ].to_dict("records")
    return data
