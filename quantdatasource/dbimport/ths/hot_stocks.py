import logging
from datetime import datetime

import pandas as pd


def addition_read_hot_stocks(dt, hot_stocks_addition_path):
    logging.info(f"增量读取同花顺热股")
    if not hot_stocks_addition_path.exists():
        logging.error(f"{hot_stocks_addition_path} 不存在")
        return
    df = pd.read_csv(
        hot_stocks_addition_path,
        dtype={"code": "string"},
        encoding="utf-8",
        index_col=0,
    )
    df["code"] = df["code"] + df["market"].map({33: ".SZ", 17: ".SH"})
    df = df.drop(columns=["analyse", "analyse_title", "market"], errors="ignore")
    # print(df.columns)
    df["date"] = datetime(dt.year, dt.month, dt.day)
    df = df.astype(
        {
            "code": "string",
            "name": "string",
            "order": "int8",
            "rate": "int32",
            "hot_rank_chg": "int8",
            "concept_tag": "string",
            "popularity_tag": "string",
        }
    )
    df = df.fillna("")
    return df
