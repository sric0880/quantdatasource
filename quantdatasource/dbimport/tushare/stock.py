import logging

import numpy as np
import pandas as pd

from quantdatasource.dbimport.tushare.stock_utils import maxupordown_status

intervals = ["1D", "w", "mon"]


def read_basic(basic_stock_path):
    logging.info("读取证券基本信息")
    df = pd.read_csv(
        basic_stock_path, dtype={"list_date": str, "delist_date": str}, index_col=0
    )
    df = df.drop(columns=["symbol"])
    df = df.rename(columns={"ts_code": "symbol", "list_status": "status"})
    df = df.astype({"symbol": "string", "name": "string", "status": "string"})
    df = df.fillna("")
    return df


def addition_read_stock_daily_bars(
    dt,
    daily_bars_addition_path,
    daily_basic_addition_path,
    moneyflow_addition_path,
    chinese_names,
):
    today = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    daily_df = pd.read_csv(daily_bars_addition_path, index_col=0)
    daily_basic_df = pd.read_csv(daily_basic_addition_path, index_col=0)
    moneyflow_df = pd.read_csv(moneyflow_addition_path, index_col=0)
    df = pd.merge(daily_df, daily_basic_df, how="left", on=["ts_code", "trade_date"])
    df = pd.merge(df, moneyflow_df, how="left", on=["ts_code", "trade_date"])
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    all_datas = []
    for row in df.itertuples():
        symbol = row.ts_code
        if row.open == 0 or row.high == 0 or row.low == 0 or row.close == 0:
            logging.warning(f"可能是新股：{symbol} ohlc==0")
            continue
        # if symbol != '001366.SZ':
        #     continue
        pe_ttm = row.pe_ttm
        mkt_cap = row.total_mv * 10000
        if np.isnan(mkt_cap) or mkt_cap == 0:
            logging.error(f"{symbol} stock_daily.csv 下载中的市值数据为空，跳过")
            continue

        stockname = chinese_names.get(symbol, "")
        if not stockname:
            logging.error(
                f"新股：{symbol} 在stock_basic中不存在，但是已经有日线了，需要手动更新股名"
            )
        new_kline = {
            "symbol": symbol,
            "dt": today,
            "name": stockname,
            "open": row.open,
            "high": row.high,
            "low": row.low,
            "close": row.close,
            "preclose": row.pre_close,
            "volume": row.vol * 100,
            "amount": row.amount * 1000,
            "pe_ttm": pe_ttm,
            "pb": row.pb,
            "mkt_cap": mkt_cap,
            "mkt_cap_ashare": row.circ_mv * 10000,
            "vip_buy_amt": row.buy_lg_amount,
            "vip_sell_amt": row.sell_lg_amount,
            "inst_buy_amt": row.buy_elg_amount,
            "inst_sell_amt": row.sell_elg_amount,
            "mid_buy_amt": row.buy_md_amount,
            "mid_sell_amt": row.sell_md_amount,
            "indi_buy_amt": row.buy_sm_amount,
            "indi_sell_amt": row.sell_sm_amount,
            "turnover": row.turnover_rate / 100,
            "free_shares": row.float_share * 10000,
            "total_shares": row.total_share * 10000,
            "maxupordown": 0,
        }
        maxupordown = row.limit_status
        if pd.isna(maxupordown):
            maxupordown = maxupordown_status(symbol, row.close, new_kline)
        if row.high == row.low:
            maxupordown = 2 * maxupordown
        new_kline["maxupordown"] = maxupordown
        new_kline["vip_net_flow_in"] = (
            new_kline["vip_buy_amt"] - new_kline["vip_sell_amt"]
        )
        new_kline["inst_net_flow_in"] = (
            new_kline["inst_buy_amt"] - new_kline["inst_sell_amt"]
        )
        new_kline["mid_net_flow_in"] = (
            new_kline["mid_buy_amt"] - new_kline["mid_sell_amt"]
        )
        new_kline["indi_net_flow_in"] = (
            new_kline["indi_buy_amt"] - new_kline["indi_sell_amt"]
        )
        new_kline["master2_net_flow_in"] = (
            new_kline["mid_net_flow_in"]
            + new_kline["vip_net_flow_in"]
            + new_kline["inst_net_flow_in"]
        )
        new_kline["master_net_flow_in"] = (
            new_kline["vip_net_flow_in"] + new_kline["inst_net_flow_in"]
        )
        new_kline["total_sell_amt"] = (
            new_kline["mid_sell_amt"]
            + new_kline["indi_sell_amt"]
            + new_kline["vip_sell_amt"]
            + new_kline["inst_sell_amt"]
        )
        new_kline["total_buy_amt"] = (
            new_kline["mid_buy_amt"]
            + new_kline["indi_buy_amt"]
            + new_kline["vip_buy_amt"]
            + new_kline["inst_buy_amt"]
        )
        new_kline["net_flow_in"] = (
            new_kline["total_buy_amt"] - new_kline["total_sell_amt"]
        )
        new_kline["maxupordown_at_open"] = maxupordown_status(
            symbol, row.open, new_kline
        )

        all_datas.append(new_kline)

    df = pd.DataFrame(all_datas)
    df = df.astype(
        {
            "dt": "datetime64[ms]",
            "name": "string",
            "open": "float32",
            "high": "float32",
            "low": "float32",
            "close": "float32",
            "volume": "uint32",
            "amount": "uint64",
            "preclose": "float32",
            "pe_ttm": "float32",
            "pb": "float32",
            "mkt_cap": "float64",
            "mkt_cap_ashare": "float64",
            "vip_buy_amt": "float32",
            "vip_sell_amt": "float32",
            "inst_buy_amt": "float32",
            "inst_sell_amt": "float32",
            "mid_buy_amt": "float32",
            "mid_sell_amt": "float32",
            "indi_buy_amt": "float32",
            "indi_sell_amt": "float32",
            "master_net_flow_in": "float32",
            "master2_net_flow_in": "float32",
            "vip_net_flow_in": "float32",
            "mid_net_flow_in": "float32",
            "inst_net_flow_in": "float32",
            "indi_net_flow_in": "float32",
            "total_sell_amt": "float32",
            "total_buy_amt": "float32",
            "net_flow_in": "float32",
            "turnover": "float32",
            "free_shares": "uint64",
            "total_shares": "uint64",
            "maxupordown": "int8",
            "maxupordown_at_open": "int8",
        }
    )
    return df
