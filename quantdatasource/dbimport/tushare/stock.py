import logging
from collections import defaultdict

import duckdb as db
import numpy as np
import pandas as pd
from quantcalendar import timestamp_us
from quantdata import get_data_last_row

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
    mongo_finance_db,
):
    today = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    daily_df = pd.read_csv(daily_bars_addition_path, index_col=0)
    daily_basic_df = pd.read_csv(daily_basic_addition_path, index_col=0)
    moneyflow_df = pd.read_csv(moneyflow_addition_path, index_col=0)
    df = pd.merge(daily_df, daily_basic_df, how="left", on=["ts_code", "trade_date"])
    df = pd.merge(df, moneyflow_df, how="left", on=["ts_code", "trade_date"])
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    all_stock_number = 0
    count_of_uplimit = 0
    count_of_downlimit = 0
    count_of_yiziup = 0
    count_of_yizidown = 0
    _lb_counts = defaultdict(int)
    all_datas = []
    dr_symbols = []
    for row in df.itertuples():
        symbol = row.ts_code
        if row.open == 0 or row.high == 0 or row.low == 0 or row.close == 0:
            logging.warning(f"可能是新股：{symbol} ohlc==0")
            continue
        # if symbol != '001366.SZ':
        #     continue
        # 当企业亏损时，pe_ttm 和 pe都有可能为Nan，需要自己计算pe_ttm
        # BUG: 当pe_ttm为Nan时，自己计算的pe_ttm也有可能大于0，那是因为tushare会根据业绩快报来计算pe，自己只能通过正式发布才能算
        pe_ttm = row.pe_ttm
        mkt_cap = row.total_mv * 10000
        if np.isnan(mkt_cap) or mkt_cap == 0:
            logging.error(f"{symbol} stock_daily.csv 下载中的市值数据为空，跳过")
            continue
        with (
            mongo_finance_db["finance_income_q"]
            .find(
                {"$and": [{"ts_code": symbol}, {"ann_date": {"$lte": dt}}]},
                projection={"end_date": 1, "f_ann_date": 1, "n_income_attr_p": 1},
            )
            .sort([("end_date", -1), ("f_ann_date", -1)]) as income_docs
        ):
            _end_date = None
            n_income_attr_ps = []
            for income_doc in income_docs:
                if len(n_income_attr_ps) >= 4:
                    break
                current_end_date = income_doc["end_date"]
                if _end_date is not None and _end_date == current_end_date:
                    continue
                n_income_attr_ps.append(income_doc["n_income_attr_p"])
                _end_date = current_end_date
            if n_income_attr_ps:
                net_profit_q = n_income_attr_ps[0]
                net_profit_ttm = sum(n_income_attr_ps)
                pe_ttm = (mkt_cap / net_profit_ttm) if pd.isna(pe_ttm) else pe_ttm
            else:
                logging.warning(f"{symbol} 无单季利润表")
                net_profit_q = 0
                net_profit_ttm = 0

        with (
            mongo_finance_db["finance_balancesheet"]
            .find(
                {"$and": [{"ts_code": symbol}, {"ann_date": {"$lte": dt}}]},
                projection={
                    "end_date": 1,
                    "total_hldr_eqy_inc_min_int": 1,
                    "total_assets": 1,
                    "total_liab": 1,
                },
            )
            .sort([("end_date", -1)])
            .limit(1) as blc_docs
        ):
            try:
                blc_doc = blc_docs.next()
                equity = blc_doc.get("total_hldr_eqy_inc_min_int", 0)
                asset = blc_doc.get("total_assets", 0)
                debt = blc_doc.get("total_liab", 0)
                if equity == 0 or asset == 0 or debt == 0:
                    logging.error(
                        f"{symbol} 资产负债表错误 equity:{equity}, asset:{asset}, debt:{debt}"
                    )
                debttoasset = debt / asset if asset > 0 else 0
            except StopIteration:
                logging.warning(f"{symbol} 无资产负债表")
                equity = 0
                asset = 0
                debt = 0
                debttoasset = 0

        with (
            mongo_finance_db["finance_cashflow_q"]
            .find(
                {"$and": [{"ts_code": symbol}, {"ann_date": {"$lte": dt}}]},
                projection={"end_date": 1, "f_ann_date": 1, "n_cashflow_act": 1},
            )
            .sort([("end_date", -1), ("f_ann_date", -1)]) as cf_docs
        ):
            _end_date = None
            n_cashflow_act = []
            for cf_doc in cf_docs:
                if len(n_cashflow_act) >= 4:
                    break
                current_end_date = cf_doc["end_date"]
                if _end_date is not None and _end_date == current_end_date:
                    continue
                n_cashflow_act.append(cf_doc.get("n_cashflow_act", 0))
                _end_date = current_end_date
            cashflow_ttm = sum(n_cashflow_act)
            if cashflow_ttm == 0:
                logging.warning(f"{symbol} 无单季现金流量表")

        stockname = chinese_names.get(symbol, "")
        if not stockname:
            logging.error(
                f"新股：{symbol} 在stock_basic中不存在，但是已经有日线了，需要手动更新股名"
            )
        new_kline = {
            "tablename": symbol,
            "dt": today,
            "name": stockname,
            "_open": row.open,
            "_high": row.high,
            "_low": row.low,
            "_close": row.close,
            "preclose": row.pre_close,
            "volume": row.vol * 100,
            "amount": row.amount * 1000,
            "net_profit_ttm": net_profit_ttm,
            "cashflow_ttm": cashflow_ttm,
            "equity": equity,
            "asset": asset,
            "debt": debt,
            "debttoasset": debttoasset,
            "net_profit_q": net_profit_q,
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
            "lb_up_count": 0,
            "lb_down_count": 0,
            "close": row.close,
            "open": row.open,
            "high": row.high,
            "low": row.low,
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

        if new_kline["maxupordown"] > 0:
            new_kline["lb_up_count"] = 1
        elif new_kline["maxupordown"] < 0:
            new_kline["lb_down_count"] = 1
        try:
            last_row = get_data_last_row(
                "bars_stock_daily",
                f"_{symbol.replace('.', '_')}",
                fields=["dt", "_close", "lb_up_count", "lb_down_count"],
                till_microsec=timestamp_us(row.trade_date),
                side=None,
            ).fetchone()
            if last_row is not None:
                _, lr_close, lr_lb_up_count, lr_lb_down_count = last_row
                if new_kline["maxupordown"] > 0:
                    new_kline["lb_up_count"] = lr_lb_up_count + 1
                elif new_kline["maxupordown"] < 0:
                    new_kline["lb_down_count"] = lr_lb_down_count + 1
                if abs(lr_close - new_kline["preclose"]) >= 0.0001:
                    # 发生除权
                    dr_symbols.append(symbol)
        except db.CatalogException:
            logging.info(f"{symbol} 新股，没有历史连板数据，无法获知今日是否除权")

        all_datas.append(new_kline)

        if "ST" not in stockname and not "退" in stockname:
            all_stock_number += 1
            if maxupordown > 0:
                count_of_uplimit += 1
            elif maxupordown < 0:
                count_of_downlimit += 1
            if maxupordown == 2:
                count_of_yiziup += 1
            elif maxupordown == -2:
                count_of_yizidown += 1
            _lb_counts[new_kline["lb_up_count"]] += 1

    logging.info(f"  发生除权的股票有: {dr_symbols}")
    df = pd.DataFrame(all_datas)
    df = df.astype(
        {
            "name": "string",
            "_open": "float32",
            "_high": "float32",
            "_low": "float32",
            "_close": "float32",
            "volume": "uint32",
            "amount": "uint64",
            "preclose": "float32",
            "net_profit_ttm": "float32",
            "cashflow_ttm": "float32",
            "equity": "float32",
            "asset": "float32",
            "debt": "float32",
            "debttoasset": "float32",
            "net_profit_q": "float32",
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
            "maxupordown": "int",
            "maxupordown_at_open": "int",
            "lb_up_count": "int",
            "lb_down_count": "int",
            "close": "float32",
            "open": "float32",
            "high": "float32",
            "low": "float32",
        }
    )
    logging.info(
        f"今日涨停:{count_of_uplimit}, 跌停:{count_of_downlimit}, 一共{all_stock_number}只股"
    )
    ratio_of_uplimit = count_of_uplimit / all_stock_number
    ratio_of_downlimit = count_of_downlimit / all_stock_number
    ratio_of_yiziup = count_of_yiziup / all_stock_number
    ratio_of_yizidown = count_of_yizidown / all_stock_number

    market_stats = {
        "dt": today,
        "count_of_uplimit": count_of_uplimit,
        "count_of_downlimit": count_of_downlimit,
        "count_of_yiziup": count_of_yiziup,
        "count_of_yizidown": count_of_yizidown,
        "ratio_of_uplimit": ratio_of_uplimit,
        "ratio_of_downlimit": ratio_of_downlimit,
        "ratio_of_yiziup": ratio_of_yiziup,
        "ratio_of_yizidown": ratio_of_yizidown,
    }
    for i in range(1, 13):
        market_stats[f"lb{i}"] = _lb_counts.get(i, 0)

    return (df, dr_symbols, market_stats)
