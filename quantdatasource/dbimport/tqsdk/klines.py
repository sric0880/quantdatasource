"""
暂时只用于K线行情的补全
天勤的K线最多只能下载8000条
天勤的K线没有amount列
"""

import logging
import re
from datetime import *

import pandas as pd
import pytz


def _convertTradingTime1(timestamp, seconds, dest_tz):
    dt = datetime.fromtimestamp(timestamp / 1e9) + timedelta(seconds=seconds)
    tz = pytz.timezone(dest_tz)
    return tz.localize(dt)


def _convertColumns(klines: pd.DataFrame):
    klines = klines.drop(
        ["id", "open_oi", "symbol", "duration"], axis=1, errors="ignore"
    )
    klines = klines.dropna()
    klines = klines.reset_index(drop=True)
    klines = klines.rename(columns={"close_oi": "open_interest"})
    klines = klines.drop_duplicates(subset=["datetime"], keep="first")
    return klines


def _commonConvert(klines, seconds, dest_tz):
    klines = _convertColumns(klines)
    klines["datetime"] = klines["datetime"].apply(
        _convertTradingTime1, args=(seconds, dest_tz)
    )
    return klines


product_types = {
    "T": 2,
    "TS": 2,
    "TF": 2,
    "TL": 2,
    "IC": 3,
    "IF": 3,
    "IH": 3,
    "IM": 3,
    "stock_cn": 3,
}


def _convertIntervalFromStr(intervalStr: str):
    ret = re.match(r"([\d]+)([smHDWMY])", intervalStr)
    if ret:
        interval = ret.group(1)
        intervalType = ret.group(2)
        return int(interval), intervalType
    else:
        logging.error("k线间隔时间格式不正确")
        return 0, None


def _symbolToProductId(symbol):
    ret = re.match(r"([a-zA-Z]{1,2})([\d]{3,4})", symbol)  # 国内期货
    if ret:
        return ret.group(1)
    return None


def _ctpHourTimes(interval_str, product_type, timeperiods):
    interval, intervalType = _convertIntervalFromStr(interval_str)
    if product_type == 1:
        if interval == 30 and intervalType == "m":
            return (
                0,
                1800,
                3600,
                5400,
                7200,
                9000,
                34200,
                36000,
                38700,
                40500,
                49500,
                51300,
                53100,
                54000,
                77400,
                79200,
                81000,
                82800,
                84600,
            )
        if len(timeperiods) == 3:  # 无夜盘品种
            if interval == 1:
                return (32400, 36000, 40500, 51300, 54000)  # 10:00 11:15 14:15 15:00
            elif interval == 2:
                return (32400, 40500, 54000)  # 11:15 15:00
            elif interval == 3:
                return (32400, 51300, 54000)  # 14:15 15:00
            elif interval == 4:
                return (32400, 54000)  # 15:00
        elif timeperiods[-1][1] == 82800:  # 23:00
            if interval == 1:
                return (
                    32400,
                    36000,
                    40500,
                    51300,
                    54000,
                    79200,
                    82800,
                )  # 10:00 11:15 14:15 15:00 22:00 23:00
            elif interval == 2:
                return (32400, 40500, 54000, 82800)  # 11:15 15:00 23:00
            elif interval == 3:
                return (32400, 36000, 54000)  # 10:00 15:00 (21:00-10:00可能跨天)
            elif interval == 4:
                return (
                    32400,
                    40500,
                    54000,
                    126900,
                )  # 11:15 15:00 (21:00-11:15可能跨天)
        elif timeperiods[-1][1] == 3600:  # 01:00
            if interval == 1:
                return (
                    0,
                    3600,
                    36000,
                    40500,
                    51300,
                    54000,
                    79200,
                    82800,
                )  # 10:00 11:15 14:15 15:00 22:00 23:00, 00:00 01:00
            elif interval == 2:
                return (3600, 40500, 54000, 82800)  # 11:15 15:00 23:00 01:00
            elif interval == 3:
                return (0, 40500, 54000)  # 11:15 15:00 00:00 (00:00-11:15可能跨天)
            elif interval == 4:
                return (-1, 3600, 54000, 90000)  # 01:00 15:00
        elif timeperiods[-1][1] == 9000:  # 02:30
            if interval == 1:
                return (
                    0,
                    3600,
                    7200,
                    34200,
                    38700,
                    49500,
                    53100,
                    54000,
                    79200,
                    82800,
                )  # 09:30 10:45 13:45 14:45 15:00 22:00 23:00, 00:00 01:00 02:00 (02:00-09:30可能跨天)
            elif interval == 2:
                return (
                    3600,
                    34200,
                    49500,
                    54000,
                    82800,
                )  # 09:30 13:45 15:00 23:00 01:00 (01:00-09:30可能跨天)
            elif interval == 3:
                return (
                    0,
                    34200,
                    53100,
                    54000,
                )  # 09:30 14:45 15:00 00:00 (00:00-09:30可能跨天)
            elif interval == 4:
                return (
                    -1,
                    3600,
                    49500,
                    54000,
                    90000,
                )  # 01:00 13:45 15:00 (01:00-13:45可能跨天)
    elif product_type == 3:
        if interval == 1 and intervalType == "H":
            return (34200, 37800, 41400, 50400, 54000)
        elif interval == 2 and intervalType == "H":
            return (34200, 41400, 54000)
        elif interval == 30 and intervalType == "m":
            return (34200, 36000, 37800, 39600, 41400, 48600, 50400, 52200, 54000)
    elif product_type == 2:
        if interval == 1 and intervalType == "H":
            return (34200, 37800, 41400, 50400, 54000, 54900)
        elif interval == 2 and intervalType == "H":
            return (34200, 41400, 54000, 54900)
        elif interval == 30 and intervalType == "m":
            return (
                34200,
                36000,
                37800,
                39600,
                41400,
                48600,
                50400,
                52200,
                54000,
                54900,
            )
    return None


def _aggKlines(klines, interval, product_type, timeperiods):
    times = _ctpHourTimes(interval, product_type, timeperiods)
    if not times:
        return None
    klines["close_bar"] = klines["datetime"].apply(
        lambda dt: dt if (dt.hour * 3600 + dt.minute * 60) in times else pd.NaT
    )
    klines["close_bar"] = klines["close_bar"].fillna(method="bfill")
    _klines = []
    for dt, bars in klines.groupby(by="close_bar"):
        open, high, low, close = (
            bars["open"].iloc[0],
            bars["high"].max(),
            bars["low"].min(),
            bars["close"].iloc[-1],
        )
        vol, oi = bars["volume"].sum(), bars["open_interest"].iloc[-1]
        _klines.append((dt, open, high, low, close, vol, oi))
    klines = pd.DataFrame(
        _klines,
        columns=["datetime", "open", "high", "low", "close", "volume", "open_interest"],
    )
    return klines


seconds_to_interval = {
    60: "1m",
    180: "3m",
    300: "5m",
    600: "10m",
    900: "15m",
}


def _save_db(df):
    if df is None:
        return
    df = df.rename(columns={"datetime": "dt"})
    df["amount"] = 0
    df = df[["dt", "open", "high", "low", "close", "volume", "amount", "open_interest"]]
    df = df.astype(
        {
            "open": "float32",
            "high": "float32",
            "low": "float32",
            "close": "float32",
            "volume": "int32",
            "open_interest": "int32",
        }
    )
    # print(df)
    # print(df.info())
    return df


exchange_map = {
    "CZCE": "ZCE",  # 郑州商品交易所
    "SHFE": "SHF",  # 上海期货交易所
    "DCE": "DCE",  # 大连商品交易所
    "CFFEX": "CFX",  # 中国金融期货交易所
    "INE": "INE",  # 上海国际能源交易所
    "GFEX": "GFE",  # 广州期货交易所
}


def read_klines(bars_history_path, bars_current_path):
    logging.info(f"读取期货K线")
    # TODO: init market times
    market_times = {}
    tz = "Asia/Shanghai"
    for basepath, is_history in [(bars_history_path, True), (bars_current_path, False)]:
        for csv in basepath.iterdir():
            if csv.suffix != ".csv":
                continue
            ret = re.match(r"(\w+).(\w+)_(\d+).csv", csv)
            exchange, symbol, seconds = ret.group(1), ret.group(2), int(ret.group(3))
            exchange = exchange_map.get(exchange, "")
            product_id = _symbolToProductId(symbol)
            product_type = product_types.get(product_id, 1)
            itv = seconds_to_interval[seconds]
            timeperiods = market_times[product_id.upper()]
            # if symbol != 'ag2305':
            #     continue
            logging.info(
                (exchange, symbol, seconds, product_id, product_type, itv, timeperiods)
            )
            df = pd.read_csv(csv)
            if df.empty:
                logging.error(f"期货K线 {csv} 数据为空")
                continue
            df_ = _commonConvert(df, seconds, tz)
            yield _save_db(df_), symbol, itv, exchange, is_history
            if seconds == 900:
                # 特殊规则 按15分钟K线生成30m, 1H, 2H, 3H, 4H
                df_30m = _aggKlines(df_, "30m", product_type, timeperiods)
                df_1H = _aggKlines(df_, "1H", product_type, timeperiods)
                df_2H = _aggKlines(df_, "2H", product_type, timeperiods)
                df_3H = _aggKlines(df_, "3H", product_type, timeperiods)
                df_4H = _aggKlines(df_, "4H", product_type, timeperiods)
                yield _save_db(df_30m), symbol, "30m", exchange, is_history
                yield _save_db(df_1H), symbol, "1H", exchange, is_history
                yield _save_db(df_2H), symbol, "2H", exchange, is_history
                yield _save_db(df_3H), symbol, "3H", exchange, is_history
                yield _save_db(df_4H), symbol, "4H", exchange, is_history
        #     break
        # break
