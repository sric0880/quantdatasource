import json
import logging
import os
import shutil
from ast import literal_eval
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqsdk import TqApi, TqAuth
from tqsdk.tools import DataDownloader

from quantdatasource.api.tqsdk_utils import *

from .utils import log


class TQSDKApi:
    def __init__(self, username, psw, output, trade_date):
        self.output = Path(output, "tqsdk")
        self.dt = trade_date
        trade_date = trade_date.strftime("%Y-%m-%d")
        self.trade_date = trade_date

        self.api = TqApi(auth=TqAuth(username, psw))
        self.future_basic_path = Path(self.output, "future_basic.csv")
        self.product_basic_path = Path(self.output, "product_basic.csv")
        self.cont_history_path = Path(self.output, "cont_history.csv")
        self.cont_list_path = Path(self.output, "cont_list.json")
        self.stock_list_path = Path(self.output, f"stock_list.json")
        self.bars_current_path = Path(self.output, "klines", "current")
        self.bars_history_path = Path(self.output, "klines", "history")
        self.adjust_factors_path = Path(self.output, "adjust_factors")
        self.adjust_factors_filepath = Path(self.output, "adjust_factors.csv")
        self.ticks_path = Path(self.output, "ticks")

        self.bars_current_path.mkdir(parents=True, exist_ok=True)
        self.bars_history_path.mkdir(parents=True, exist_ok=True)
        self.adjust_factors_path.mkdir(parents=True, exist_ok=True)
        self.ticks_path.mkdir(parents=True, exist_ok=True)

    def close(self):
        self.api.close()

    @log
    def full_download_future_basic(self):
        """
        全量下载上市期货合约信息
        """
        quotes = self.api.query_quotes(ins_class="FUTURE", expired=False)
        # TODO: query_quotes 这个接口突然莫名其妙返回一堆外盘主力合约，需要将它过滤掉 2023/11/24
        quotes = [
            q
            for q in quotes
            if q.startswith("CFFEX")
            or q.startswith("SHFE")
            or q.startswith("DCE")
            or q.startswith("CZCE")
            or q.startswith("INE")
            or q.startswith("GFEX")
        ]
        df = self.api.query_symbol_info(quotes)
        df.to_csv(self.future_basic_path, index=False)
        logging.info("  期货基本资料下载完成")

        # 生成product_id -> exchange id 的映射
        exchanges = {}
        for row in df.itertuples():
            exchanges[row.product_id] = row.exchange_id
        logging.info("  品种对应交易所生成完成")

        names = {}
        for row in df.itertuples():
            names[row.product_id] = row.instrument_name[:-4]
        logging.info("  品种中文名称生成完成")

        # 每个品种选3个最大持仓量合约
        # TODO: 广州期货交易所的硅需要购买专业版才能查询
        quote_list = self.api.get_quote_list(
            [s for s in quotes if not s.startswith("GFEX")]
        )
        symbol_info = defaultdict(list)
        cont_symbols = {}
        for q in quote_list:
            symbol = q.instrument_id[q.instrument_id.rindex(".") + 1 :]
            product_id = q.product_id
            symbol_info[product_id].append(
                {"symbol": symbol, "volume": q.volume, "oi": q.open_interest}
            )
        for product_id, lst in symbol_info.items():
            sorted_lst = sorted(lst, key=lambda x: x["oi"], reverse=True)
            if len(sorted_lst) > 3:
                sorted_lst = sorted_lst[:3]
            cont_symbols[product_id] = [q["symbol"] for q in sorted_lst if q["oi"] > 10]
        logging.info("  主力合约选举完成")

        df = pd.concat(
            [
                pd.Series(exchanges, name="exchange"),
                pd.Series(names, name="name"),
                pd.Series(cont_symbols, name="cont_symbols"),
            ],
            axis=1,
        )
        df.to_csv(self.product_basic_path)

    @log
    def full_download_future_cont_history(self, symbols=None):
        """全量下载主力连续合约对应的历史合约"""
        if not symbols:
            with open(self.cont_list_path, "r") as f:
                symbols = json.load(f)
        klines = self.api.query_his_cont_quotes(symbols, n=2200)
        klines.to_csv(self.cont_history_path, index=False)

    @log
    def full_download_future_cont_list(self):
        """全量下载主力连续合约列表"""
        contlist = self.api.query_quotes(ins_class="CONT")
        with open(self.cont_list_path, "w") as f:
            json.dump(contlist, f)

    @log
    def full_download_stock_list(self):
        """全量下载股票列表"""
        stocklist = self.api.query_quotes(ins_class="STOCK")
        # 排除B股
        stocklist = [
            symbol
            for symbol in stocklist
            if (
                not symbol.startswith("SSE.900")
                and not symbol.startswith("SZSE.200")
                and not symbol.startswith("SZSE.201")
            )
        ]
        # print(stocklist)
        logging.info(f"  一共{len(stocklist)}只股票")
        with open(self.stock_list_path, "w") as f:
            json.dump(stocklist, f, indent=4)

    @log
    def download_bars(
        self, symbol, interval, data_length=8000, force_replace=False, is_history=True
    ):
        path = self.bars_history_path if is_history else self.bars_current_path
        pathname = Path(path, f"{symbol}_{interval}.csv")
        if not force_replace and os.path.exists(pathname):
            # logging.info(f"{pathname} already exists, skip download.")
            return

        logging.info(f"---------{symbol} {interval} start -----------")
        klines = self.api.get_kline_serial(
            symbol, data_length=data_length, duration_seconds=interval
        )
        klines.to_csv(pathname, index=False)
        logging.info(f"---------{symbol} {interval} over -----------")

    @log
    def full_download_bars(self):
        """
        全量下载主力合约K线数据，用于行情数据的补全（除了日线之外的分钟线、小时线）
        """
        # 先下载所有历史合约，再下载当前合约
        # 3H/4H 合约需要上一个合约拼接
        # 由于天勤的日线最早只能下到16年，所以日线由tushare接口下载
        if self.bars_current_path.exists():
            shutil.rmtree(self.bars_current_path, ignore_errors=True)
        os.makedirs(self.bars_current_path)
        quotes = self.api.query_quotes(ins_class="FUTURE", expired=True)
        valid_quotes = [q for q in quotes if q not in cannot_download_symbols]
        logging.info(
            f"  总共有{len(quotes)}只历史合约, 排除掉下载阻塞的标的，剩下{len(valid_quotes)}"
        )

        for q in valid_quotes:
            # 已经下载的不再下载
            self.download_bars(q, 900)  # 15m

        removed_ = []
        for basepath in [self.bars_history_path]:
            # for basepath in [self.bars_history_path, self.bars_current_path]:
            for csv in os.listdir(basepath):
                if not csv.endswith(".csv"):
                    continue
                df = pd.read_csv(os.path.join(basepath, csv))
                if df.empty:
                    logging.error(f"  删除 {csv} : 数据为空")
                    removed_.append(os.path.join(basepath, csv))
                    continue
                df = df.loc[(df["id"] >= 0) & (df["close"] == 0)]
                if len(df) > 0:
                    logging.error(f"  删除 {csv} : 合法数据中收盘价为0")
                    removed_.append(os.path.join(basepath, csv))
                    continue
        for csv in removed_:
            os.remove(csv)

        if not self.product_basic_path.exists():
            logging.error(
                f"  {self.product_basic_path}不存在，必须先调用 download_future_basic"
            )
            return

        df = pd.read_csv(self.product_basic_path, index_col=0)
        all_symbols = []
        intervals = [
            (60, 1000),  # 1m
            (180, 1000),  # 3m
            (300, 1000),  # 5m
            (600, 1000),  # 10m
            (900, 8000),  # 15m
        ]
        # not_commodity_products = ['T', 'TS', 'TF', 'TL', 'IC', 'IF', 'IH', 'IM']
        df["cont_symbols"] = df["cont_symbols"].apply(
            lambda x: literal_eval(x) if not pd.isna(x) else []
        )
        for row in df.itertuples():
            product_id = row.Index
            exchange = row.exchange
            cont_symbols = row.cont_symbols
            for symbol in cont_symbols:
                for sec, length in intervals:
                    all_symbols.append((symbol, product_id, exchange, sec, length))

        for symbol, product_id, exchange, sec, length in all_symbols:
            self.download_bars(
                f"{exchange}.{symbol}",
                sec,
                data_length=length,
                force_replace=True,
                is_history=False,
            )

    @log
    def cal_cont_future_adjust_factors(self, force_replace=False):
        """主力合约中计算出价差并保存"""
        logging.warning("依赖期货日线数据 查看期货日线是否最新，更新期货K线")
        if not self.cont_history_path.exists():
            logging.error(
                f"  {self.cont_history_path}不存在，必须先调用 full_download_future_cont_list"
            )
            return
        cont_history = pd.read_csv(self.cont_history_path)
        cont_history["date"] = pd.to_datetime(cont_history["date"], utc=True)
        # TODO: 下载的cont_history有错误，和实际ticks数据对不上，这里只是将发现的错误修正，没时间全部查看一遍
        # TODO：2018-06-19 ag 白银也有问题
        correct_cont_history(cont_history)
        # TODO: connect to database for fetching daily bars in get_close_price_diff
        for symbol in cont_history.columns[1:]:
            adjust_factors_file = Path(self.adjust_factors_path, f"{symbol}.csv")
            if not force_replace and adjust_factors_file.exists():
                continue
            print(symbol)
            df = cont_history[["date", symbol]]
            df = df.rename(columns={symbol: "symbol"})
            df = df.groupby(by=["symbol"], sort=False, as_index=False).first()
            df["symbol"] = df["symbol"].map(to_tushare_symbol)
            df["pre_symbol"] = df["symbol"].shift(1)
            df["diff"] = df.apply(
                lambda x: get_close_price_diff(x["symbol"], x["pre_symbol"], x["date"]),
                axis=1,
            )
            df["factor"] = df["diff"].cumsum()
            # print(df)
            df.to_csv(adjust_factors_file)

        # 第三步合并所有合约的价差到一个文件中
        df = pd.concat(
            [
                pd.read_csv(
                    Path(self.adjust_factors_path, f"{symbol}.csv"), index_col=0
                )
                for symbol in cont_history.columns[1:]
            ],
            ignore_index=True,
        )
        df.to_csv(self.adjust_factors_filepath, index=True)

    @log
    def full_download_ticks(
        self, symbols_or_file, start_month, end_month, zip, symbol_type="future"
    ):
        if not symbols_or_file:
            return
        if isinstance(symbols_or_file, str):
            with open(symbols_or_file, "r") as f:
                symbols = json.load(f)
        else:
            symbols = symbols_or_file

        all_tick_months = pd.date_range(start_month, end_month, freq="M")
        for symbol in symbols:
            folder_name = Path(self.ticks_path, symbol)
            if os.path.exists(folder_name + ".zip"):
                # unzip_file(folder_name+'.zip', dir = args.folder)
                continue
            if not folder_name.is_dir():
                folder_name.mkdir(exist_ok=True)
            curp = os.getcwd()
            os.chdir(folder_name)
            download_tasks = []
            download_files = []
            for _yearmonth in all_tick_months:
                year, month = _yearmonth.year, _yearmonth.month
                if symbol_type == "astock" and not stock_is_on_list(
                    symbol, year, month, self.output
                ):
                    continue
                _year, _month = next_month(year, month)
                task_name = f"{symbol}_{year}_{month:02d}"
                output_filename = f"{task_name}.csv"
                output_zip_filename = f"{task_name}.zip"
                download_files.append((output_filename, output_zip_filename))
                if not os.path.exists(output_filename) and not os.path.exists(
                    output_zip_filename
                ):
                    downloader = DataDownloader(
                        self.api,
                        symbol_list=[symbol],
                        dur_sec=0,
                        start_dt=datetime(year, month, 1, 0, 0, 0),
                        end_dt=datetime(_year, _month, 1, 0, 0, 0),
                        csv_file_name=output_filename,
                    )
                    download_tasks.append(
                        (
                            (symbol, year, month, output_filename),
                            downloader,
                        )
                    )
                    logging.info(f"开始下载: {output_filename}")

            while not all([v[1].is_finished() for v in download_tasks]):
                self.api.wait_update()

            for output_filename, output_zip_filename in download_files:
                if is_file_empty(output_filename):
                    os.remove(output_filename)
                    logging.error(f"下载数据为空：移除{output_filename}")
                if is_file_empty(output_zip_filename):
                    os.remove(output_zip_filename)
                    logging.error(f"下载数据为空：移除{output_zip_filename}")
                if zip:
                    if os.path.exists(output_filename) and not os.path.exists(
                        output_zip_filename
                    ):
                        zip_file(output_filename, output_zip_filename)
                        os.remove(output_filename)
            os.chdir(curp)
