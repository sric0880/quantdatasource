import logging
import os
import shutil
from datetime import timedelta
from pathlib import Path
from time import sleep, time

import pandas as pd
import tushare as ts
from quantcalendar import CalendarAstock, pydt_from_second, pydt_from_sec_list

from .utils import log


class TushareApi:
    dir = "tushare"

    def __init__(self, token, output, trade_date) -> None:
        self.api = ts.pro_api(token)
        self.output = os.path.join(output, self.dir)
        self.dt = trade_date
        trade_date = trade_date.strftime("%Y%m%d")
        self.trade_date = trade_date
        self.basic_stock_path = Path(output, "stock_basic.csv")
        self.basic_cb_path = Path(output, "cb_basic.csv")
        self.ths_index_a_concepts_path = Path(output, "ths_index_a_concepts.csv")
        self.finance_income_path = Path(output, "income")
        self.finance_balancesheet_path = Path(output, "balancesheet")
        self.finance_cashflow_path = Path(output, "cashflow")
        self.finance_income_addition_path = Path(self.finance_income_path, "additions")
        self.finance_balancesheet_addition_path = Path(
            self.finance_balancesheet_path, "additions"
        )
        self.finance_cashflow_addition_path = Path(
            self.finance_cashflow_path, "additions"
        )
        self.ths_daily_bars_path = Path(output, "ths_concepts")
        self.ths_daily_bars_addition_path = Path(
            output, "ths_concepts", "additions", f"{trade_date}.csv"
        )
        self.ths_concepts_members_path = Path(output, "ths_concepts_members")
        self.daily_bars_addition_path = Path(output, "daily", f"{trade_date}.csv")
        self.daily_basic_addition_path = Path(
            output, "daily_basic", f"{trade_date}.csv"
        )
        self.moneyflow_addition_path = Path(output, "moneyflow", f"{trade_date}.csv")
        self.lhb_addition_path = Path(output, "lhb", f"{trade_date}.csv")
        self.lhb_inst_addition_path = Path(output, "lhb_inst", f"{trade_date}.csv")
        self.cb_daily_bars_path = Path(output, "cb", "daily")
        self.cb_daily_bars_addition_path = Path(
            self.cb_daily_bars_path, "additions", f"{trade_date}.csv"
        )
        self.cb_share_path = Path(output, "cb", "share")
        self.cb_call_path = Path(output, "cb", "call")
        self.index_daily_addition_path = Path(
            output, "index", "daily", f"{trade_date}.csv"
        )
        self.index_week_addition_path = Path(
            output, "index", "week", f"{trade_date}.csv"
        )
        self.index_month_addition_path = Path(
            output, "index", "month", f"{trade_date}.csv"
        )
        self.future_daily_current_path = Path(output, "daily", "current")
        self.future_daily_history_path = Path(output, "daily", "history")

    @log
    def full_download_stock_basic(self):
        """
        全量下载证券基本信息
        """
        dfs = [
            self.api.stock_basic(
                list_status="L",
                fields=[
                    "ts_code",
                    "symbol",
                    "name",
                    "market",
                    "list_date",
                    "delist_date",
                    "list_status",
                    "exchange",
                    "industry",
                    "area",
                    "fullname",
                    "enname",
                    "cnspell",
                    "curr_type",
                    "is_hs",
                ],
            ),
            self.api.stock_basic(
                list_status="D",
                fields=[
                    "ts_code",
                    "symbol",
                    "name",
                    "market",
                    "list_date",
                    "delist_date",
                    "list_status",
                    "exchange",
                    "industry",
                    "area",
                    "fullname",
                    "enname",
                    "cnspell",
                    "curr_type",
                    "is_hs",
                ],
            ),
        ]
        df = pd.concat(dfs)
        df.to_csv(self.basic_stock_path)

    @log
    def full_download_cb_basic(self):
        """
        全量下载可转债基本信息
        """
        df = self.api.cb_basic()
        df.to_csv(self.basic_cb_path)

    @log
    def full_download_ths_index(self):
        """
        全量下载同花顺概念板块列表
        """
        df = self.api.ths_index(
            exchange="A", type="N", fields="ts_code,name,count,list_date"
        )
        df.to_csv(self.ths_index_a_concepts_path)

    @log
    def full_download_finance_data(self, force_replace=False):
        """
        全量下载所有财务报表
        """
        if not self.basic_stock_path.exists():
            logging.error(
                f"{self.basic_stock_path}不存在，必须先调用 full_download_stock_basic"
            )
            return
        df = pd.read_csv(self.basic_stock_path, index_col=0)
        for row in df.itertuples():
            symbol = row.ts_code
            path = Path(self.finance_income_path, f"{symbol}.csv")
            if not force_replace and path.exists():
                continue
            _df = self.api.income(ts_code=symbol)
            _df.to_csv(path)
            logging.info(f"  {symbol} 利润表下载完成")
        for row in df.itertuples():
            symbol = row.ts_code
            path = Path(self.finance_balancesheet_path, f"{symbol}.csv")
            if not force_replace and path.exists():
                continue
            _df = self.api.balancesheet(ts_code=symbol)
            _df.to_csv(path)
            logging.info(f"  {symbol} 资产负债表下载完成")
        for row in df.itertuples():
            symbol = row.ts_code
            path = Path(self.finance_cashflow_path, f"{symbol}.csv")
            if not force_replace and path.exists():
                continue
            _df = self.api.cashflow(ts_code=symbol)
            _df.to_csv(path)
            logging.info(f"  {symbol} 现金流量表下载完成")

    @log
    def addition_download_finance_data(self):
        trade_yesterday = (self.dt - timedelta(days=1)).strftime("%Y%m%d")
        # 财务数据(包括昨天的，可能昨天下载的数据，还没有更新好)
        for tradedt in [self.trade_date, trade_yesterday]:
            _df = self.api.income_vip(ann_date=tradedt)
            _df.to_csv(Path(self.finance_income_addition_path, f"{tradedt}.csv"))
            logging.info(f"tushare {tradedt} 利润表下载完成")
            _df = self.api.balancesheet_vip(ann_date=tradedt)
            _df.to_csv(Path(self.finance_balancesheet_addition_path, f"{tradedt}.csv"))
            logging.info(f"tushare {tradedt} 资产负债表下载完成")
            _df = self.api.cashflow_vip(ann_date=tradedt)
            _df.to_csv(Path(self.finance_cashflow_addition_path, f"{tradedt}.csv"))
            logging.info(f"tushare {tradedt} 现金流量表下载完成")

    @log
    def full_download_concepts_bars(self, force_replace=False):
        """
        全量下载同花顺概念板块日线数据(每分钟最多访问500次)
        """
        if not self.ths_index_a_concepts_path.exists():
            logging.error(
                f"{self.ths_index_a_concepts_path}不存在，必须先调用 full_download_ths_index"
            )
            return
        df = pd.read_csv(self.ths_index_a_concepts_path, index_col=0)
        for row in df.itertuples():
            t = time()
            symbol = row.ts_code
            path = Path(self.ths_daily_bars_path, f"{symbol}.csv")
            if not force_replace and path.exists():
                continue
            dfs = []
            dfs.append(self.api.ths_daily(ts_code=symbol))
            while len(dfs[-1]) == 3000:
                dfs.append(self.api.ths_daily(ts_code=symbol, offset=3000))
            df = pd.concat(dfs, ignore_index=True)
            df.to_csv(path)
            print(f"  {symbol} 下载完成")
            eclipse = time() - t
            wait_t = 0.13 - eclipse
            if wait_t > 0:
                sleep(wait_t)

    @log
    def addition_download_concepts_bars(self):
        """增量下载同花顺概念板块日线数据"""
        df = self.api.ths_daily(trade_date=self.trade_date)
        df.to_csv(self.ths_daily_bars_addition_path)

    @log
    def full_download_concepts_members(self, force_replace=False):
        """全量下载同花顺概念板块成分股"""
        if not self.ths_index_a_concepts_path.exists():
            logging.error(
                f"{self.ths_index_a_concepts_path}不存在，必须先调用 full_download_ths_index"
            )
            return
        df = pd.read_csv(self.ths_index_a_concepts_path, index_col=0)
        for row in df.itertuples():
            t = time()
            symbol = row.ts_code
            path = Path(self.ths_concepts_members_path, f"{symbol}.csv")
            if not force_replace and path.exists():
                continue
            df = self.api.ths_member(ts_code=symbol)
            df.to_csv(path)
            # print(f"{symbol}下载完成")
            eclipse = time() - t
            wait_t = 0.31 - eclipse
            if wait_t > 0:
                sleep(wait_t)

    @log
    def full_download_lhb(self):
        """全量下载龙虎榜数据(接口限流每分钟500次)"""
        cal = CalendarAstock()
        for row in pydt_from_sec_list(cal.get_tradedays_gte()):
            tradeday = row.strftime("%Y%m%d")
            t = time()
            do_download = False
            # 龙虎榜数据从2005年开始
            f = os.path.join(self.output, "lhb", f"{tradeday}.csv")
            self.lhb_addition_path
            if not os.path.exists(f):
                df = self.api.top_list(trade_date=tradeday)
                df.to_csv(f, index=False)
                do_download = True

            # 龙虎榜明细数据只从2012年开始
            if tradeday > "20120101":
                f2 = os.path.join(self.output, "lhb_inst", f"{tradeday}.csv")
                if not os.path.exists(f2):
                    df = self.api.top_inst(trade_date=tradeday)
                    df.to_csv(f2, index=False)
                    do_download = True
            if do_download:
                eclipse = time() - t
                wait_t = 0.13 - eclipse
                if wait_t > 0:
                    sleep(wait_t)

    @log
    def full_download_cb_daily(self):
        """全量下载可转债日线数据"""
        if not self.basic_cb_path.exists():
            logging.error(
                f"{self.basic_cb_path}不存在，必须先调用 full_download_cb_basic"
            )
            return
        cb_basic_df = pd.read_csv(self.basic_cb_path)
        for row in cb_basic_df.itertuples():
            f = Path(self.cb_daily_bars_path, f"{row.ts_code}.csv")
            if f.exists():
                df = pd.read_csv(f)
                if "bond_value" not in df:
                    f.unlink()
                    logging.info(f"  删除{f}")
            if not f.exists():
                df = self.api.cb_daily(
                    ts_code=row.ts_code,
                    fields=[
                        "ts_code",
                        "trade_date",
                        "pre_close",
                        "open",
                        "high",
                        "low",
                        "close",
                        "change",
                        "pct_chg",
                        "vol",
                        "amount",
                        "bond_value",
                        "bond_over_rate",
                        "cb_value",
                        "cb_over_rate",
                    ],
                )
                if len(df) >= 2000:
                    logging.error(f"  {row.ts_code} 超过限额，需要再拉取")
                df.to_csv(f, index=False)

    @log
    def addition_download_cb_daily(self):
        """增量下载可转债日线数据"""
        df = self.api.cb_daily(
            trade_date=self.trade_date,
            fields=[
                "ts_code",
                "trade_date",
                "pre_close",
                "open",
                "high",
                "low",
                "close",
                "change",
                "pct_chg",
                "vol",
                "amount",
                "bond_value",
                "bond_over_rate",
                "cb_value",
                "cb_over_rate",
            ],
        )
        df.to_csv(self.cb_daily_bars_addition_path)

    @log
    def full_download_cb_share_data(self, include_delist_cbs=True, force_replace=False):
        """全量下载可转债转股数据(接口限流每分钟400次)"""
        if not self.basic_cb_path.exists():
            logging.error(
                f"{self.basic_cb_path}不存在，必须先调用 full_download_cb_basic"
            )
            return
        cb_basic_df = pd.read_csv(self.basic_cb_path)
        for row in cb_basic_df.itertuples():
            if not include_delist_cbs and row.remain_size == 0:
                continue
            f = Path(self.cb_share_path, f"{row.ts_code}.csv")
            if force_replace or not f.exists():
                t = time()
                df = self.api.cb_share(ts_code=row.ts_code)
                df.to_csv(f, index=False)
                eclipse = time() - t
                wait_t = 0.16 - eclipse
                if wait_t > 0:
                    sleep(wait_t)

    @log
    def full_download_cb_call_data(self, include_delist_cbs=True, force_replace=False):
        """全量下载可转债赎回数据(接口限流每分钟400次)"""
        if not self.basic_cb_path.exists():
            logging.error(
                f"{self.basic_cb_path}不存在，必须先调用 full_download_cb_basic"
            )
            return
        cb_basic_df = pd.read_csv(self.basic_cb_path)
        for row in cb_basic_df.itertuples():
            if not include_delist_cbs and row.remain_size == 0:
                continue
            f = Path(self.cb_call_path, f"{row.ts_code}.csv")
            if force_replace or not f.exists():
                t = time()
                df = self.api.cb_call(ts_code=row.ts_code)
                df.to_csv(f, index=False)
                eclipse = time() - t
                wait_t = 0.16 - eclipse
                if wait_t > 0:
                    sleep(wait_t)

    @log
    def addition_download_daily(self):
        """增量下载个股日线"""
        df = self.api.daily(trade_date=self.trade_date)
        df.to_csv(self.daily_bars_addition_path)

    @log
    def addition_download_daily_basic(self):
        """增量下载每日指标"""
        df = self.api.daily_basic(
            ts_code="",
            trade_date=self.trade_date,
            fields="ts_code,trade_date,turnover_rate,pe,pe_ttm,pb,total_share,float_share,total_mv,circ_mv,limit_status",
        )
        df.to_csv(self.daily_basic_addition_path)

    @log
    def addition_download_moneyflow(self):
        """增量下载每日资金流"""
        df = self.api.moneyflow(trade_date=self.trade_date)
        df.to_csv(self.moneyflow_addition_path)

    @log
    def addition_download_lhb(self):
        """增量下载龙虎榜"""
        df = self.api.top_list(trade_date=self.trade_date)
        df.to_csv(self.lhb_addition_path, index=False)
        df = self.api.top_inst(trade_date=self.trade_date)
        df.to_csv(self.lhb_inst_addition_path, index=False)

    @log
    def addition_download_index(self, index_codes):
        """下载指数数据"""
        cal = CalendarAstock()
        next_tradeday = pydt_from_second(
            cal.get_tradeday_next(self.dt + timedelta(days=1))
        )
        is_last_weekday = next_tradeday.isocalendar()[1] != self.dt.isocalendar()[1]
        is_last_monthday = next_tradeday.month != self.dt.month
        logging.debug(
            f"  下一交易日 {next_tradeday}, is_last_weekday: {is_last_weekday}, is_last_monthday: {is_last_monthday}"
        )
        index_daily_dfs = []
        index_week_dfs = []
        index_month_dfs = []
        for index_code, _ in index_codes:
            d = self.api.index_daily(ts_code=index_code, trade_date=self.trade_date)
            w = self.api.index_weekly(ts_code=index_code, trade_date=self.trade_date)
            m = self.api.index_monthly(ts_code=index_code, trade_date=self.trade_date)
            if d.empty:
                logging.error(f"  大盘指数 {index_code} {self.trade_date} 日线数据为空")
            else:
                index_daily_dfs.append(d)
            if w.empty:
                if is_last_weekday:
                    logging.error(
                        f"  大盘指数 {index_code} {self.trade_date} 周线数据为空"
                    )
            else:
                index_week_dfs.append(w)
            if m.empty:
                if is_last_monthday:
                    logging.error(
                        f"  大盘指数 {index_code} {self.trade_date} 月线数据为空"
                    )
            else:
                index_month_dfs.append(m)
        if index_daily_dfs:
            index_daily_df = pd.concat(index_daily_dfs)
            index_daily_df.to_csv(Path(self.index_daily_addition_path))
        if index_week_dfs:
            index_week_df = pd.concat(index_week_dfs)
            index_week_df.to_csv(Path(self.index_week_addition_path))
        if index_month_dfs:
            index_month_df = pd.concat(index_month_dfs)
            index_month_df.to_csv(Path(self.index_month_addition_path))

    @log
    def full_download_all_future_bars(self):
        """全量下载期货日线数据（只能下载日线，在下午5点之后调用，3点之后可能还没有更新好）"""
        # 合约类型 (1 普通合约 2主力与连续合约 默认取全部)
        # 中金所
        df1 = self.api.fut_basic(
            exchange="CFFEX",
            fut_type="1",
            fields="ts_code,symbol,name,list_date,delist_date",
        )
        df1["exchange"] = "CFFEX"
        # 大商所
        df2 = self.api.fut_basic(
            exchange="DCE",
            fut_type="1",
            fields="ts_code,symbol,name,list_date,delist_date",
        )
        df2["exchange"] = "DCE"
        # 郑商所
        df3 = self.api.fut_basic(
            exchange="CZCE",
            fut_type="1",
            fields="ts_code,symbol,name,list_date,delist_date",
        )
        df3["exchange"] = "CZCE"
        # 上期所
        df4 = self.api.fut_basic(
            exchange="SHFE",
            fut_type="1",
            fields="ts_code,symbol,name,list_date,delist_date",
        )
        df4["exchange"] = "SHFE"
        # 上海国际能源交易中心
        df5 = self.api.fut_basic(
            exchange="INE",
            fut_type="1",
            fields="ts_code,symbol,name,list_date,delist_date",
        )
        df5["exchange"] = "INE"
        # 广州期货交易所
        df6 = self.api.fut_basic(
            exchange="GFEX",
            fut_type="1",
            fields="ts_code,symbol,name,list_date,delist_date",
        )
        df6["exchange"] = "GFEX"
        df = pd.concat([df1, df2, df3, df4, df5, df6], ignore_index=True)
        df["delist_date"] = pd.to_datetime(df["delist_date"], format="%Y%m%d")
        history_df = df.loc[df["delist_date"] < self.dt]
        current_df = df.loc[df["delist_date"] >= self.dt]
        for row in history_df.itertuples():
            pathname = Path(self.future_daily_history_path, f"{row.ts_code}.csv")
            if not pathname.exists():
                logging.info(f"下载历史{row.ts_code}日线")
                _df = self.api.fut_daily(ts_code=row.ts_code)
                _df.to_csv(pathname)

        if self.future_daily_current_path.exists():
            shutil.rmtree(self.future_daily_current_path)
        os.makedirs(self.future_daily_current_path)
        for row in current_df.itertuples():
            logging.info(f"  下载线上{row.ts_code}日线")
            _df = self.api.fut_daily(ts_code=row.ts_code)
            _df.to_csv(Path(self.future_daily_current_path, f"{row.ts_code}.csv"))
