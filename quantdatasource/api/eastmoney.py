"""
东方财富数据源
1. 财报数据
"""

import asyncio
import logging
import os
import time
import warnings
from pathlib import Path
from warnings import simplefilter

import aiohttp
import pandas as pd
import requests

from .utils import log

warnings.filterwarnings("ignore")

simplefilter(action="ignore", category=FutureWarning)


class EastMoneyApi:
    dir = "eastmoney"

    def __init__(self, output, trade_date):
        self.output = os.path.join(output, self.dir)
        self.dt = trade_date
        trade_date = trade_date.strftime("%Y-%m-%d")
        self.trade_date = trade_date
        self.analyst_reports_addition_path = Path(
            output, f"analyst_reports_{trade_date}.csv"
        )

    async def _download_analyst_reports(self, date_start, date_end):
        # 获取单页研报数据
        async def _download_analyst_report_one_page(client, page, date_start, date_end):
            async with client.get(
                f"https://reportapi.eastmoney.com/report/list?industryCode=*&pageSize=100&industry=*&beginTime={date_start}&endTime={date_end}&pageNo={page}&qType=0&code=*&_={int(round(time.time() * 1000))}"
            ) as resp:
                json_data = await resp.json(content_type=None)
                return pd.DataFrame(json_data["data"])

        async with aiohttp.ClientSession() as client:
            df = pd.concat(
                list(
                    await asyncio.gather(
                        *[
                            _download_analyst_report_one_page(
                                client, page, date_start, date_end
                            )
                            for page in range(1, 1380)
                        ]
                    )
                )
            )  # 并发爬取1-1380页
            df.to_csv(Path(self.output, f"analyst_reports_{date_start}_{date_end}.csv"))

    @log
    def full_download_analyst_reports(self, date_start, date_end):
        """
        下载研报数据("2018-01-01" - "2023-02-23")
        """
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(self._download_analyst_reports(date_start, date_end))

    @log
    def addition_download_analyst_reports(self):
        """增量下载研报数据"""
        date_start = date_end = self.trade_date
        page = 1
        lst_df = []
        while True:
            with requests.get(
                f"https://reportapi.eastmoney.com/report/list?industryCode=*&pageSize=100&industry=*&beginTime={date_start}&endTime={date_end}&pageNo={page}&qType=0&code=*&_={int(round(time.time() * 1000))}"
            ) as resp:
                json_data = resp.json()
                df = pd.DataFrame(json_data["data"])
                if df.empty:
                    break
                df["publishDate"] = pd.to_datetime(df["publishDate"])
                lst_df.append(df)
                if len(df) < 100 or df["publishDate"].iloc[0].date() < self.dt.date():
                    break
                page += 1
        if not lst_df:
            logging.info(f"  今日{date_end}无研报数据下载")
            return
        df = pd.concat(lst_df)
        df.to_csv(self.analyst_reports_addition_path)
        last_report = df.iloc[0]
        last_report_time = last_report["publishDate"]
        logging.info(f"  最新更新止{last_report_time}")
