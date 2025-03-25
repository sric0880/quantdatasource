"""
同花顺数据源
1. 热股排行榜
"""

import logging
import warnings
from pathlib import Path
from warnings import simplefilter

import pandas as pd
import requests

from .utils import log

warnings.filterwarnings("ignore")

simplefilter(action="ignore", category=FutureWarning)


class THSApi:
    def __init__(self, output, trade_date):
        self.dt = trade_date
        trade_date = trade_date.strftime("%Y-%m-%d")
        self.trade_date = trade_date
        self.output = Path(output, "ths")
        self.hot_stocks_addition_path = Path(
            self.output, f"hot_stocks_{trade_date}.csv"
        )

        self.output.mkdir(exist_ok=True)

    @log
    def addition_download_hot_stocks(self):
        """增量下载同花顺人气榜"""
        with requests.get(
            f"https://eq.10jqka.com.cn/open/api/hot_list/v1/hot_stock/a/hour/data.txt",
            headers={
                "Referer": "https://eq.10jqka.com.cn/frontend/thsTopRank/index.html?client_userid=eqedq&share_hxapp=gsc&share_action=&back_source=hyperlink",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
            },
        ) as resp:
            json_data = resp.json()
            stock_list = json_data["data"]["stock_list"]
            for stock_info in stock_list:
                if "tag" not in stock_info:
                    stock_info["concept_tag"] = ""
                    stock_info["popularity_tag"] = ""
                else:
                    concept_tags = stock_info["tag"].get("concept_tag", [])
                    concept_tag = ",".join(concept_tags)
                    stock_info["concept_tag"] = concept_tag
                    popularity_tag = stock_info["tag"].get("popularity_tag", "")
                    stock_info["popularity_tag"] = popularity_tag
                    stock_info.pop("tag")
            df = pd.DataFrame(stock_list)
            if df.empty:
                logging.warning(f"无同花顺热股数据")
                return
            df.to_csv(self.hot_stocks_addition_path)
