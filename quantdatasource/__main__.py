import fire

from quantdatasource.api.eastmoney import EastMoneyApi
from quantdatasource.api.tqsdk import TQSDKApi
from quantdatasource.api.tushare import TushareApi

if __name__ == "__main__":
    fire.Fire({"tushare": TushareApi, "eastmoney": EastMoneyApi, "tqsdk": TQSDKApi})
