# 收集期货合约手续费和保证金率（每个券商不一样）
import concurrent.futures
import inspect
import json
import logging
import re
import threading
import time
from collections import deque, namedtuple
from pathlib import Path

import requests
from openctp_ctp import tdapi

from .utils import log

output = "datasource/CTPFuture/ctp"


def _get_last_prices(symbols):
    gudaima = ",".join(symbols)
    headers = {"referer": "http://finance.sina.com.cn"}
    resp = requests.get(
        "http://hq.sinajs.cn/list=" + gudaima, headers=headers, timeout=6
    )
    ret = {}
    for line in resp.text.split("\n"):
        grp = re.match(r'^var hq_str_(\w+)="(.*)";$', line)
        if grp:
            symbol = grp[1]
            content = grp[2]
            if content:
                data = content.split(",")
                symbol_name = data[0]
                last_price = float(data[8])
                ret[symbol] = (symbol_name, last_price)

    return ret


def roundToPriceTick(price, priceTick):
    if isinstance(priceTick, str):
        _str_pricetick = priceTick
        _float_pricetick = float(priceTick)
    else:
        _str_pricetick = str(priceTick)
        _float_pricetick = priceTick
    numofdigit = 0
    try:
        numofdigit = len(_str_pricetick) - _str_pricetick.index(".") - 1
    except ValueError:
        pass
    if price % _float_pricetick != 0:
        return round(
            float(int(round(price / _float_pricetick)) * _float_pricetick), numofdigit
        )
    return price


UserProductInfo = "BMax"
QueryResult = namedtuple("QueryResult", ["final", "temp"])


class CtpApi(tdapi.CThostFtdcTraderSpi):
    requestID = 0

    def __init__(self, config):
        tdapi.CThostFtdcTraderSpi.__init__(self)
        self.contracts = {}
        self.commissions = {}
        self.config = config
        self.query_results = {}  # 查询结果存放
        self.queryEvent = threading.Event()
        self.launchEvent = threading.Event()
        self.g_liukong_time = time.time()  # 用于查询流控
        self.connect()

    def get_query_results(self, query_method_name, is_temp=False, setdefault=None):
        t = self.query_results.setdefault(
            query_method_name, QueryResult(final=setdefault, temp=setdefault)
        )
        if is_temp:
            res = t.temp
        else:
            res = t.final
        return res

    def connect(self):
        logging.info("交易系统开始连接")
        self.tradeapi: tdapi.CThostFtdcTraderApi = (
            tdapi.CThostFtdcTraderApi.CreateFtdcTraderApi()
        )
        self.tradeapi.RegisterSpi(self)
        self.tradeapi.SubscribePrivateTopic(tdapi.THOST_TERT_QUICK)
        self.tradeapi.SubscribePublicTopic(tdapi.THOST_TERT_QUICK)
        frontAddresses = self.config["FrontAddresses"]
        for frontAddress in frontAddresses:
            logging.info(f"{__package__} Register Front: {frontAddress}")
            self.tradeapi.RegisterFront(frontAddress)
        # 如果不在交易时段，Init会报并停止继续执行
        self.tradeapi.Init()

    def close(self):
        if self.tradeapi is not None:
            logging.debug("Release")
            self.tradeapi.RegisterSpi(None)
            self.tradeapi.Release()
            self.tradeapi = None
            logging.debug("Release Ok")

    def launch(self):
        self.launchEvent.clear()
        self.auth()

    def auth(self):
        authfield = tdapi.CThostFtdcReqAuthenticateField()
        authfield.BrokerID = self.config["BrokerID"]
        authfield.UserID = self.config["UserID"]
        authfield.AppID = self.config["AppID"]
        authfield.AuthCode = self.config["AuthCode"]
        authfield.UserProductInfo = UserProductInfo
        self.requestID += 1
        ret = -1
        while ret < 0:
            ret = self.tradeapi.ReqAuthenticate(authfield, self.requestID)
            if ret == 0:
                logging.debug("ReqAuthenticate")
            else:
                logging.error(f"ReqAuthenticate fail: {ret}. Retry...")
                time.sleep(1)

    def login(self):
        loginfield = tdapi.CThostFtdcReqUserLoginField()
        loginfield.BrokerID = self.config["BrokerID"]
        loginfield.UserID = self.config["UserID"]
        loginfield.Password = self.config["Password"]
        loginfield.UserProductInfo = UserProductInfo
        self.requestID += 1
        ret = -1
        while ret < 0:
            ret = self.tradeapi.ReqUserLogin(loginfield, self.requestID)
            if ret == 0:
                logging.debug("ReqUserLogin")
            else:
                logging.error(f"ReqUserLogin fail: {ret}. Retry...")
                time.sleep(1)

    def logout(self):
        logoutfield = tdapi.CThostFtdcUserLogoutField()
        logoutfield.BrokerID = self.config["BrokerID"]
        logoutfield.UserID = self.config["UserID"]
        self.requestID += 1
        ret = self.tradeapi.ReqUserLogout(logoutfield, self.requestID)
        if ret == 0:
            logging.debug("ReqUserLogout ok")
        else:
            logging.error(f"ReqUserLogout fail: {ret}")

    def OnFrontConnected(self) -> None:
        logging.debug("OnFrontConnected")
        self.launch()

    def OnFrontDisconnected(self, nReason: int) -> None:
        if nReason == 0x1001:
            logging.error("OnFrontDisconnected Reason: 网络读失败")
        elif nReason == 0x1002:
            logging.error("OnFrontDisconnected Reason: 网络写失败")
        elif nReason == 0x2001:
            logging.error("OnFrontDisconnected Reason: 接收心跳超时")
        elif nReason == 0x2002:
            logging.error("OnFrontDisconnected Reason: 发送心跳失败")
        elif nReason == 0x2003:
            logging.error("OnFrontDisconnected Reason: 收到错误报文")

    def OnRspAuthenticate(
        self,
        pRspAuthenticateField: tdapi.CThostFtdcRspAuthenticateField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ) -> None:
        if pRspInfo and pRspInfo.ErrorID > 0:
            logging.error(
                f"OnRspAuthenticate Error: {pRspInfo.ErrorID}  {pRspInfo.ErrorMsg}"
            )
            return
        if pRspAuthenticateField:
            logging.debug("OnRspAuthenticate ok")
            logging.info(f"> BrokerID = {pRspAuthenticateField.BrokerID}")
            logging.info(f"> UserID = {pRspAuthenticateField.UserID}")
            logging.info(f"> AppID = {pRspAuthenticateField.AppID}")
            logging.info(f"> AppType = {pRspAuthenticateField.AppType}")
            self.login()

    def OnRspUserLogin(
        self,
        pRspUserLogin: tdapi.CThostFtdcRspUserLoginField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ) -> None:
        if pRspInfo and pRspInfo.ErrorID > 0:
            logging.error(
                f"OnRspUserLogin Error: {pRspInfo.ErrorID}  {pRspInfo.ErrorMsg}"
            )
            if pRspInfo.ErrorID == 140:  # 首次登录必须修改密码，请修改密码后重新登录
                psdUpdField = tdapi.CThostFtdcUserPasswordUpdateField()
                psdUpdField.BrokerID = self.config["BrokerID"]
                psdUpdField.UserID = self.config["UserID"]
                psdUpdField.OldPassword = self.config["Password"]
                psdUpdField.NewPassword = ""
                self.requestID += 1
                self.tradeapi.ReqUserPasswordUpdate(psdUpdField, self.requestID)
            return
        logging.debug("OnRspUserLogin ok")
        if pRspUserLogin:
            self.sessionId = pRspUserLogin.SessionID
            self.frontId = pRspUserLogin.FrontID
            self.maxOrderRef = int(pRspUserLogin.MaxOrderRef)
            self.tradingDay = pRspUserLogin.TradingDay
            self.launchEvent.set()

    def OnRspUserLogout(
        self,
        pUserLogout: tdapi.CThostFtdcUserLogoutField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ) -> None:
        if pRspInfo and pRspInfo.ErrorID > 0:
            logging.error(
                f"OnRspUserLogout Error: {pRspInfo.ErrorID}  {pRspInfo.ErrorMsg}"
            )
        else:
            logging.debug("OnRspUserLogout ok")

    def OnRspUserPasswordUpdate(
        self,
        pUserPasswordUpdate: tdapi.CThostFtdcUserPasswordUpdateField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ) -> None:
        if pRspInfo and pRspInfo.ErrorID > 0:
            logging.error(
                f"OnRspUserPasswordUpdate Error: {pRspInfo.ErrorID}  {pRspInfo.ErrorMsg}"
            )
        else:
            logging.debug("OnRspUserPasswordUpdate ok")

    def OnRspSettlementInfoConfirm(
        self,
        pSettlementInfoConfirm: tdapi.CThostFtdcSettlementInfoConfirmField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ) -> None:
        logging.debug("OnRspSettlementInfoConfirm ok")
        if pRspInfo and pRspInfo.ErrorID > 0:
            logging.error(
                f"OnRspSettlementInfoConfirm: {pRspInfo.ErrorID}  {pRspInfo.ErrorMsg}"
            )
            return
        self.queryEvent.set()

    def OnRspQrySettlementInfo(
        self,
        pSettlementInfo: tdapi.CThostFtdcSettlementInfoField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ) -> None:
        logging.debug("OnRspQrySettlementInfo")
        if pRspInfo and pRspInfo.ErrorID > 0:
            logging.error(
                f"OnRspQrySettlementInfo Error: {pRspInfo.ErrorID}  {pRspInfo.ErrorMsg}"
            )
            return
        if pSettlementInfo:
            logging.debug(f"OnRspQrySettlementInfo content: {pSettlementInfo.Content}")
        else:
            logging.debug(f"OnRspQrySettlementInfo content: None")
        if bIsLast:
            self.queryEvent.set()

    def OnRspQryInstrument(
        self,
        pInstrument: tdapi.CThostFtdcInstrumentField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ) -> None:
        if pRspInfo and pRspInfo.ErrorID > 0:
            logging.error(
                f"OnRspQryInstrument: {pRspInfo.ErrorID}  {pRspInfo.ErrorMsg}"
            )
            self.queryEvent.set()
            return
        # 将期货以外的其他品种过滤掉
        if pInstrument.ProductClass == tdapi.THOST_FTDC_PC_Futures:
            contract = {
                "instrument_id": pInstrument.InstrumentID,
                "exchange_id": pInstrument.ExchangeID,
                # "instrument_name"      : pInstrument.InstrumentName,
                "volume_multiple": pInstrument.VolumeMultiple,
                "price_tick": pInstrument.PriceTick,
                "is_trading": pInstrument.IsTrading == 1,
                "delivery_year": pInstrument.DeliveryYear,
                "delivery_month": pInstrument.DeliveryMonth,
                "max_market_order_volume": pInstrument.MaxMarketOrderVolume,
                "min_market_order_volume": pInstrument.MinMarketOrderVolume,
                "max_limit_order_volume": pInstrument.MaxLimitOrderVolume,
                "min_limit_order_volume": pInstrument.MinLimitOrderVolume,
                # "long_margin_ratio"        : pInstrument.LongMarginRatio,
                # "short_margin_ratio"       : pInstrument.ShortMarginRatio
            }
            self.contracts[contract["instrument_id"]] = contract

        if bIsLast:
            logging.debug("OnRspQryInstrument ok")
            self.queryEvent.set()
            if nRequestID > self.requestID:
                self.requestID = nRequestID

    def OnRspQryInstrumentMarginRate(
        self,
        pInstrumentMarginRate: tdapi.CThostFtdcInstrumentMarginRateField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        if pRspInfo and pRspInfo.ErrorID > 0:
            logging.error(
                f"OnRspQryInstrumentMarginRate: {pRspInfo.ErrorID}  {pRspInfo.ErrorMsg}"
            )
        else:
            if pInstrumentMarginRate:
                logging.info(
                    str(
                        {
                            name: value
                            for name, value in inspect.getmembers(pInstrumentMarginRate)
                            if name[0].isupper()
                        }
                    )
                )
                logging.debug(f"OnRspQryInstrumentMarginRate ok")
        tqr = self.get_query_results("queryInstrumentMarginRatio", is_temp=True)
        if tqr:
            tqr.popleft()
        if len(tqr) == 0:
            self.queryEvent.set()

    def OnRspQryInstrumentCommissionRate(
        self,
        pInstrumentCommissionRate: tdapi.CThostFtdcInstrumentCommissionRateField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        if pRspInfo and pRspInfo.ErrorID > 0:
            logging.error(
                f"OnRspQryInstrumentCommissionRate: {pRspInfo.ErrorID}  {pRspInfo.ErrorMsg}"
            )
        else:
            if pInstrumentCommissionRate:
                logging.info(
                    str(
                        {
                            name: value
                            for name, value in inspect.getmembers(
                                pInstrumentCommissionRate
                            )
                            if name[0].isupper()
                        }
                    )
                )
                logging.debug(
                    f"OnRspQryInstrumentCommissionRate ok {pInstrumentCommissionRate.InstrumentID}"
                )
        tqr = self.get_query_results("queryCommission", is_temp=True)
        if tqr:
            tqr.popleft()
        if len(tqr) == 0:
            self.queryEvent.set()

    # TODO: 暂时不考虑报单手续费，中金所特有
    # def OnRspQryInstrumentOrderCommRate(pInstrumentOrderCommRate: tdapi.CThostFtdcInstrumentOrderCommRateField, pRspInfo: tdapi.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
    #     pass
    def on_common_query(self, queryKey, pResult, pRspInfo, bIsLast):
        if pRspInfo and pRspInfo.ErrorID > 0:
            logging.error(
                f"on_common_query {queryKey}: {pRspInfo.ErrorID}  {pRspInfo.ErrorMsg}"
            )
        else:
            if pResult:
                data = {
                    name: value
                    for name, value in inspect.getmembers(pResult)
                    if name[0].isupper()
                }
                tqr = self.get_query_results(queryKey, is_temp=True, setdefault=[])
                tqr.append(data)
        if bIsLast:
            t = self.query_results.get(queryKey, None)
            if t is not None:
                self.query_results[queryKey] = QueryResult(final=t.temp, temp=[])
            self.queryEvent.set()

    def OnRspQryTransferSerial(
        self,
        pTransferSerial: tdapi.CThostFtdcTransferSerialField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        self.on_common_query("queryTransferSerial", pTransferSerial, pRspInfo, bIsLast)

    def OnRspError(
        self, pRspInfo: tdapi.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool
    ) -> None:
        if pRspInfo:
            logging.error(
                f"OnRspError->nRequestID: {nRequestID}, ErrorID: {pRspInfo.ErrorID} ErrorMsg: {pRspInfo.ErrorMsg}"
            )
            if nRequestID > self.requestID:
                self.requestID = nRequestID

    def querySettlementInfo(self):
        settlementinfo = tdapi.CThostFtdcQrySettlementInfoField()
        settlementinfo.BrokerID = self.config["BrokerID"]
        settlementinfo.InvestorID = self.config["UserID"]
        settlementinfo.TradingDay = self.tradingDay
        self.requestID += 1
        self.g_liukong_time = time.time()
        self.tradeapi.ReqQrySettlementInfo(settlementinfo, self.requestID)
        logging.debug("ReqQrySettlementInfo")

    def confirmSettlement(self):
        settlementinfoconfirm = tdapi.CThostFtdcSettlementInfoConfirmField()
        settlementinfoconfirm.BrokerID = self.config["BrokerID"]
        settlementinfoconfirm.InvestorID = self.config["UserID"]
        self.requestID += 1
        ret = -1
        while ret < 0:
            ret = self.tradeapi.ReqSettlementInfoConfirm(
                settlementinfoconfirm, self.requestID
            )
            if ret == 0:
                logging.debug("ReqSettlementInfoConfirm ok")
            else:
                logging.error(f"ReqSettlementInfoConfirm fail: {ret}. Retry...")
                time.sleep(1)

    def queryContracts(self, product_id=None):
        qryinstrument = tdapi.CThostFtdcQryInstrumentField()
        if product_id:
            qryinstrument.ProductID = product_id
        self.requestID += 1
        current_time = time.time()
        time.sleep(max(0.001, 1 - (current_time - self.g_liukong_time)))  # 流控
        self.g_liukong_time = time.time()
        logging.info("查询所有合约信息")
        result = self.tradeapi.ReqQryInstrument(qryinstrument, self.requestID)
        if result != 0:
            logging.error(
                f"查询合约出错 result: {result} self.requestID: {self.requestID}"
            )
        return result

    def queryInstrumentMarginRatio(self, symbols, checkmode):
        """查询保证金率"""
        for symbol in symbols:
            qryinstrumentmarginrate = tdapi.CThostFtdcQryInstrumentMarginRateField()
            qryinstrumentmarginrate.BrokerID = self.config["BrokerID"]
            qryinstrumentmarginrate.InvestorID = self.config["UserID"]
            qryinstrumentmarginrate.InstrumentID = symbol
            qryinstrumentmarginrate.HedgeFlag = tdapi.THOST_FTDC_HF_Speculation
            self.requestID += 1
            current_time = time.time()
            time.sleep(max(0.001, 1 - (current_time - self.g_liukong_time)))  # 流控
            self.g_liukong_time = time.time()
            logging.info(f"查询 {symbol} 保证金率")
            result = -1
            while result < 0:
                result = self.tradeapi.ReqQryInstrumentMarginRate(
                    qryinstrumentmarginrate, self.requestID
                )
                if result < 0:
                    logging.error(
                        f"查询保证金出错 result: {result} self.requestID: {self.requestID}"
                    )
                else:
                    tqr = self.get_query_results(
                        "queryInstrumentMarginRatio", is_temp=True, setdefault=deque()
                    )
                    tqr.append(symbol)
                if not checkmode:
                    break

    def queryCommission(self, symbols, checkmode):
        """查询手续费率"""
        for symbol in symbols:
            qrycommissionrate = tdapi.CThostFtdcQryInstrumentCommissionRateField()
            qrycommissionrate.BrokerID = self.config["BrokerID"]
            qrycommissionrate.InvestorID = self.config["UserID"]
            qrycommissionrate.InstrumentID = symbol
            self.requestID += 1
            current_time = time.time()
            time.sleep(max(0.001, 1 - (current_time - self.g_liukong_time)))  # 流控
            self.g_liukong_time = time.time()
            logging.info(f"查询{symbol}手续费率")
            result = -1
            while result < 0:
                result = self.tradeapi.ReqQryInstrumentCommissionRate(
                    qrycommissionrate, self.requestID
                )
                if result < 0:
                    logging.error(
                        f"查询手续费出错 result: {result} self.requestID: {self.requestID}"
                    )
                else:
                    tqr = self.get_query_results(
                        "queryCommission", is_temp=True, setdefault=deque()
                    )
                    tqr.append(symbol)
                if not checkmode:
                    break

    def queryTransferSerial(self):
        qrytransferserialfield = tdapi.CThostFtdcQryTransferSerialField()
        qrytransferserialfield.BrokerID = self.config["BrokerID"]
        qrytransferserialfield.AccountID = self.config["UserID"]
        qrytransferserialfield.CurrencyID = "CNY"
        self.requestID += 1
        current_time = time.time()
        time.sleep(max(0.001, 1 - (current_time - self.g_liukong_time)))  # 流控
        self.g_liukong_time = time.time()
        logging.info(f"查询转账流水")
        result = self.tradeapi.ReqQryTransferSerial(
            qrytransferserialfield, self.requestID
        )
        if result < 0:
            logging.error(
                f"查询转账流水 result: {result} self.requestID: {self.requestID}"
            )
        return result


class SimpleCtpApi(CtpApi):
    """只支持登录查询功能"""

    def __init__(self, config, output):
        super().__init__(config)
        self.contracts_path = Path(output, f'contracts_{self.config["BrokerID"]}.json')
        self.margin_ratios = {}

    def result(self, query_method_name, is_temp=False, setdefault=None):
        if query_method_name == "queryContracts":
            return self.contracts
        elif query_method_name == "queryInstrumentMarginRatio":
            return self.margin_ratios
        elif query_method_name == "queryCommission":
            return self.commissions
        return self.get_query_results(query_method_name, is_temp, setdefault)

    def OnRspQryInstrumentMarginRate(
        self,
        pInstrumentMarginRate: tdapi.CThostFtdcInstrumentMarginRateField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        if pRspInfo and pRspInfo.ErrorID > 0:
            logging.error(
                f"OnRspQryInstrumentMarginRate: {pRspInfo.ErrorID}  {pRspInfo.ErrorMsg}"
            )
        else:
            if pInstrumentMarginRate:
                self.margin_ratios[pInstrumentMarginRate.InstrumentID] = {
                    "long_margin_ratio_bymoney": pInstrumentMarginRate.LongMarginRatioByMoney,
                    "short_margin_ratio_bymoney": pInstrumentMarginRate.ShortMarginRatioByMoney,
                    "long_margin_ratio_byvolume": pInstrumentMarginRate.LongMarginRatioByVolume,
                    "short_margin_ratio_byvolume": pInstrumentMarginRate.ShortMarginRatioByVolume,
                }
        tqr = self.get_query_results("queryInstrumentMarginRatio", is_temp=True)
        if tqr:
            tqr.popleft()
        if len(tqr) == 0:
            self.queryEvent.set()

    def OnRspQryInstrumentCommissionRate(
        self,
        pInstrumentCommissionRate: tdapi.CThostFtdcInstrumentCommissionRateField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        if pRspInfo and pRspInfo.ErrorID > 0:
            logging.error(
                f"OnRspQryInstrumentCommissionRate: {pRspInfo.ErrorID}  {pRspInfo.ErrorMsg}"
            )
        else:
            if pInstrumentCommissionRate:
                self.commissions[pInstrumentCommissionRate.InstrumentID] = {
                    "commission_of_open_bymoney": pInstrumentCommissionRate.OpenRatioByMoney,
                    "commission_of_open_byvolume": pInstrumentCommissionRate.OpenRatioByVolume,
                    "commission_of_close_bymoney": pInstrumentCommissionRate.CloseRatioByMoney,
                    "commission_of_close_byvolume": pInstrumentCommissionRate.CloseRatioByVolume,
                    "commission_of_closetoday_bymoney": pInstrumentCommissionRate.CloseTodayRatioByMoney,
                    "commission_of_closetoday_byvolume": pInstrumentCommissionRate.CloseTodayRatioByVolume,
                }
        tqr = self.get_query_results("queryCommission", is_temp=True)
        if tqr:
            tqr.popleft()
        if len(tqr) == 0:
            self.queryEvent.set()

    def query_api(self, method_name, wait_time=10, **kwargs):
        if not self.launchEvent.wait(10):
            logging.error("api launch timeout")
            return None
        function = getattr(self, method_name, None)
        if function is None:
            logging.error(f"{method_name} is not in ctp_api")
            return None
        else:
            function(**kwargs)
            ret = None
            if not self.queryEvent.wait(wait_time):
                logging.error(f"api {method_name} timeout")
            if ret is None:
                ret = self.result(method_name)
            if ret is None:
                logging.error(f"api {method_name} return None")
        return ret

    @log
    def full_download_contracts(self):
        """全量下载期货手续费及保证金"""
        contracts = self.query_api("queryContracts", wait_time=120)
        if contracts is not None:
            symbols = list(contracts.keys())
            workers = 1  # 可以同时登录3个（4个会有1个登录失败导致合约信息不全）
            length = len(symbols)
            split_symbols = [
                symbols[i * length // workers : (i + 1) * length // workers]
                for i in range(workers)
            ]
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                margin_ratios_futures = [
                    executor.submit(
                        self.query_api,
                        "queryInstrumentMarginRatio",
                        wait_time=600,
                        symbols=sub_symbols,
                        checkmode=False,
                    )
                    for sub_symbols in split_symbols
                ]
                commission_futures = [
                    executor.submit(
                        self.query_api,
                        "queryCommission",
                        wait_time=600,
                        symbols=sub_symbols,
                        checkmode=False,
                    )
                    for sub_symbols in split_symbols
                ]
                for future in concurrent.futures.as_completed(margin_ratios_futures):
                    try:
                        margin_ratios = future.result()
                        for symbol, margin_ratio in margin_ratios.items():
                            contract = contracts.get(symbol, None)
                            if contract:
                                contract.update(margin_ratio)
                    except Exception as exc:
                        logging.error(
                            "margin_ratios: generated an exception: %s" % (exc),
                            exc_info=True,
                        )
                for future in concurrent.futures.as_completed(commission_futures):
                    try:
                        commissions = future.result()
                        for symbol_or_product_id, commission in commissions.items():
                            contract = contracts.get(symbol_or_product_id, None)
                            if contract is not None:
                                _contracts = [contract]
                            else:
                                _contracts = [
                                    contract
                                    for s, contract in contracts.items()
                                    if s.startswith(symbol_or_product_id)
                                ]
                            for c in _contracts:
                                c.update(commission)
                    except Exception as exc:
                        logging.error(
                            "commissions: generated an exception: %s" % (exc),
                            exc_info=True,
                        )

            prices = _get_last_prices([symbol.upper() for symbol in contracts.keys()])
            for symbol, c in contracts.items():
                if symbol.upper() in prices:
                    price_data = prices[symbol.upper()]
                    symbol_name = price_data[0]
                    last_price = price_data[1]
                    mult = c["volume_multiple"]
                    c["last_price"] = last_price
                    c["symbol_name"] = symbol_name

                    if "long_margin_ratio_byvolume" in c:
                        long_margin_ratio_byvolume = c["long_margin_ratio_byvolume"]
                        long_margin_ratio_bymoney = c["long_margin_ratio_bymoney"]
                        short_margin_ratio_byvolume = c["short_margin_ratio_byvolume"]
                        short_margin_ratio_bymoney = c["short_margin_ratio_bymoney"]
                        long_margin = (
                            long_margin_ratio_byvolume
                            + long_margin_ratio_bymoney * last_price * mult
                        )
                        short_margin = (
                            short_margin_ratio_byvolume
                            + short_margin_ratio_bymoney * last_price * mult
                        )
                        c["long_margin"] = long_margin
                        c["short_margin"] = short_margin

                    if "commission_of_open_bymoney" in c:
                        commission_of_open_bymoney = c["commission_of_open_bymoney"]
                        commission_of_open_byvolume = c["commission_of_open_byvolume"]
                        commission_of_close_bymoney = c["commission_of_close_bymoney"]
                        commission_of_close_byvolume = c["commission_of_close_byvolume"]
                        commission_of_closetoday_bymoney = c[
                            "commission_of_closetoday_bymoney"
                        ]
                        commission_of_closetoday_byvolume = c[
                            "commission_of_closetoday_byvolume"
                        ]
                        open_comm = (
                            last_price * mult * commission_of_open_bymoney
                            + commission_of_open_byvolume
                        )
                        close_comm = (
                            last_price * mult * commission_of_close_bymoney
                            + commission_of_close_byvolume
                        )
                        closetoday_comm = (
                            last_price * mult * commission_of_closetoday_bymoney
                            + commission_of_closetoday_byvolume
                        )
                        c["open_comm"] = open_comm
                        c["close_comm"] = close_comm
                        c["closetoday_comm"] = closetoday_comm
                else:
                    c["last_price"] = 0
                    c["long_margin"] = 0
                    c["short_margin"] = 0
                    c["open_comm"] = 0
                    c["close_comm"] = 0
                    c["closetoday_comm"] = 0
                    c["symbol_name"] = 0
            with open(self.contracts_path, "w") as f:
                json.dump(list(contracts.values()), f)
