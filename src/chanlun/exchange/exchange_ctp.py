#!/usr/bin/env python
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Union

import pandas as pd
import pytz
from openctp_ctp.thostmduserapi import (
    CThostFtdcDepthMarketDataField,
    CThostFtdcMdApi,
    CThostFtdcReqAuthenticateField,
    CThostFtdcReqUserLoginField,
    CThostFtdcRspInfoField,
    CThostFtdcRspUserLoginField,
)
from tenacity import retry, retry_if_result, stop_after_attempt, wait_random

from chanlun import config
from chanlun.exchange.exchange import Exchange, Tick


class MarketCTP(Exchange):
    def __init__(self):
        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")

        # CTP配置，从配置文件读取
        self.broker_id = (
            config.CTP_BROKER_ID if hasattr(config, "CTP_BROKER_ID") else "9999"
        )
        self.user_id = config.CTP_USER_ID if hasattr(config, "CTP_USER_ID") else ""
        self.password = config.CTP_PASSWORD if hasattr(config, "CTP_PASSWORD") else ""
        self.app_id = (
            config.CTP_APP_ID if hasattr(config, "CTP_APP_ID") else ""
        )  # 添加AppID
        self.auth_code = (
            config.CTP_AUTH_CODE if hasattr(config, "CTP_AUTH_CODE") else ""
        )  # 添加授权码

        # 行情和交易服务器地址
        self.md_front = (
            config.CTP_MD_FRONT
            if hasattr(config, "CTP_MD_FRONT")
            else "tcp://180.168.146.187:10131"
        )
        self.td_front = (
            config.CTP_TD_FRONT
            if hasattr(config, "CTP_TD_FRONT")
            else "tcp://180.168.146.187:10130"
        )

        # 行情数据缓存
        self.ticks_cache = {}
        self.tick_callbacks = {}

        # 创建临时目录
        self.temp_path = os.path.expanduser("~/.chanlun-pro/ctp")
        os.makedirs(self.temp_path, exist_ok=True)

        # 扩展行情数据缓存
        self.ticks_cache = {}
        self.tick_queue = {}
        self.second_bars = {}
        self.max_tick_cache = 1000
        self.tick_callbacks = {}

        # 初始化行情接口
        self.md_api = None
        self.init_market()

    def init_market(self):
        """初始化行情接口"""

        class MdSpi(CThostFtdcMdApi):
            def __init__(self, market: Any) -> None:
                super().__init__()
                self.market = market
                self.connected: bool = False
                self.logged_in: bool = False
                self.authenticated: bool = False  # 添加认证状态

            def OnFrontConnected(self) -> None:
                print("行情服务器连接成功")
                self.connected = True

                # 如果设置了AppID，先进行认证
                if self.market.app_id:
                    req = CThostFtdcReqAuthenticateField()
                    req.BrokerID = self.market.broker_id
                    req.UserID = self.market.user_id
                    req.AppID = self.market.app_id
                    req.AuthCode = self.market.auth_code
                    self.ReqAuthenticate(req, 0)
                else:
                    # 没有设置AppID，直接登录
                    self._login()

            def OnRspAuthenticate(
                self, pRspAuthenticateField, pRspInfo, nRequestID, bIsLast
            ):
                """认证响应"""
                if pRspInfo and pRspInfo.ErrorID == 0:
                    print("交易账户认证成功")
                    self.authenticated = True
                    self._login()
                else:
                    print(
                        f"交易账户认证失败：{pRspInfo.ErrorMsg if pRspInfo else '未知错误'}"
                    )

            def _login(self):
                """执行登录"""
                req = CThostFtdcReqUserLoginField()
                req.BrokerID = self.market.broker_id
                req.UserID = self.market.user_id
                req.Password = self.market.password
                self.ReqUserLogin(req, 0)

            def OnRspUserLogin(
                self,
                pRspUserLogin: CThostFtdcRspUserLoginField,
                pRspInfo: CThostFtdcRspInfoField,
                nRequestID: int,
                bIsLast: bool,
            ) -> None:
                if pRspInfo and pRspInfo.ErrorID == 0:
                    self.logged_in = True
                    # 检查是否有Level2权限
                    self.has_level2 = (
                        hasattr(pRspUserLogin, "UserLevel")
                        and pRspUserLogin.UserLevel > 0
                    )
                    print(
                        f"行情服务器登录成功, {'有' if self.has_level2 else '无'}Level2权限"
                    )
                else:
                    print(
                        f"行情服务器登录失败：{pRspInfo.ErrorMsg if pRspInfo else '未知错误'}"
                    )

            def OnRtnDepthMarketData(
                self, pDepthMarketData: CThostFtdcDepthMarketDataField
            ) -> None:
                if not pDepthMarketData:
                    return

                code = pDepthMarketData.InstrumentID
                current_time = datetime.now()

                # 基础行情数据
                tick_data = {
                    "code": code,
                    "time": current_time,
                    "last": pDepthMarketData.LastPrice,
                    "high": pDepthMarketData.HighestPrice,
                    "low": pDepthMarketData.LowestPrice,
                    "open": pDepthMarketData.OpenPrice,
                    "volume": pDepthMarketData.Volume,
                    "amount": pDepthMarketData.Turnover,
                    "rate": (
                        (pDepthMarketData.LastPrice - pDepthMarketData.PreClosePrice)
                        / pDepthMarketData.PreClosePrice
                        * 100
                        if pDepthMarketData.PreClosePrice != 0
                        else 0
                    ),
                }

                # Level2行情数据
                if self.has_level2 and hasattr(pDepthMarketData, "BidPrice20"):
                    for i in range(1, 21):
                        tick_data[f"buy{i}"] = getattr(pDepthMarketData, f"BidPrice{i}")
                        tick_data[f"buy{i}_volume"] = getattr(
                            pDepthMarketData, f"BidVolume{i}"
                        )
                        tick_data[f"sell{i}"] = getattr(
                            pDepthMarketData, f"AskPrice{i}"
                        )
                        tick_data[f"sell{i}_volume"] = getattr(
                            pDepthMarketData, f"AskVolume{i}"
                        )
                # 普通五档行情
                else:
                    for i in range(1, 6):
                        tick_data[f"buy{i}"] = getattr(pDepthMarketData, f"BidPrice{i}")
                        tick_data[f"buy{i}_volume"] = getattr(
                            pDepthMarketData, f"BidVolume{i}"
                        )
                        tick_data[f"sell{i}"] = getattr(
                            pDepthMarketData, f"AskPrice{i}"
                        )
                        tick_data[f"sell{i}_volume"] = getattr(
                            pDepthMarketData, f"AskVolume{i}"
                        )

                tick = Tick(**tick_data)
                self.market.ticks_cache[code] = tick

                if code in self.market.tick_callbacks:
                    for callback in self.market.tick_callbacks[code]:
                        callback(tick)

        self.md_api = MdSpi(self)
        self.md_api.CreateFtdcMdApi(os.path.join(self.temp_path, "md"))
        self.md_api.RegisterFront(self.md_front)
        self.md_api.Init()

        # 等待连接成功
        for _ in range(10):
            if self.md_api.connected:
                break
            time.sleep(1)

        if not self.md_api.connected:
            raise Exception("行情服务器连接失败")

        # 登录
        req = CThostFtdcReqUserLoginField()
        req.BrokerID = self.broker_id
        req.UserID = self.user_id
        req.Password = self.password

        self.md_api.ReqUserLogin(req, 0)

        # 等待登录成功
        for _ in range(10):
            if self.md_api.logged_in:
                break
            time.sleep(1)

        if not self.md_api.logged_in:
            raise Exception("行情服务器登录失败")

    def default_code(self):
        return "rb2503"

    def support_frequencys(self):
        return {
            "d": "1d",
            "60m": "1h",
            "30m": "30m",
            "15m": "15m",
            "5m": "5m",
            "1m": "1m",
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random(min=1, max=5),
        retry=retry_if_result(lambda _r: _r is None),
    )
    def klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> Union[pd.DataFrame, None]:
        """获取K线数据"""
        # 这里需要实现从CTP获取历史K线数据的逻辑
        # 由于CTP不直接提供历史数据，可能需要对接其他数据源
        pass

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """获取实时行情"""
        if not codes:
            return {}

        # 订阅行情
        self.md_api.SubscribeMarketData(codes)
        time.sleep(1)  # 等待数据推送

        # 返回缓存的行情数据
        return {code: tick for code, tick in self.ticks_cache.items() if code in codes}

    def now_trading(self):
        """当前是否是交易时间"""
        now = datetime.datetime.now()
        if now.weekday() in [5, 6]:  # 周六日不交易
            return False

        hour = now.hour
        minute = now.minute

        # 日盘
        if (9 <= hour < 11) or (hour == 11 and minute <= 30):
            return True
        if 13 <= hour < 15:
            return True

        # 夜盘
        if 21 <= hour < 23:
            return True
        if hour == 23 and minute <= 30:
            return True

        return False

    def balance(self):
        raise Exception("CTP交易功能在trader目录实现")

    def positions(self, code: str = ""):
        raise Exception("CTP交易功能在trader目录实现")

    def order(self, code: str, o_type: str, amount: float, args=None):
        raise Exception("CTP交易功能在trader目录实现")


if __name__ == "__main__":
    market = MarketCTP()

    # 测试订阅行情
    codes = ["rb2401", "IF2401"]
    ticks = market.ticks(codes)
    print(ticks)
