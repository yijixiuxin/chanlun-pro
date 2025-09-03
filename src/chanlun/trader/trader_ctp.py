import os
import time
from datetime import datetime
from typing import Any, Dict

from openctp_ctp.thostmduserapi import (
    CThostFtdcInputOrderActionField,
    THOST_FTDC_AF_Delete,
)
from openctp_ctp.thosttraderapi import (
    THOST_FTDC_TC_GFD,  # 当日有效
    THOST_FTDC_VC_AV,  # 任意数量
    CThostFtdcInputOrderField,
    CThostFtdcOrderField,
    CThostFtdcQryInstrumentField,
    CThostFtdcQryInvestorPositionField,
    CThostFtdcQryOrderField,
    CThostFtdcQryTradeField,
    CThostFtdcQryTradingAccountField,
    CThostFtdcReqAuthenticateField,
    CThostFtdcReqUserLoginField,
    CThostFtdcSettlementInfoConfirmField,
    CThostFtdcTraderApi,
    THOST_FTDC_CC_Immediately,  # 立即触发
    THOST_FTDC_D_Buy,  # 买入
    THOST_FTDC_D_Sell,  # 卖出
    THOST_FTDC_HF_Speculation,  # 投机
    THOST_FTDC_OF_Close,  # 平仓
    THOST_FTDC_OPT_LimitPrice,  # 限价单
)

from chanlun import utils
from chanlun.backtesting.backtest_trader import BackTestTrader
from chanlun.backtesting.base import POSITION, Operation
from chanlun.db import db
from chanlun.exchange.exchange_ctp import MarketCTP


class MyTraderCallback(CThostFtdcTraderApi):
    """CTP交易回调"""

    def __init__(self, trader: Any) -> None:
        super().__init__()
        self.trader = trader
        self.connected: bool = False
        self.logged_in: bool = False
        self.authenticated: bool = False  # 添加认证状态
        self.front_id: int | None = None
        self.session_id: int | None = None
        self.order_ref: int = 0
        self.orders: Dict[str, CThostFtdcOrderField] = {}
        self.positions: Dict[str, Any] = {}

    def OnFrontConnected(self):
        print("交易服务器连接成功")
        self.connected = True

        # 如果设置了AppID，先进行认证
        if self.trader.ex.app_id:
            req = CThostFtdcReqAuthenticateField()
            req.BrokerID = self.trader.ex.broker_id
            req.UserID = self.trader.ex.user_id
            req.AppID = self.trader.ex.app_id
            req.AuthCode = self.trader.ex.auth_code
            self.ReqAuthenticate(req, 0)
        else:
            # 没有设置AppID，直接登录
            self._login()

    def OnRspAuthenticate(self, pRspAuthenticateField, pRspInfo, nRequestID, bIsLast):
        """认证响应"""
        if pRspInfo and pRspInfo.ErrorID == 0:
            print("交易账户认证成功")
            self.authenticated = True
            self._login()
        else:
            print(f"交易账户认证失败：{pRspInfo.ErrorMsg if pRspInfo else '未知错误'}")

    def _login(self):
        """执行登录"""
        req = CThostFtdcReqUserLoginField()
        req.BrokerID = self.trader.ex.broker_id
        req.UserID = self.trader.ex.user_id
        req.Password = self.trader.ex.password
        self.ReqUserLogin(req, 0)

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID == 0:
            self.logged_in = True
            self.front_id = pRspUserLogin.FrontID
            self.session_id = pRspUserLogin.SessionID
            print("交易服务器登录成功")
        else:
            print(f"交易服务器登录失败：{pRspInfo.ErrorMsg}")

    def OnRtnOrder(self, pOrder):
        """委托回报"""
        print(f"委托回报: {pOrder.InstrumentID} {pOrder.OrderStatus}")
        self.orders[pOrder.OrderRef] = pOrder

    def OnRtnTrade(self, pTrade):
        """成交回报"""
        print(
            f"成交回报: {pTrade.InstrumentID} 价格:{pTrade.Price} 数量:{pTrade.Volume}"
        )

    def OnRspQryInvestorPosition(
        self, pInvestorPosition, pRspInfo, nRequestID, bIsLast
    ):
        """持仓查询回报"""
        if pInvestorPosition:
            key = f"{pInvestorPosition.InstrumentID}_{pInvestorPosition.PosiDirection}"
            self.positions[key] = pInvestorPosition


class CTPTrader(BackTestTrader):
    """CTP期货交易实现"""

    def __init__(self, name, log=None):
        super().__init__(name=name, mode="online", market="futures", log=log)
        self.ex = MarketCTP()

        # 最大持仓数量
        self.max_pos = 3

        # 创建临时目录
        self.temp_path = os.path.expanduser("~/.ctp/ctp")
        os.makedirs(self.temp_path, exist_ok=True)

        # 初始化交易接口
        self.trader_api = MyTraderCallback(self)
        self.trader_api.CreateTrader(os.path.join(self.temp_path, "td"))
        self.trader_api.RegisterFront(self.ex.td_front)
        self.trader_api.Init()

        # 等待连接和登录
        self._wait_ready()

    def _wait_ready(self):
        """等待接口就绪"""
        for _ in range(10):
            if self.trader_api.connected:
                break
            time.sleep(1)

        if not self.trader_api.connected:
            raise Exception("交易服务器连接失败")

        # 等待认证和登录完成
        for _ in range(10):
            if self.trader_api.logged_in:
                break
            time.sleep(1)

        if not self.trader_api.logged_in:
            raise Exception("交易服务器登录失败")

    def close(self):
        """关闭接口"""
        if self.trader_api:
            self.trader_api.Release()

    def open_buy(self, code, opt: Operation, amount: float = None):
        """开多仓"""
        tick = self.ex.ticks([code])
        if code not in tick:
            return False

        # 检查持仓数量
        req_qry = CThostFtdcQryInvestorPositionField()
        req_qry.BrokerID = self.ex.broker_id
        req_qry.InvestorID = self.ex.user_id
        req_qry.InstrumentID = code
        self.trader_api.ReqQryInvestorPosition(req_qry, 0)

        for _ in range(10):
            if self.trader_api.logged_in:
                break
            time.sleep(1)

        if not self.trader_api.logged_in:
            raise Exception("交易服务器登录失败")

    def close(self):
        """关闭接口"""
        if self.trader_api:
            self.trader_api.Release()

    def open_buy(self, code, opt: Operation, amount: float = None):
        """开多仓"""
        tick = self.ex.ticks([code])
        if code not in tick:
            return False

        # 检查持仓数量
        self.trader_api.ReqQryInvestorPosition(
            ApiStruct.QryInvestorPosition(
                BrokerID=self.broker_id, InvestorID=self.user_id, InstrumentID=code
            ),
            0,
        )
        time.sleep(1)  # 等待查询结果

        if len(self.trader_api.positions) >= self.max_pos:
            return False

        # 下单
        self.trader_api.order_ref += 1
        req = ApiStruct.InputOrder(
            InstrumentID=code,
            OrderPriceType=ApiStruct.THOST_FTDC_OPT_LimitPrice,
            Direction=ApiStruct.THOST_FTDC_D_Buy,
            CombOffsetFlag=ApiStruct.THOST_FTDC_OF_Open,
            CombHedgeFlag=ApiStruct.THOST_FTDC_HF_Speculation,
            LimitPrice=tick[code].last,
            VolumeTotalOriginal=amount or 1,
            TimeCondition=ApiStruct.THOST_FTDC_TC_GFD,
            VolumeCondition=ApiStruct.THOST_FTDC_VC_AV,
            MinVolume=1,
            ContingentCondition=ApiStruct.THOST_FTDC_CC_Immediately,
            OrderRef=str(self.trader_api.order_ref),
        )

        result = self.trader_api.ReqOrderInsert(req, 0)
        if result != 0:
            return False

        # 等待订单回报
        time.sleep(1)
        order = self.trader_api.orders.get(str(self.trader_api.order_ref))
        if not order:
            return False

        # 记录订单
        db.order_save(
            "futures",
            code,
            code,
            "buy",
            tick[code].last,
            amount or 1,
            opt.msg,
            datetime.now(),
        )

        msg = (
            f"期货开多 {code} 价格 {tick[code].last} 数量 {amount or 1} 原因 {opt.msg}"
        )
        utils.send_fs_msg("futures_trader", "期货交易提醒", [msg])

        return {"price": tick[code].last, "amount": amount or 1}

    def open_sell(self, code, opt: Operation, amount: float = None):
        """开空仓"""
        tick = self.ex.ticks([code])
        if code not in tick:
            return False

        # 检查持仓数量
        self.trader_api.ReqQryInvestorPosition(
            ApiStruct.QryInvestorPosition(
                BrokerID=self.ex.broker_id,
                InvestorID=self.ex.user_id,
                InstrumentID=code,
            ),
            0,
        )
        time.sleep(1)

        if len(self.trader_api.positions) >= self.max_pos:
            return False

        # 下单
        self.trader_api.order_ref += 1
        req = ApiStruct.InputOrder(
            InstrumentID=code,
            OrderPriceType=ApiStruct.THOST_FTDC_OPT_LimitPrice,
            Direction=ApiStruct.THOST_FTDC_D_Sell,  # 卖出开仓
            CombOffsetFlag=ApiStruct.THOST_FTDC_OF_Open,  # 开仓
            CombHedgeFlag=ApiStruct.THOST_FTDC_HF_Speculation,
            LimitPrice=tick[code].last,
            VolumeTotalOriginal=amount or 1,
            TimeCondition=ApiStruct.THOST_FTDC_TC_GFD,
            VolumeCondition=ApiStruct.THOST_FTDC_VC_AV,
            MinVolume=1,
            ContingentCondition=ApiStruct.THOST_FTDC_CC_Immediately,
            OrderRef=str(self.trader_api.order_ref),
        )

        result = self.trader_api.ReqOrderInsert(req, 0)
        if result != 0:
            return False

        time.sleep(1)
        order = self.trader_api.orders.get(str(self.trader_api.order_ref))
        if not order:
            return False

        db.order_save(
            "futures",
            code,
            code,
            "sell",
            tick[code].last,
            amount or 1,
            opt.msg,
            datetime.now(),
        )

        msg = (
            f"期货开空 {code} 价格 {tick[code].last} 数量 {amount or 1} 原因 {opt.msg}"
        )
        utils.send_fs_msg("futures_trader", "期货交易提醒", [msg])

        return {"price": tick[code].last, "amount": amount or 1}

    def close_buy(self, code, pos: POSITION, opt):
        """平多仓"""
        tick = self.ex.ticks([code])
        if code not in tick:
            return False

        self.trader_api.order_ref += 1
        req = ApiStruct.InputOrder(
            InstrumentID=code,
            OrderPriceType=ApiStruct.THOST_FTDC_OPT_LimitPrice,
            Direction=ApiStruct.THOST_FTDC_D_Sell,
            CombOffsetFlag=ApiStruct.THOST_FTDC_OF_Close,
            CombHedgeFlag=ApiStruct.THOST_FTDC_HF_Speculation,
            LimitPrice=tick[code].last,
            VolumeTotalOriginal=pos.amount,
            TimeCondition=ApiStruct.THOST_FTDC_TC_GFD,
            VolumeCondition=ApiStruct.THOST_FTDC_VC_AV,
            MinVolume=1,
            ContingentCondition=ApiStruct.THOST_FTDC_CC_Immediately,
            OrderRef=str(self.trader_api.order_ref),
        )

        result = self.trader_api.ReqOrderInsert(req, 0)
        if result != 0:
            return False

        time.sleep(1)
        order = self.trader_api.orders.get(str(self.trader_api.order_ref))
        if not order:
            return False

        db.order_save(
            "futures",
            code,
            code,
            "sell",
            tick[code].last,
            pos.amount,
            opt.msg,
            datetime.now(),
        )

        msg = f"期货平多 {code} 价格 {tick[code].last} 数量 {pos.amount} 原因 {opt.msg}"
        utils.send_fs_msg("futures_trader", "期货交易提醒", [msg])

        return {"price": tick[code].last, "amount": pos.amount}

    def close_sell(self, code, pos: POSITION, opt):
        """平空仓"""
        tick = self.ex.ticks([code])
        if code not in tick:
            return False

        self.trader_api.order_ref += 1
        req = ApiStruct.InputOrder(
            InstrumentID=code,
            OrderPriceType=ApiStruct.THOST_FTDC_OPT_LimitPrice,
            Direction=ApiStruct.THOST_FTDC_D_Buy,  # 买入平仓
            CombOffsetFlag=ApiStruct.THOST_FTDC_OF_Close,  # 平仓
            CombHedgeFlag=ApiStruct.THOST_FTDC_HF_Speculation,
            LimitPrice=tick[code].last,
            VolumeTotalOriginal=pos.amount,
            TimeCondition=ApiStruct.THOST_FTDC_TC_GFD,
            VolumeCondition=ApiStruct.THOST_FTDC_VC_AV,
            MinVolume=1,
            ContingentCondition=ApiStruct.THOST_FTDC_CC_Immediately,
            OrderRef=str(self.trader_api.order_ref),
        )

        result = self.trader_api.ReqOrderInsert(req, 0)
        if result != 0:
            return False

        time.sleep(1)
        order = self.trader_api.orders.get(str(self.trader_api.order_ref))
        if not order:
            return False

        db.order_save(
            "futures",
            code,
            code,
            "buy",
            tick[code].last,
            pos.amount,
            opt.msg,
            datetime.now(),
        )

        msg = f"期货平空 {code} 价格 {tick[code].last} 数量 {pos.amount} 原因 {opt.msg}"
        utils.send_fs_msg("futures_trader", "期货交易提醒", [msg])

        return {"price": tick[code].last, "amount": pos.amount}

    def lock_position(self, code: str, pos: POSITION, opt: Operation):
        """锁仓操作
        当持有多仓时开等量空仓，或持有空仓时开等量多仓
        """
        tick = self.ex.ticks([code])
        if code not in tick:
            return False

        # 查询当前持仓
        self.trader_api.ReqQryInvestorPosition(
            ApiStruct.QryInvestorPosition(
                BrokerID=self.ex.broker_id,
                InvestorID=self.ex.user_id,
                InstrumentID=code,
            ),
            0,
        )
        time.sleep(1)

        # 根据持仓方向决定锁仓方向
        if pos.direction == "buy":
            # 持有多仓，开空仓锁仓
            self.trader_api.order_ref += 1
            req = ApiStruct.InputOrder(
                InstrumentID=code,
                OrderPriceType=ApiStruct.THOST_FTDC_OPT_LimitPrice,
                Direction=ApiStruct.THOST_FTDC_D_Sell,  # 开空仓
                CombOffsetFlag=ApiStruct.THOST_FTDC_OF_Open,
                CombHedgeFlag=ApiStruct.THOST_FTDC_HF_Speculation,
                LimitPrice=tick[code].last,
                VolumeTotalOriginal=pos.amount,  # 锁仓数量等于持仓数量
                TimeCondition=ApiStruct.THOST_FTDC_TC_GFD,
                VolumeCondition=ApiStruct.THOST_FTDC_VC_AV,
                MinVolume=1,
                ContingentCondition=ApiStruct.THOST_FTDC_CC_Immediately,
                OrderRef=str(self.trader_api.order_ref),
            )
            direction = "sell"
        else:
            # 持有空仓，开多仓锁仓
            self.trader_api.order_ref += 1
            req = ApiStruct.InputOrder(
                InstrumentID=code,
                OrderPriceType=ApiStruct.THOST_FTDC_OPT_LimitPrice,
                Direction=ApiStruct.THOST_FTDC_D_Buy,  # 开多仓
                CombOffsetFlag=ApiStruct.THOST_FTDC_OF_Open,
                CombHedgeFlag=ApiStruct.THOST_FTDC_HF_Speculation,
                LimitPrice=tick[code].last,
                VolumeTotalOriginal=pos.amount,  # 锁仓数量等于持仓数量
                TimeCondition=ApiStruct.THOST_FTDC_TC_GFD,
                VolumeCondition=ApiStruct.THOST_FTDC_VC_AV,
                MinVolume=1,
                ContingentCondition=ApiStruct.THOST_FTDC_CC_Immediately,
                OrderRef=str(self.trader_api.order_ref),
            )
            direction = "buy"

        result = self.trader_api.ReqOrderInsert(req, 0)
        if result != 0:
            return False

        time.sleep(1)
        order = self.trader_api.orders.get(str(self.trader_api.order_ref))
        if not order:
            return False

        db.order_save(
            "futures",
            code,
            code,
            direction,
            tick[code].last,
            pos.amount,
            f"锁仓:{opt.msg}",
            datetime.now(),
        )

        msg = f"期货锁仓 {code} 方向:{direction} 价格:{tick[code].last} 数量:{pos.amount} 原因:{opt.msg}"
        utils.send_fs_msg("futures_trader", "期货交易提醒", [msg])

        return {"price": tick[code].last, "amount": pos.amount}

    def force_close(self, code: str, pos: POSITION, opt: Operation):
        """强制平仓
        使用对手价强平，提高成交概率
        """
        tick = self.ex.ticks([code])
        if code not in tick:
            return False

        self.trader_api.order_ref += 1

        # 根据持仓方向决定平仓方向和价格
        if pos.direction == "buy":
            # 平多仓，用卖一价
            direction = THOST_FTDC_D_Sell
            price = tick[code].sell1
        else:
            # 平空仓，用买一价
            direction = THOST_FTDC_D_Buy
            price = tick[code].buy1

        req = CThostFtdcInputOrderField()
        req.InstrumentID = code
        req.OrderPriceType = THOST_FTDC_OPT_LimitPrice
        req.Direction = direction
        req.CombOffsetFlag = THOST_FTDC_OF_Close
        req.CombHedgeFlag = THOST_FTDC_HF_Speculation
        req.LimitPrice = price
        req.VolumeTotalOriginal = pos.amount
        req.TimeCondition = THOST_FTDC_TC_GFD
        req.VolumeCondition = THOST_FTDC_VC_AV
        req.MinVolume = 1
        req.ContingentCondition = THOST_FTDC_CC_Immediately
        req.OrderRef = str(self.trader_api.order_ref)

        result = self.trader_api.ReqOrderInsert(req, 0)
        if result != 0:
            return False

        time.sleep(1)
        order = self.trader_api.orders.get(str(self.trader_api.order_ref))
        if not order:
            return False

        direction_str = "sell" if direction == THOST_FTDC_D_Sell else "buy"
        db.order_save(
            "futures",
            code,
            code,
            direction_str,
            price,
            pos.amount,
            f"强平:{opt.msg}",
            datetime.now(),
        )

        msg = f"期货强平 {code} 方向:{direction_str} 价格:{price} 数量:{pos.amount} 原因:{opt.msg}"
        utils.send_fs_msg("futures_trader", "期货交易提醒", [msg])

        return {"price": price, "amount": pos.amount}

    def force_close_all(self, opt: Operation):
        """
        强制平掉所有持仓

        示例：
        #强平单个持仓
        opt = Operation(code, "close", "risk", 0, {}, "风控强平")
        trader.force_close(code, position, opt)

        # 强平所有持仓
        opt = Operation("ALL", "close", "risk", 0, {}, "风控全部强平")
        trader.force_close_all(opt)
        """
        # 查询所有持仓
        req = CThostFtdcQryInvestorPositionField()
        req.BrokerID = self.ex.broker_id
        req.InvestorID = self.ex.user_id
        self.trader_api.ReqQryInvestorPosition(req, 0)
        time.sleep(1)

        results = []
        for key, pos_info in self.trader_api.positions.items():
            code = pos_info.InstrumentID
            direction = "buy" if pos_info.PosiDirection == "2" else "sell"
            amount = pos_info.Position

            pos = POSITION(
                code=code, direction=direction, price=pos_info.OpenPrice, amount=amount
            )

            result = self.force_close(code, pos, opt)
            if result:
                results.append(result)
            time.sleep(0.1)  # 避免请求太快

        return results

    def close_sell(self, code, pos: POSITION, opt):
        """平空仓"""
        tick = self.ex.ticks([code])
        if code not in tick:
            return False

        self.trader_api.order_ref += 1
        req = ApiStruct.InputOrder(
            InstrumentID=code,
            OrderPriceType=ApiStruct.THOST_FTDC_OPT_LimitPrice,
            Direction=ApiStruct.THOST_FTDC_D_Buy,  # 买入平仓
            CombOffsetFlag=ApiStruct.THOST_FTDC_OF_Close,  # 平仓
            CombHedgeFlag=ApiStruct.THOST_FTDC_HF_Speculation,
            LimitPrice=tick[code].last,
            VolumeTotalOriginal=pos.amount,
            TimeCondition=ApiStruct.THOST_FTDC_TC_GFD,
            VolumeCondition=ApiStruct.THOST_FTDC_VC_AV,
            MinVolume=1,
            ContingentCondition=ApiStruct.THOST_FTDC_CC_Immediately,
            OrderRef=str(self.trader_api.order_ref),
        )

        result = self.trader_api.ReqOrderInsert(req, 0)
        if result != 0:
            return False

        time.sleep(1)
        order = self.trader_api.orders.get(str(self.trader_api.order_ref))
        if not order:
            return False

        db.order_save(
            "futures",
            code,
            code,
            "buy",
            tick[code].last,
            pos.amount,
            opt.msg,
            datetime.now(),
        )

        msg = f"期货平空 {code} 价格 {tick[code].last} 数量 {pos.amount} 原因 {opt.msg}"
        utils.send_fs_msg("futures_trader", "期货交易提醒", [msg])

        return {"price": tick[code].last, "amount": pos.amount}

    def lock_position(self, code: str, pos: POSITION, opt: Operation):
        """锁仓操作
        当持有多仓时开等量空仓，或持有空仓时开等量多仓
        """
        tick = self.ex.ticks([code])
        if code not in tick:
            return False

        # 查询当前持仓
        self.trader_api.ReqQryInvestorPosition(
            ApiStruct.QryInvestorPosition(
                BrokerID=self.ex.broker_id,
                InvestorID=self.ex.user_id,
                InstrumentID=code,
            ),
            0,
        )
        time.sleep(1)

        # 根据持仓方向决定锁仓方向
        if pos.direction == "buy":
            # 持有多仓，开空仓锁仓
            self.trader_api.order_ref += 1
            req = ApiStruct.InputOrder(
                InstrumentID=code,
                OrderPriceType=ApiStruct.THOST_FTDC_OPT_LimitPrice,
                Direction=ApiStruct.THOST_FTDC_D_Sell,  # 开空仓
                CombOffsetFlag=ApiStruct.THOST_FTDC_OF_Open,
                CombHedgeFlag=ApiStruct.THOST_FTDC_HF_Speculation,
                LimitPrice=tick[code].last,
                VolumeTotalOriginal=pos.amount,  # 锁仓数量等于持仓数量
                TimeCondition=ApiStruct.THOST_FTDC_TC_GFD,
                VolumeCondition=ApiStruct.THOST_FTDC_VC_AV,
                MinVolume=1,
                ContingentCondition=ApiStruct.THOST_FTDC_CC_Immediately,
                OrderRef=str(self.trader_api.order_ref),
            )
            direction = "sell"
        else:
            # 持有空仓，开多仓锁仓
            self.trader_api.order_ref += 1
            req = ApiStruct.InputOrder(
                InstrumentID=code,
                OrderPriceType=ApiStruct.THOST_FTDC_OPT_LimitPrice,
                Direction=ApiStruct.THOST_FTDC_D_Buy,  # 开多仓
                CombOffsetFlag=ApiStruct.THOST_FTDC_OF_Open,
                CombHedgeFlag=ApiStruct.THOST_FTDC_HF_Speculation,
                LimitPrice=tick[code].last,
                VolumeTotalOriginal=pos.amount,  # 锁仓数量等于持仓数量
                TimeCondition=ApiStruct.THOST_FTDC_TC_GFD,
                VolumeCondition=ApiStruct.THOST_FTDC_VC_AV,
                MinVolume=1,
                ContingentCondition=ApiStruct.THOST_FTDC_CC_Immediately,
                OrderRef=str(self.trader_api.order_ref),
            )
            direction = "buy"

        result = self.trader_api.ReqOrderInsert(req, 0)
        if result != 0:
            return False

        time.sleep(1)
        order = self.trader_api.orders.get(str(self.trader_api.order_ref))
        if not order:
            return False

        db.order_save(
            "futures",
            code,
            code,
            direction,
            tick[code].last,
            pos.amount,
            f"锁仓:{opt.msg}",
            datetime.now(),
        )

        msg = f"期货锁仓 {code} 方向:{direction} 价格:{tick[code].last} 数量:{pos.amount} 原因:{opt.msg}"
        utils.send_fs_msg("futures_trader", "期货交易提醒", [msg])

        return {"price": tick[code].last, "amount": pos.amount}

    def confirm_settlement(self):
        """确认结算单"""
        req = CThostFtdcSettlementInfoConfirmField()
        req.BrokerID = self.ex.broker_id
        req.InvestorID = self.ex.user_id
        self.trader_api.ReqSettlementInfoConfirm(req, self.trader_api.order_ref)

    def query_instrument(self, code=""):
        """查询合约"""
        req = CThostFtdcQryInstrumentField()
        if code:
            req.InstrumentID = code
        self.trader_api.ReqQryInstrument(req, self.trader_api.order_ref)

    def cancel_order(self, order_ref: str):
        """撤单"""
        if order_ref not in self.trader_api.orders:
            return False

        order = self.trader_api.orders[order_ref]
        req = CThostFtdcInputOrderActionField()
        req.InstrumentID = order.InstrumentID
        req.OrderRef = order_ref
        req.FrontID = self.trader_api.front_id
        req.SessionID = self.trader_api.session_id
        req.ActionFlag = THOST_FTDC_AF_Delete
        req.BrokerID = self.ex.broker_id
        req.InvestorID = self.ex.user_id

        return self.trader_api.ReqOrderAction(req, self.trader_api.order_ref) == 0

    def OnRspSettlementInfoConfirm(
        self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast
    ):
        """结算单确认响应"""
        if pRspInfo and pRspInfo.ErrorID == 0:
            print("结算单确认成功")
        else:
            print(f"结算单确认失败：{pRspInfo.ErrorMsg if pRspInfo else '未知错误'}")

    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        """合约查询响应"""
        if pInstrument:
            instrument_info = {
                "code": pInstrument.InstrumentID,
                "exchange_id": pInstrument.ExchangeID,
                "product_id": pInstrument.ProductID,
                "price_tick": pInstrument.PriceTick,
                "volume_multiple": pInstrument.VolumeMultiple,
                "max_market_order_volume": pInstrument.MaxMarketOrderVolume,
                "min_market_order_volume": pInstrument.MinMarketOrderVolume,
            }
            print(f"合约信息: {instrument_info}")

    def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast):
        """报单录入请求响应"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            print(f"报单失败：{pRspInfo.ErrorMsg}")

    def OnErrRtnOrderInsert(self, pInputOrder, pRspInfo):
        """报单录入错误回报"""
        if pRspInfo:
            print(f"报单错误：{pRspInfo.ErrorMsg}")

    def OnRspOrderAction(self, pInputOrderAction, pRspInfo, nRequestID, bIsLast):
        """报单操作请求响应"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            print(f"撤单失败：{pRspInfo.ErrorMsg}")

    def OnErrRtnOrderAction(self, pOrderAction, pRspInfo):
        """报单操作错误回报"""
        if pRspInfo:
            print(f"撤单错误：{pRspInfo.ErrorMsg}")

    def query_trading_account(self):
        """查询资金账户"""
        req = CThostFtdcQryTradingAccountField()
        req.BrokerID = self.ex.broker_id
        req.InvestorID = self.ex.user_id
        self.trader_api.ReqQryTradingAccount(req, self.trader_api.order_ref)

    def query_orders(self, code=""):
        """查询委托"""
        req = CThostFtdcQryOrderField()
        req.BrokerID = self.ex.broker_id
        req.InvestorID = self.ex.user_id
        if code:
            req.InstrumentID = code
        self.trader_api.ReqQryOrder(req, self.trader_api.order_ref)

    def query_trades(self, code=""):
        """查询成交"""
        req = CThostFtdcQryTradeField()
        req.BrokerID = self.ex.broker_id
        req.InvestorID = self.ex.user_id
        if code:
            req.InstrumentID = code
        self.trader_api.ReqQryTrade(req, self.trader_api.order_ref)

    def get_position(self, code: str) -> Dict:
        """获取单个合约的持仓"""
        positions = {}
        for key, pos in self.trader_api.positions.items():
            if pos.InstrumentID == code:
                direction = "buy" if pos.PosiDirection == "2" else "sell"
                positions[direction] = {
                    "code": pos.InstrumentID,
                    "direction": direction,
                    "volume": pos.Position,
                    "price": pos.OpenPrice,
                    "margin": pos.UseMargin,
                    "profit": pos.PositionProfit,
                }
        return positions

    def get_all_positions(self) -> Dict:
        """获取所有持仓"""
        positions = {}
        for key, pos in self.trader_api.positions.items():
            code = pos.InstrumentID
            direction = "buy" if pos.PosiDirection == "2" else "sell"
            if code not in positions:
                positions[code] = {}
            positions[code][direction] = {
                "code": code,
                "direction": direction,
                "volume": pos.Position,
                "price": pos.OpenPrice,
                "margin": pos.UseMargin,
                "profit": pos.PositionProfit,
            }
        return positions


if __name__ == "__main__":
    trader = CTPTrader("ctp_trader")

    try:
        # 确认结算单
        trader.confirm_settlement()

        # 查询账户资金
        trader.query_trading_account()
        time.sleep(1)

        # 查询持仓
        code = "rb2401"
        positions = trader.get_position(code)
        print(f"持仓信息: {positions}")

        # 开仓测试
        opt = Operation(code, "buy", "test", 0, {}, "测试买入")
        trade_res = trader.open_buy(code, opt, 1)
        print(f"开仓结果: {trade_res}")

        # 等待成交
        time.sleep(5)

        # 查询委托和成交
        trader.query_orders(code)
        trader.query_trades(code)

    except Exception as e:
        print(f"发生错误: {str(e)}")
    finally:
        trader.close()
