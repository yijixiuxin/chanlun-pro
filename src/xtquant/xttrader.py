#coding=utf-8

from . import xtpythonclient as _XTQC_
from . import xttype as _XTTYPE_
from . import xtbson as bson
from . import xtconstant as _XTCONST_

def title(s = None):
    import inspect
    if not s:
        s = inspect.stack()[1].function
    print('-' * 33 + s + '-' * 33)
    return

def cp(s = None):
    import inspect
    st = inspect.stack()
    pos = {'title':st[1].function, 'line':st[1].lineno}
    print('-' * 33 + f'{pos}, {s}' + '-' * 33)
    return

# 交易回调类
class XtQuantTraderCallback(object):
    def on_connected(self):
        """
        连接成功推送
        """
        pass
        
    def on_disconnected(self):
        """
        连接断开推送
        """
        pass

    def on_account_status(self, status):
        """
        :param status: XtAccountStatus对象
        :return:
        """
        pass

    def on_stock_asset(self, asset):
        """
        :param asset: XtAsset对象
        :return:
        """
        pass

    def on_stock_order(self, order):
        """
        :param order: XtOrder对象
        :return:
        """
        pass

    def on_stock_trade(self, trade):
        """
        :param trade: XtTrade对象
        :return:
        """
        pass

    def on_stock_position(self, position):
        """
        :param position: XtPosition对象
        :return:
        """
        pass

    def on_order_error(self, order_error):
        """
        :param order_error: XtOrderError 对象
        :return:
        """
        pass

    def on_cancel_error(self, cancel_error):
        """
        :param cancel_error:XtCancelError 对象
        :return:
        """
        pass

    def on_order_stock_async_response(self, response):
        """
        :param response: XtOrderResponse 对象
        :return:
        """
        pass
    
    def on_cancel_order_stock_async_response(self, response):
        """
        :param response: XtCancelOrderResponse 对象
        :return:
        """
        pass

    def on_smt_appointment_async_response(self, response):
        """
        :param response: XtSmtAppointmentResponse 对象
        :return:
        """
        pass
    
    def on_bank_transfer_async_response(self, response):
        """
        :param response: XtBankTransferResponse 对象
        :return:
        """
        pass

    def on_ctp_internal_transfer_async_response(self, response):
        """
        :param response: XtBankTransferResponse 对象
        :return:
        """
        pass

class XtQuantTrader(object):
    def __init__(self, path, session, callback=None):
        """
        :param path: mini版迅投极速交易客户端安装路径下，userdata文件夹具体路径
        :param session: 当前任务执行所属的会话id
        :param callback: 回调方法
        """
        import asyncio
        from threading import current_thread

        self.async_client = _XTQC_.XtQuantAsyncClient(path.encode('gb18030'), 'xtquant', session)
        self.callback = callback

        self.connected = False

        self.loop = asyncio.new_event_loop()
        if "MainThread" == current_thread().name:
            self.oldloop = asyncio.get_event_loop()
        asyncio.set_event_loop(self.loop)
        self.cbs = {}

        self.executor = None
        self.resp_executor = None

        self.relaxed_resp_order_enabled = False
        self.relaxed_resp_executor = None

        self.queuing_order_seq = set() # 发起委托的seq,获取resp时移除
        self.handled_async_order_stock_order_id = set() # 已处理了返回的委托order_id
        self.queuing_order_errors_byseq = {} # 队列中的委托失败信息，在对应委托尚未返回(检测seq或者order_id)时存入，等待回调error_callback
        self.queuing_order_errors_byid = {}

        self.handled_async_cancel_order_stock_order_id = set()
        self.handled_async_cancel_order_stock_order_sys_id = set()
        self.queuing_cancel_errors_by_order_id = {}
        self.queuing_cancel_errors_by_order_sys_id = {}
        
        
    #########################
        #push
        def on_common_push_callback_wrapper(argc, callback):
            if argc == 0:
                def on_push_data():
                    self.executor.submit(callback)
                return on_push_data
            elif argc == 1:
                def on_push_data(data):
                    self.executor.submit(callback, data)
                return on_push_data
            elif argc == 2:
                def on_push_data(data1, data2):
                    self.executor.submit(callback, data1, data2)
                return on_push_data
            else:
                return None
        
        #response
        def on_common_resp_callback(seq, resp):
            callback = self.cbs.pop(seq, None)
            if callback:
                self.resp_executor.submit(callback, resp)
            return
        
        self.async_client.bindOnSubscribeRespCallback(on_common_resp_callback)
        self.async_client.bindOnUnsubscribeRespCallback(on_common_resp_callback)
        self.async_client.bindOnQueryStockAssetCallback(on_common_resp_callback)
        self.async_client.bindOnQueryStockOrdersCallback(on_common_resp_callback)
        self.async_client.bindOnQueryStockTradesCallback(on_common_resp_callback)
        self.async_client.bindOnQueryStockPositionsCallback(on_common_resp_callback)
        self.async_client.bindOnQueryCreditDetailRespCallback(on_common_resp_callback)
        self.async_client.bindOnQueryStkCompactsRespCallback(on_common_resp_callback)
        self.async_client.bindOnQueryCreditSubjectsRespCallback(on_common_resp_callback)
        self.async_client.bindOnQueryCreditSloCodeRespCallback(on_common_resp_callback)
        self.async_client.bindOnQueryCreditAssureRespCallback(on_common_resp_callback)
        self.async_client.bindOnQueryNewPurchaseLimitCallback(on_common_resp_callback)
        self.async_client.bindOnQueryIPODataCallback(on_common_resp_callback)
        self.async_client.bindOnTransferRespCallback(on_common_resp_callback)
        self.async_client.bindOnQueryComFundRespCallback(on_common_resp_callback)
        self.async_client.bindOnSmtQueryQuoterRespCallback(on_common_resp_callback)
        self.async_client.bindOnSmtQueryOrderRespCallback(on_common_resp_callback)
        self.async_client.bindOnSmtQueryCompactRespCallback(on_common_resp_callback)
        self.async_client.bindOnQueryPositionStatisticsRespCallback(on_common_resp_callback)
        self.async_client.bindOnExportDataRespCallback(on_common_resp_callback)
        self.async_client.bindOnSyncTransactionFromExternalRespCallback(on_common_resp_callback)
        self.async_client.bindOnBankTransferRespCallback(on_common_resp_callback)
        self.async_client.bindOnQueryBankInfoRespCallback(on_common_resp_callback)
        self.async_client.bindOnQueryBankAmountRespCallback(on_common_resp_callback)
        self.async_client.bindOnQueryBankTransferStreamRespCallback(on_common_resp_callback)
        self.async_client.bindOnQuerySecuAccountRespCallback(on_common_resp_callback)
        self.async_client.bindOnCtpInternalTransferRespCallback(on_common_resp_callback)
     
        self.async_client.bindOnQueryAccountInfosCallback(on_common_resp_callback)
        self.async_client.bindOnQueryAccountStatusCallback(on_common_resp_callback)
    #########################
        
        enable_push = 1
        
        #order push
        
        def on_push_OrderStockAsyncResponse(seq, resp):
            callback = self.cbs.pop(seq, None)
            if callback:
                resp = _XTTYPE_.XtOrderResponse(resp.m_strAccountID, resp.m_nOrderID, resp.m_strStrategyName, resp.m_strOrderRemark, resp.m_strErrorMsg, seq)
                callback(resp)
                self.queuing_order_seq.discard(seq)
                e = self.queuing_order_errors_byseq.pop(seq, None)
                if not e:
                    e = self.queuing_order_errors_byid.pop(resp.order_id, None)
                if e is not None:
                    self.callback.on_order_error(e)
                else:
                    self.handled_async_order_stock_order_id.add(resp.order_id)
            return
        
        if enable_push:
            self.async_client.bindOnOrderStockRespCallback(on_common_push_callback_wrapper(2, on_push_OrderStockAsyncResponse))
        
        def on_push_CancelOrderStockAsyncResponse(seq, resp):
            callback = self.cbs.pop(seq, None)
            if callback:
                resp = _XTTYPE_.XtCancelOrderResponse(resp.m_strAccountID, resp.m_nCancelResult, resp.m_nOrderID, resp.m_strOrderSysID, seq, resp.m_strErrorMsg)
                callback(resp)
                
                if not resp.order_sysid:
                    e = self.queuing_cancel_errors_by_order_id.pop(resp.order_id, None)
                    if e is not None:
                        self.handled_async_cancel_order_stock_order_id.discard(resp.order_id)
                        self.callback.on_cancel_error(e)
                    else:
                        self.handled_async_cancel_order_stock_order_id.add(resp.order_id)
                else:
                    e = self.queuing_cancel_errors_by_order_sys_id.pop(resp.order_sysid, None)
                    if e is not None:
                        self.handled_async_cancel_order_stock_order_sys_id.discard(resp.order_sysid)
                        self.callback.on_cancel_error(e)
                    else:
                        self.handled_async_cancel_order_stock_order_sys_id.add(resp.order_sysid)
            return
        
        if enable_push:
            self.async_client.bindOnCancelOrderStockRespCallback(on_common_push_callback_wrapper(2, on_push_CancelOrderStockAsyncResponse))
            
        def on_push_disconnected():
            if self.callback:
                self.callback.on_disconnected()

        if enable_push:
            self.async_client.bindOnDisconnectedCallback(on_common_push_callback_wrapper(0, on_push_disconnected))

        def on_push_AccountStatus(data):
            data = _XTTYPE_.XtAccountStatus(data.m_strAccountID, data.m_nAccountType, data.m_nStatus)
            self.callback.on_account_status(data)

        if enable_push:
            self.async_client.bindOnUpdateAccountStatusCallback(on_common_push_callback_wrapper(1, on_push_AccountStatus))

        def on_push_StockAsset(data):
            self.callback.on_stock_asset(data)

        if enable_push:
            self.async_client.bindOnStockAssetCallback(on_common_push_callback_wrapper(1, on_push_StockAsset))

        def on_push_OrderStock(data):
            self.callback.on_stock_order(data)

        if enable_push:
            self.async_client.bindOnStockOrderCallback(on_common_push_callback_wrapper(1, on_push_OrderStock))

        def on_push_StockTrade(data):
            self.callback.on_stock_trade(data)

        if enable_push:
            self.async_client.bindOnStockTradeCallback(on_common_push_callback_wrapper(1, on_push_StockTrade))

        def on_push_StockPosition(data):
            self.callback.on_stock_position(data)

        if enable_push:
            self.async_client.bindOnStockPositionCallback(on_common_push_callback_wrapper(1, on_push_StockPosition))

        def on_push_OrderError(data):
            if data.seq not in self.queuing_order_seq or data.order_id in self.handled_async_order_stock_order_id:
                self.handled_async_order_stock_order_id.discard(data.order_id)
                self.callback.on_order_error(data)
            else:
                self.queuing_order_errors_byseq[data.seq] = data
                self.queuing_order_errors_byid[data.order_id] = data

        if enable_push:
            self.async_client.bindOnOrderErrorCallback(on_common_push_callback_wrapper(1, on_push_OrderError))

        def on_push_CancelError(data):
            if data.order_id in self.handled_async_cancel_order_stock_order_id:
                self.handled_async_cancel_order_stock_order_id.discard(data.order_id)
                self.callback.on_cancel_error(data)      
            elif data.order_sysid in self.handled_async_cancel_order_stock_order_sys_id:
                self.handled_async_cancel_order_stock_order_sys_id.discard(data.order_sysid)
                self.callback.on_cancel_error(data)
            else:
                self.queuing_cancel_errors_by_order_id[data.order_id] = data
                self.queuing_cancel_errors_by_order_sys_id[data.order_sysid] = data

        if enable_push:
            self.async_client.bindOnCancelErrorCallback(on_common_push_callback_wrapper(1, on_push_CancelError))
        
        def on_push_SmtAppointmentAsyncResponse(seq, resp):
            callback = self.cbs.pop(seq, None)
            if callback:
                resp = _XTTYPE_.XtSmtAppointmentResponse(seq, resp.m_bSuccess, resp.m_strMsg, resp.m_strApplyID)
                callback(resp)
            return
        
        if enable_push:
            self.async_client.bindOnSmtAppointmentRespCallback(on_common_push_callback_wrapper(2, on_push_SmtAppointmentAsyncResponse))
   
        def on_push_bankTransferAsyncResponse(seq, resp):
            callback = self.cbs.pop(seq, None)
            if callback:
                resp = _XTTYPE_.XtBankTransferResponse(seq, resp.success, resp.error_msg)
                callback(resp)
            return
        
        if enable_push:
            self.async_client.bindOnBankTransferRespCallback(on_common_push_callback_wrapper(2, on_push_bankTransferAsyncResponse))

        def on_push_ctpInternalTransferAsyncResponse(seq, resp):
            callback = self.cbs.pop(seq, None)
            if callback:
                resp = _XTTYPE_.XtBankTransferResponse(seq, resp.success, resp.error_msg)
                callback(resp)
            return
      
        if enable_push:
            self.async_client.bindOnCtpInternalTransferRespCallback(on_common_push_callback_wrapper(2, on_push_ctpInternalTransferAsyncResponse))

    ########################

    def common_op_async_with_seq(self, seq, callable, callback):
        self.cbs[seq] = callback

        def apply(func, *args):
            return func(*args)
        apply(*callable)

        return seq

    def set_timeout(self, timeout=0):
        self.async_client.setTimeout(timeout)

    def common_op_sync_with_seq(self, seq, callable):
        from concurrent.futures import Future
        future = Future()
        self.cbs[seq] = lambda resp:future.set_result(resp)

        def apply(func, *args):
            return func(*args)
        apply(*callable)

        return future.result()

    #########################
    
    
    def __del__(self):
        import asyncio
        from threading import current_thread
        if "MainThread" == current_thread().name:
            asyncio.set_event_loop(self.oldloop)

    def register_callback(self, callback):
        self.callback = callback

    def start(self):
        from concurrent.futures import ThreadPoolExecutor
        self.async_client.init()
        self.async_client.start()
        self.executor = ThreadPoolExecutor(max_workers = 1)
        self.relaxed_resp_executor = ThreadPoolExecutor(max_workers = 1)
        self.resp_executor = self.relaxed_resp_executor if self.relaxed_resp_order_enabled else self.executor
        return

    def stop(self):
        self.async_client.stop()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.executor.shutdown(wait = True)
        self.relaxed_resp_executor.shutdown(wait = True)
        return

    def connect(self):
        result = self.async_client.connect()
        self.connected = result == 0
        return result

    def sleep(self, time):
        import asyncio
        async def sleep_coroutine(time):
            await asyncio.sleep(time)
        asyncio.run_coroutine_threadsafe(sleep_coroutine(time), self.loop).result()

    def run_forever(self):
        import time
        while True:
            time.sleep(2)
        return

    def set_relaxed_response_order_enabled(self, enabled):
        self.relaxed_resp_order_enabled = enabled
        self.resp_executor = self.relaxed_resp_executor if self.relaxed_resp_order_enabled else self.executor
        return

    def subscribe(self, account):
        req = _XTQC_.SubscribeReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.subscribeWithSeq, seq, req)
        )

    def unsubscribe(self, account):
        req = _XTQC_.UnsubscribeReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.unsubscribeWithSeq, seq, req)
        )

    def order_stock_async(self, account, stock_code, order_type, order_volume, price_type, price, strategy_name='',
                          order_remark=''):
        """
        :param account: 证券账号
        :param stock_code: 证券代码, 例如"600000.SH"
        :param order_type: 委托类型, 23:买, 24:卖
        :param order_volume: 委托数量, 股票以'股'为单位, 债券以'张'为单位
        :param price_type: 报价类型, 详见帮助手册
        :param price: 报价价格, 如果price_type为指定价, 那price为指定的价格, 否则填0
        :param strategy_name: 策略名称
        :param order_remark: 委托备注
        :return: 返回下单请求序号, 成功委托后的下单请求序号为大于0的正整数, 如果为-1表示委托失败
        """
        req = _XTQC_.OrderStockReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_strStockCode = stock_code
        req.m_nOrderType = order_type
        req.m_nOrderVolume = int(order_volume)
        req.m_nPriceType = price_type
        req.m_dPrice = price
        req.m_strStrategyName = strategy_name
        req.m_strOrderRemark = order_remark
        req.m_strOrderRemarkNew = order_remark
        req.m_dOrderAmount = order_volume
        req.m_strStockCode1 = stock_code
        req.m_strAccountID1 = account.account_id

        seq = self.async_client.nextSeq()
        self.queuing_order_seq.add(seq)
        self.cbs[seq] = self.callback.on_order_stock_async_response
        self.async_client.orderStockWithSeq(seq, req)
        return seq

    def order_stock(self, account, stock_code, order_type, order_volume, price_type, price, strategy_name='',
                          order_remark=''):
        """
        :param account: 证券账号
        :param stock_code: 证券代码, 例如"600000.SH"
        :param order_type: 委托类型, 23:买, 24:卖
        :param order_volume: 委托数量, 股票以'股'为单位, 债券以'张'为单位
        :param price_type: 报价类型, 详见帮助手册
        :param price: 报价价格, 如果price_type为指定价, 那price为指定的价格, 否则填0
        :param strategy_name: 策略名称
        :param order_remark: 委托备注
        :return: 返回下单请求序号, 成功委托后的下单请求序号为大于0的正整数, 如果为-1表示委托失败
        """
        req = _XTQC_.OrderStockReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_strStockCode = stock_code
        req.m_nOrderType = order_type
        req.m_nOrderVolume = int(order_volume)
        req.m_nPriceType = price_type
        req.m_dPrice = price
        req.m_strStrategyName = strategy_name
        req.m_strOrderRemark = order_remark
        req.m_strOrderRemarkNew = order_remark
        req.m_dOrderAmount = order_volume
        req.m_strStockCode1 = stock_code
        req.m_strAccountID1 = account.account_id
        
        seq = self.async_client.nextSeq()
        self.queuing_order_seq.add(seq)
        resp = self.common_op_sync_with_seq(
            seq,
            (self.async_client.orderStockWithSeq, seq, req)
        )
        return resp.order_id

    def cancel_order_stock(self, account, order_id):
        """
        :param account: 证券账号
        :param order_id: 委托编号, 报单时返回的编号
        :return: 返回撤单成功或者失败, 0:成功,  -1:撤单失败
        """
        req = _XTQC_.CancelOrderStockReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_nOrderID = order_id
        
        seq = self.async_client.nextSeq()
        resp = self.common_op_sync_with_seq(
            seq,
            (self.async_client.cancelOrderStockWithSeq, seq, req)
        )
        return resp.cancel_result

    def cancel_order_stock_async(self, account, order_id):
        """
        :param account: 证券账号
        :param order_id: 委托编号, 报单时返回的编号
        :return: 返回撤单请求序号, 成功委托后的撤单请求序号为大于0的正整数, 如果为-1表示撤单失败
        """
        req = _XTQC_.CancelOrderStockReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_nOrderID = order_id
        
        seq = self.async_client.nextSeq()
        self.cbs[seq] = self.callback.on_cancel_order_stock_async_response
        self.async_client.cancelOrderStockWithSeq(seq, req)
        return seq

    def cancel_order_stock_sysid(self, account, market, sysid):
        """
        :param account:证券账号
        :param market: 交易市场 0:上海 1:深圳
        :param sysid: 柜台合同编号
        :return:返回撤单成功或者失败, 0:成功,  -1:撤单失败
        """
        req = _XTQC_.CancelOrderStockReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        if isinstance(market, str):
            req.m_strMarket = market
            req.m_nMarket = _XTCONST_.MARKET_STR_TO_ENUM_MAPPING.get(market, -1)
        else:
            req.m_nMarket = market
        req.m_strOrderSysID = sysid
        
        seq = self.async_client.nextSeq()
        resp = self.common_op_sync_with_seq(
            seq,
            (self.async_client.cancelOrderStockWithSeq, seq, req)
        )
        return resp.cancel_result

    def cancel_order_stock_sysid_async(self, account, market, sysid):
        """
        :param account:证券账号
        :param market: 交易市场 0:上海 1:深圳
        :param sysid: 柜台编号
        :return:返回撤单请求序号, 成功委托后的撤单请求序号为大于0的正整数, 如果为-1表示撤单失败
        """
        req = _XTQC_.CancelOrderStockReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        if isinstance(market, str):
            req.m_strMarket = market
            req.m_nMarket = _XTCONST_.MARKET_STR_TO_ENUM_MAPPING.get(market, -1)
        else:
            req.m_nMarket = market
        req.m_strOrderSysID = sysid
        
        seq = self.async_client.nextSeq()
        self.cbs[seq] = self.callback.on_cancel_order_stock_async_response
        self.async_client.cancelOrderStockWithSeq(seq, req)
        return seq

    def query_account_infos(self):
        """
        :return: 返回账号列表
        """
        req = _XTQC_.QueryAccountInfosReq()
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryAccountInfosWithSeq, seq, req)
        )
        
    query_account_info = query_account_infos
    
    def query_account_infos_async(self, callback):
        """
        :return: 返回账号列表
        """
        req = _XTQC_.QueryAccountInfosReq()
        
        seq = self.async_client.nextSeq()
        return self.common_op_async_with_seq(
            seq,
            (self.async_client.queryAccountInfosWithSeq, seq, req)
            , callback
        )
        
    def query_account_status(self):
        """
        :return: 返回账号状态
        """
        req = _XTQC_.QueryAccountStatusReq()
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryAccountStatusWithSeq, seq, req)
        )
    
    def query_account_status_async(self, callback):
        """
        :return: 返回账号状态
        """
        req = _XTQC_.QueryAccountStatusReq()
        
        seq = self.async_client.nextSeq()
        return self.common_op_async_with_seq(
            seq,
            (self.async_client.queryAccountStatusWithSeq, seq, req)
            , callback
        )

    def query_stock_asset(self, account):
        """
        :param account: 证券账号
        :return: 返回当前证券账号的资产数据
        """
        req = _XTQC_.QueryStockAssetReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        resp = self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryStockAssetWithSeq, seq, req)
        )

        if resp and len(resp):
            return resp[0]
        return None
    
    def query_stock_asset_async(self, account, callback):
        """
        :param account: 证券账号
        :return: 返回当前证券账号的资产数据
        """
        req = _XTQC_.QueryStockAssetReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        def _cb(resp):
            callback(resp[0] if resp else None)
        resp = self.common_op_async_with_seq(
            seq,
            (self.async_client.queryStockAssetWithSeq, seq, req)
            , _cb
        )
        return

    def query_stock_order(self, account, order_id):
        """
        :param account: 证券账号
        :param order_id:  订单编号，同步报单接口返回的编号
        :return: 返回订单编号对应的委托对象
        """
        req = _XTQC_.QueryStockOrdersReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_nOrderID = order_id
        
        seq = self.async_client.nextSeq()
        resp = self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryStockOrdersWithSeq, seq, req)
        )
        if resp and len(resp):
            return resp[0]
        return None

    def query_stock_orders(self, account, cancelable_only = False):
        """
        :param account: 证券账号
        :param cancelable_only: 仅查询可撤委托
        :return: 返回当日所有委托的委托对象组成的list
        """
        req = _XTQC_.QueryStockOrdersReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_bCanCancel = cancelable_only
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryStockOrdersWithSeq, seq, req)
        )
    
    def query_stock_orders_async(self, account, callback, cancelable_only = False):
        """
        :param account: 证券账号
        :param cancelable_only: 仅查询可撤委托
        :return: 返回当日所有委托的委托对象组成的list
        """
        req = _XTQC_.QueryStockOrdersReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_bCanCancel = cancelable_only
        
        seq = self.async_client.nextSeq()
        return self.common_op_async_with_seq(
            seq,
            (self.async_client.queryStockOrdersWithSeq, seq, req)
            , callback
        )
    
    def query_stock_trades(self, account):
        """
        :param account:  证券账号
        :return:  返回当日所有成交的成交对象组成的list
        """
        req = _XTQC_.QueryStockTradesReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryStockTradesWithSeq, seq, req)
        )
    
    def query_stock_trades_async(self, account, callback):
        """
        :param account:  证券账号
        :return:  返回当日所有成交的成交对象组成的list
        """
        req = _XTQC_.QueryStockTradesReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_async_with_seq(
            seq,
            (self.async_client.queryStockTradesWithSeq, seq, req)
            , callback
        )

    def query_stock_position(self, account, stock_code):
        """
        :param account: 证券账号
        :param stock_code: 证券代码, 例如"600000.SH"
        :return: 返回证券代码对应的持仓对象
        """
        req = _XTQC_.QueryStockPositionsReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_strStockCode = stock_code
        req.m_strStockCode1 = stock_code
        
        seq = self.async_client.nextSeq()
        resp = self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryStockPositionsWithSeq, seq, req)
        )
        if resp and len(resp):
            return resp[0]
        return None

    def query_stock_positions(self, account):
        """
        :param account: 证券账号
        :return: 返回当日所有持仓的持仓对象组成的list
        """
        req = _XTQC_.QueryStockPositionsReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryStockPositionsWithSeq, seq, req)
        )
    
    def query_stock_positions_async(self, account, callback):
        """
        :param account: 证券账号
        :return: 返回当日所有持仓的持仓对象组成的list
        """
        req = _XTQC_.QueryStockPositionsReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_async_with_seq(
            seq
            , (self.async_client.queryStockPositionsWithSeq, seq, req)
            , callback
        )

    def query_credit_detail(self, account):
        """
        :param account: 证券账号
        :return: 返回当前证券账号的资产数据
        """
        req = _XTQC_.QueryCreditDetailReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryCreditDetailWithSeq, seq, req)
        )
    
    def query_credit_detail_async(self, account, callback):
        """
        :param account: 证券账号
        :return: 返回当前证券账号的资产数据
        """
        req = _XTQC_.QueryCreditDetailReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_async_with_seq(
            seq,
            (self.async_client.queryCreditDetailWithSeq, seq, req)
            , callback
        )

    def query_stk_compacts(self, account):
        """
        :param account: 证券账号
        :return: 返回负债合约
        """
        req = _XTQC_.QueryStkCompactsReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryStkCompactsWithSeq, seq, req)
        )
    
    def query_stk_compacts_async(self, account, callback):
        """
        :param account: 证券账号
        :return: 返回负债合约
        """
        req = _XTQC_.QueryStkCompactsReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_async_with_seq(
            seq,
            (self.async_client.queryStkCompactsWithSeq, seq, req)
            , callback
        )
    
    def query_credit_subjects(self, account):
        """
        :param account: 证券账号
        :return: 返回融资融券标的
        """
        req = _XTQC_.QueryCreditSubjectsReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryCreditSubjectsWithSeq, seq, req)
        )
    
    def query_credit_subjects_async(self, account, callback):
        """
        :param account: 证券账号
        :return: 返回融资融券标的
        """
        req = _XTQC_.QueryCreditSubjectsReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_async_with_seq(
            seq,
            (self.async_client.queryCreditSubjectsWithSeq, seq, req)
            , callback
        )
    
    def query_credit_slo_code(self, account):
        """
        :param account: 证券账号
        :return: 返回可融券数据
        """
        req = _XTQC_.QueryCreditSloCodeReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryCreditSloCodeWithSeq, seq, req)
        )
    
    def query_credit_slo_code_async(self, account, callback):
        """
        :param account: 证券账号
        :return: 返回可融券数据
        """
        req = _XTQC_.QueryCreditSloCodeReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_async_with_seq(
            seq,
            (self.async_client.queryCreditSloCodeWithSeq, seq, req)
            , callback
        )
    
    def query_credit_assure(self, account):
        """
        :param account: 证券账号
        :return: 返回标的担保品
        """
        req = _XTQC_.QueryCreditAssureReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryCreditAssureWithSeq, seq, req)
        )
    
    def query_credit_assure_async(self, account, callback):
        """
        :param account: 证券账号
        :return: 返回标的担保品
        """
        req = _XTQC_.QueryCreditAssureReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_async_with_seq(
            seq,
            (self.async_client.queryCreditAssureWithSeq, seq, req)
            , callback
        )
    
    def query_new_purchase_limit(self, account):
        """
        :param account: 证券账号
        :return: 返回账户新股申购额度数据
        """
        req = _XTQC_.QueryNewPurchaseLimitReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        new_purchase_limit_list = self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryNewPurchaseLimitWithSeq, seq, req)
        )
        new_purchase_limit_result = dict()
        for item in new_purchase_limit_list:
            new_purchase_limit_result[item.m_strNewPurchaseLimitKey] = item.m_nNewPurchaseLimitValue
        return new_purchase_limit_result
        
    def query_new_purchase_limit_async(self, account, callback):
        """
        :param account: 证券账号
        :return: 返回账户新股申购额度数据
        """
        req = _XTQC_.QueryNewPurchaseLimitReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_async_with_seq(
            seq,
            (self.async_client.queryNewPurchaseLimitWithSeq, seq, req)
            , callback
        )
    
    def query_ipo_data(self):
        """
        :return: 返回新股新债信息
        """
        req = _XTQC_.QueryIPODataReq()
        req.m_strIPOType = ''
        
        seq = self.async_client.nextSeq()
        ipo_data_list = self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryIPODataWithSeq, seq, req)
        )
        ipo_data_result = dict()
        for item in ipo_data_list:
            ipo_data_result[item.m_strIPOCode] = {
                'name': item.m_strIPOName, 
                'type':  item.m_strIPOType, 
                'maxPurchaseNum':  item.m_nMaxPurchaseNum, 
                'minPurchaseNum':  item.m_nMinPurchaseNum, 
                'purchaseDate':  item.m_strPurchaseDate, 
                'issuePrice': item.m_dIssuePrice,
            }
        return ipo_data_result
        
    def query_ipo_data_async(self, callback):
        """
        :return: 返回新股新债信息
        """
        req = _XTQC_.QueryIPODataReq()
        req.m_strIPOType = ''
        
        seq = self.async_client.nextSeq()
        return self.common_op_async_with_seq(
            seq,
            (self.async_client.queryIPODataWithSeq, seq, req)
            , callback
        )
    
    def fund_transfer(self, account, transfer_direction, price):
        """
        :param account: 证券账号
        :param transfer_direction: 划拨方向
        :param price: 划拨金额
        :return: 返回划拨操作结果
        """
        req = _XTQC_.TransferParam()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_nOrderType = transfer_direction
        req.m_dPrice = price
        
        seq = self.async_client.nextSeq()
        transfer_result = self.common_op_sync_with_seq(
            seq,
            (self.async_client.transferWithSeq, seq, req)
        )
        return transfer_result.m_bSuccess, transfer_result.m_strMsg
        
    def secu_transfer(self, account, transfer_direction, stock_code, volume, transfer_type):
        """
        :param account: 证券账号
        :param transfer_direction: 划拨方向
        :param stock_code: 证券代码, 例如"SH600000"
        :param volume: 划拨数量, 股票以'股'为单位, 债券以'张'为单位
        :param transfer_type: 划拨类型
        :return: 返回划拨操作结果
        """
        req = _XTQC_.TransferParam()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_nOrderType = transfer_direction
        req.m_strStockCode = stock_code
        req.m_nOrderVolume = volume
        req.m_nCreditTransferType = transfer_type
        req.m_strStockCode1 = stock_code
        
        seq = self.async_client.nextSeq()
        transfer_result = self.common_op_sync_with_seq(
            seq,
            (self.async_client.transferWithSeq, seq, req)
        )
        return transfer_result.m_bSuccess, transfer_result.m_strMsg
        
    def query_com_fund(self, account):
        """
        :param account: 证券账号
        :return: 返回普通柜台资金信息
        """
        req = _XTQC_.QueryComFundReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        fund_list = self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryComFundWithSeq, seq, req)
        )
        result = dict()
        if fund_list[0]:
            result = {
                'success': fund_list[0].m_bSuccess,
                'error': fund_list[0].m_strMsg,
                'currentBalance': fund_list[0].m_dCurrentBalance,
                'enableBalance': fund_list[0].m_dEnableBalance, 
                'fetchBalance': fund_list[0].m_dFetchBalance,
                'interest': fund_list[0].m_dInterest,
                'assetBalance': fund_list[0].m_dAssetBalance,
                'fetchCash': fund_list[0].m_dFetchCash,
                'marketValue': fund_list[0].m_dMarketValue,
                'debt': fund_list[0].m_dDebt,
            }
        return result
    
    def query_com_position(self, account):
        """
        :param account: 证券账号
        :return: 返回普通柜台持仓信息
        """
        req = _XTQC_.QueryComPositionReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        position_list = self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryComPositionWithSeq, seq, req)
        )
        result = list()
        for item in position_list:
            result.append(
            {
                'success': item.m_bSuccess,
                'error': item.m_strMsg,
                'stockAccount': item.m_strAccountID,
                'exchangeType':  item.m_strExchangeType, 
                'stockCode': item.m_strStockCode,
                'stockName': item.m_strStockName,
                'totalAmt': item.m_dTotalAmt,
                'enableAmount': item.m_dEnableAmount,
                'lastPrice': item.m_dLastPrice,
                'costPrice': item.m_dCostPrice,
                'income': item.m_dIncome,
                'incomeRate': item.m_dIncomeRate,
                'marketValue': item.m_dMarketValue,
                'costBalance': item.m_dCostBalance,
                'bsOnTheWayVol': item.m_nBsOnTheWayVol,
                'prEnableVol': item.m_nPrEnableVol,
                'stockCode1': item.m_strStockCode1,
            })
        return result

    def smt_query_quoter(self, account):
        """
        :param account: 证券账号
        :return: 返回券源行情信息
        """
        req = _XTQC_.SmtQueryQuoterReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        quoter_list = self.common_op_sync_with_seq(
            seq,
            (self.async_client.smtQueryQuoterWithSeq, seq, req)
        )
        result = list()
        for item in quoter_list:
            result.append(
            {
                'success': item.m_bSuccess,
                'error': item.m_strMsg,
                'finType': item.m_strFinType,
                'stockType': item.m_strStockType,
                'date': item.m_nDate,
                'code': item.m_strCode,
                'codeName': item.m_strCodeName,
                'exchangeType': item.m_strExchangeType,
                'fsmpOccupedRate': item.m_dFsmpOccupedRate,
                'fineRate': item.m_dFineRate,
                'fsmpreendRate': item.m_dFsmpreendRate,
                'usedRate': item.m_dUsedRate,
                'unUusedRate': item.m_dUnUusedRate,
                'initDate': item.m_nInitDate,
                'endDate': item.m_nEndDate,
                'enableSloAmountT0': item.m_dEnableSloAmountT0,
                'enableSloAmountT3': item.m_dEnableSloAmountT3,
                'srcGroupId': item.m_strSrcGroupID,
                'applyMode': item.m_strApplyMode,
                'lowDate': item.m_nLowDate
            })
        return result

    def smt_negotiate_order_async(self, account, src_group_id, order_code, date, amount, apply_rate, dict_param={}):
        """
        :param account: 证券账号
        :param src_group_id: 来源组编号
        :param order_code: 证券代码，如'600000.SH'
        :param date: 期限天数
        :param amount: 委托数量
        :param apply_rate: 资券申请利率
        :return: 返回约券请求序号, 成功请求后的序号为大于0的正整数, 如果为-1表示请求失败
        注:
        目前有如下参数通过一个可缺省的字典传递，键名与参数名称相同
        subFareRate: 提前归还利率
        fineRate: 罚息利率
        """
        req = _XTQC_.SmtNegotiateOrderReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_strSrcGroupID = src_group_id
        req.m_strOrderCode = order_code
        req.m_nDate = date
        req.m_nAmount = amount
        req.m_dApplyRate = apply_rate
        
        if 'subFareRate' in dict_param:
            req.m_dSubFareRate = dict_param['subFareRate']
        if 'fineRate' in dict_param:
            req.m_dFineRate = dict_param['fineRate']
        
        seq = self.async_client.nextSeq()
        self.cbs[seq] = self.callback.on_smt_appointment_async_response
        self.async_client.smtNegotiateOrderWithSeq(seq, req)
        return seq

    def smt_appointment_order_async(self, account, order_code, date, amount, apply_rate):
        """
        :param account: 证券账号
        :param order_code: 证券代码，如'600000.SH'
        :param date: 期限天数
        :param amount: 委托数量
        :param apply_rate: 资券申请利率
        :return: 返回约券请求序号, 成功请求后的序号为大于0的正整数, 如果为-1表示请求失败
        """
        req = _XTQC_.SmtAppointmentOrderReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_strOrderCode = order_code
        req.m_nDate = date
        req.m_nAmount = amount
        req.m_dApplyRate = apply_rate
        
        seq = self.async_client.nextSeq()
        self.cbs[seq] = self.callback.on_smt_appointment_async_response
        self.async_client.smtAppointmentOrderWithSeq(seq, req)
        return seq

    def smt_appointment_cancel_async(self, account, apply_id):
        """
        :param account: 证券账号
        :param apply_id: 资券申请编号
        :return: 返回约券撤单请求序号, 成功请求后的序号为大于0的正整数, 如果为-1表示请求失败
        """
        req = _XTQC_.SmtAppointmentCancelReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_strApplyId = applyId
        
        seq = self.async_client.nextSeq()
        self.cbs[seq] = self.callback.on_smt_appointment_async_response
        self.async_client.smtAppointmentCancelWithSeq(seq, req)
        return seq

    def smt_query_order(self, account):
        """
        :param account: 证券账号
        :return: 返回券源行情信息
        """
        req = _XTQC_.SmtQueryOrderReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        order_list = self.common_op_sync_with_seq(
            seq,
            (self.async_client.smtQueryOrderWithSeq, seq, req)
        )
        result = list()
        for item in order_list:
            result.append(
            {
                'success': item.m_bSuccess,
                'error': item.m_strMsg,
                'initDate': item.m_nInitDate,
                'currDate': item.m_nCurrDate,
                'currTime': item.m_nCurrTime,
                'applyId': item.m_strApplyID,
                'srcGroupId': item.m_strSrcGroupID,
                'cashcompactId': item.m_strCashcompactID,
                'applyMode': item.m_strApplyMode,
                'finType': item.m_strFinType,
                'exchangeType': item.m_strExchangeType,
                'code': item.m_strCode,
                'codeName': item.m_strCodeName,
                'date': item.m_nDate,
                'applyRate': item.m_dApplyRate,
                'entrustBalance': item.m_dEntrustBalance,
                'entrustAmount': item.m_dEntrustAmount,
                'businessBalance': item.m_dBusinessBalance,
                'businessAmount': item.m_dBusinessAmount,
                'validDate': item.m_nValidDate,
                'dateClear': item.m_nDateClear,
                'entrustNo': item.m_strEntrustNo,
                'applyStatus': item.m_strApplyStatus,
                'usedRate': item.m_dUsedRate,
                'unUusedRate': item.m_dUnUusedRate,
                'comGroupId': item.m_strComGroupID
            })
        return result

    def smt_query_compact(self, account):
        """
        :param account: 证券账号
        :return: 返回券源行情信息
        """
        req = _XTQC_.SmtQueryCompactReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        compact_list = self.common_op_sync_with_seq(
            seq,
            (self.async_client.smtQueryCompactWithSeq, seq, req)
        )
        result = list()
        for item in compact_list:
            result.append(
            {
                'success': item.m_bSuccess,
                'error': item.m_strMsg,
                'createDate': item.m_nCreateDate,
                'cashcompactId': item.m_strCashcompactID,
                'oriCashcompactId': item.m_strOriCashcompactID,
                'applyId': item.m_strApplyID,
                'srcGroupId': item.m_strSrcGroupID,
                'comGroupId': item.m_strComGroupID,
                'finType': item.m_strFinType,
                'exchangeType': item.m_strExchangeType,
                'code': item.m_strCode,
                'codeName': item.m_strCodeName,
                'date': item.m_nDate,
                'beginCompacAmount': item.m_dBeginCompacAmount,
                'beginCompacBalance': item.m_dBeginCompacBalance,
                'compacAmount': item.m_dCompacAmount,
                'compacBalance': item.m_dCompacBalance,
                'returnAmount': item.m_dReturnAmount,
                'returnBalance': item.m_dReturnBalance,
                'realBuyAmount': item.m_dRealBuyAmount,
                'fsmpOccupedRate': item.m_dFsmpOccupedRate,
                'compactInterest': item.m_dCompactInterest,
                'compactFineInterest': item.m_dCompactFineInterest,
                'repaidInterest': item.m_dRepaidInterest,
                'repaidFineInterest': item.m_dRepaidFineInterest,
                'fineRate': item.m_dFineRate,
                'preendRate': item.m_dPreendRate,
                'compactType': item.m_strCompactType,
                'postponeTimes': item.m_nPostponeTimes,
                'compactStatus': item.m_strCompactStatus,
                'lastInterestDate': item.m_nLastInterestDate,
                'interestEndDate': item.m_nInterestEndDate,
                'validDate': item.m_nValidDate,
                'dateClear': item.m_nDateClear,
                'usedAmount': item.m_dUsedAmount,
                'usedBalance': item.m_dUsedBalance,
                'usedRate': item.m_dUsedRate,
                'unUusedRate': item.m_dUnUusedRate,
                'srcGroupName': item.m_strSrcGroupName,
                'repaidDate': item.m_nRepaidDate,
                'preOccupedInterest': item.m_dPreOccupedInterest,
                'compactInterestx': item.m_dCompactInterestx,
                'enPostponeAmount': item.m_dEnPostponeAmount,
                'postponeStatus': item.m_strPostponeStatus,
                'applyMode': item.m_strApplyMode
            })
        return result

    def smt_compact_renewal_async(self, account, cash_compact_id, order_code, defer_days, defer_num, apply_rate):
        """
        :param account: 证券账号
        :param cash_compact_id: 头寸合约编号
        :param order_code: 证券代码，如'600000.SH'
        :param defer_days: 申请展期天数
        :param defer_num: 申请展期数量
        :param apply_rate: 资券申请利率
        :return: 返回约券展期请求序号, 成功请求后的序号为大于0的正整数, 如果为-1表示请求失败
        """
        req = _XTQC_.SmtCompactRenewalReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_strCompactId = cash_compact_id
        req.m_strOrderCode = order_code
        req.m_nDeferDays = defer_days
        req.m_nDeferNum = defer_num
        req.m_dApplyRate = apply_rate
        
        seq = self.async_client.nextSeq()
        self.cbs[seq] = self.callback.on_smt_appointment_async_response
        self.async_client.smtCompactRenewalWithSeq(seq, req)
        return seq

    def smt_compact_return_async(self, account, src_group_id, cash_compact_id, order_code, occur_amount):
        """
        :param account: 证券账号
        :param src_group_id: 来源组编号
        :param cash_compact_id: 头寸合约编号
        :param order_code: 证券代码，如'600000.SH'
        :param occur_amount: 发生数量
        :return: 返回约券归还请求序号, 成功请求后的序号为大于0的正整数, 如果为-1表示请求失败
        """
        req = _XTQC_.SmtCompactReturnReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        req.m_strSrcGroupId = src_group_id
        req.m_strCompactId = cash_compact_id
        req.m_strOrderCode = order_code
        req.m_nOccurAmount = occur_amount
        
        seq = self.async_client.nextSeq()
        self.cbs[seq] = self.callback.on_smt_appointment_async_response
        self.async_client.smtCompactReturnWithSeq(seq, req)
        return seq
        
    def query_position_statistics(self, account):
        """
        :param account: 证券账号
        :return: 返回当日所有持仓统计的持仓对象组成的list
        """
        req = _XTQC_.QueryPositionStatisticsReq()
        req.m_nAccountType = account.account_type
        req.m_strAccountID = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryPositionStatisticsWithSeq, seq, req)
        )

    def export_data(self, account, result_path, data_type, start_time = None, end_time = None, user_param = {}):
        """
        :param account: 证券账号
        :param result_path: 导出路径，包含文件名及.csv后缀，如'C:\\Users\\Desktop\\test\\deal.csv'
        :param data_type: 数据类型，如'deal'
        :param start_time: 开始时间
        :param end_time: 结束时间
        :param user_param: 用户参数
        :return: 返回dict格式的结果反馈信息
        """
        fix_param = dict()
        fix_param['accountID'] = account.account_id
        fix_param['accountType'] = account.account_type
        fix_param['resultPath'] = result_path
        fix_param['dataType'] = data_type
        fix_param['startTime'] = start_time
        fix_param['endTime'] = end_time
        seq = self.async_client.nextSeq()
        resp = self.common_op_sync_with_seq(
            seq,
            (self.async_client.exportDataWithSeq, seq, bson.BSON.encode(fix_param), bson.BSON.encode(user_param))
        )
        import json
        result = json.loads(resp)
        return result

    def query_data(self, account, result_path, data_type, start_time = None, end_time = None, user_param = {}):
        """
        入参同export_data
        :return: 返回dict格式的数据信息
        """
        result = self.export_data(account, result_path, data_type, start_time, end_time, user_param)
        if 'error' in result.keys():
            return result
        else:
            import pandas as pd
            import os
            data = pd.read_csv(result_path)
            os.remove(result_path)
            return data

    def sync_transaction_from_external(self, operation, data_type, account, deal_list):
        """
        :param operation: 操作类型,有"UPDATE","REPLACE","ADD","DELETE"
        :param data_type: 数据类型,有"DEAL"
        :param account: 证券账号
        :param deal_list: 成交列表,每一项是Deal成交对象的参数字典,键名参考官网数据字典,大小写保持一致
        :return: 返回dict格式的结果反馈信息
        """
        fix_param = dict()
        fix_param['operation'] = operation
        fix_param['dataType'] = data_type
        fix_param['accountID'] = account.account_id
        fix_param['accountType'] = account.account_type
        bson_list = [bson.BSON.encode(it) for it in deal_list]
        seq = self.async_client.nextSeq()
        resp = self.common_op_sync_with_seq(
            seq,
            (self.async_client.syncTransactionFromExternalWithSeq, seq, bson.BSON.encode(fix_param), bson_list)
        )
        import json
        result = json.loads(resp)
        return result

    def bank_transfer_in(self, account, bank_no, bank_account, balance, bank_pwd = '', fund_pwd = ''):
        """
        :param account - StockAccount: 资金账号
        :param bank_no - str: 银行编号，可通过query_bank_info查回
        :param bank_account - str: 银行账号
        :param balance - float: 转账金额
        :param bank_pwd - str: 银行账号密码
        :param fund_pwd - str: 资金账号密码
        :return: 返回转账结果
        """
        req = _XTQC_.BankTransferReq()
        req.account_type = account.account_type
        req.account_id = account.account_id
        req.fund_pwd = str(fund_pwd)
        req.direction = 501
        req.bank_account = str(bank_account)
        req.bank_no = str(bank_no)
        req.bank_pwd = str(bank_pwd)
        req.balance = float(balance)
        
        seq = self.async_client.nextSeq()
        result = self.common_op_sync_with_seq(
            seq,
            (self.async_client.bankTransferWithSeq, seq, req)
        )
        return result.success, result.msg

    def bank_transfer_in_async(self, account, bank_no, bank_account, balance, bank_pwd = '', fund_pwd = ''):
        """
        :param account - StockAccount: 资金账号
        :param bank_no - str: 银行编号，可通过query_bank_info查回
        :param bank_account - str: 银行账号
        :param balance - float: 转账金额
        :param bank_pwd - str: 银行账号密码
        :param fund_pwd - str: 资金账号密码
        :return: 返回请求序号, 成功请求后的序号为大于0的正整数, 如果为-1表示请求失败
        """
        req = _XTQC_.BankTransferReq()
        req.account_type = account.account_type
        req.account_id = account.account_id
        req.fund_pwd = str(fund_pwd)
        req.direction = 501
        req.bank_account = str(bank_account)
        req.bank_no = str(bank_no)
        req.bank_pwd = str(bank_pwd)
        req.balance = float(balance)
        
        seq = self.async_client.nextSeq()
        self.cbs[seq] = self.callback.on_bank_transfer_async_response
        self.async_client.bankTransferWithSeq(seq, req)
        return seq

    def bank_transfer_out(self, account, bank_no, bank_account, balance, bank_pwd = '', fund_pwd = ''):
        """
        :param account - StockAccount: 资金账号
        :param bank_no - str: 银行编号，可通过query_bank_info查回
        :param bank_account - str: 银行账号
        :param balance - float: 转账金额
        :param bank_pwd - str: 银行账号密码
        :param fund_pwd - str: 资金账号密码
        :return: 返回转账结果
        """
        req = _XTQC_.BankTransferReq()
        req.account_type = account.account_type
        req.account_id = account.account_id
        req.fund_pwd = str(fund_pwd)
        req.direction = 502
        req.bank_account = str(bank_account)
        req.bank_no = str(bank_no)
        req.bank_pwd = str(bank_pwd)
        req.balance = float(balance)
        
        seq = self.async_client.nextSeq()
        result = self.common_op_sync_with_seq(
            seq,
            (self.async_client.bankTransferWithSeq, seq, req)
        )
        return result.success, result.msg

    def bank_transfer_out_async(self, account, bank_no, bank_account, balance, bank_pwd = '', fund_pwd = ''):
        """
        :param account - StockAccount: 资金账号
        :param bank_no - str: 银行编号，可通过query_bank_info查回
        :param bank_account - str: 银行账号
        :param balance - float: 转账金额
        :param bank_pwd - str: 银行账号密码
        :param fund_pwd - str: 资金账号密码
        :return: 返回请求序号, 成功请求后的序号为大于0的正整数, 如果为-1表示请求失败
        """
        req = _XTQC_.BankTransferReq()
        req.account_type = account.account_type
        req.account_id = account.account_id
        req.fund_pwd = str(fund_pwd)
        req.direction = 502
        req.bank_account = str(bank_account)
        req.bank_no = str(bank_no)
        req.bank_pwd = str(bank_pwd)
        req.balance = float(balance)
        
        seq = self.async_client.nextSeq()
        self.cbs[seq] = self.callback.on_bank_transfer_async_response
        self.async_client.bankTransferWithSeq(seq, req)
        return seq

    def query_bank_info(self, account):
        """
        :param account - StockAccount: 资金账号
        :return: 返回BankInfo结构组成的list
        """
        req = _XTQC_.QueryBankInfoReq()
        req.account_type = account.account_type
        req.account_id = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryBankInfoWithSeq, seq, req)
        )

    def query_bank_amount(self, account, bank_no, bank_account, bank_pwd):
        """
        :param account - StockAccount: 资金账号
        :param bank_no - str: 银行编号，可通过query_bank_info查回
        :param bank_account - str: 银行账号
        :param bank_pwd - str: 银行账号密码
        :return: 返回BankAmount组成的list
        """
        req = _XTQC_.QueryBankAmountReq()
        req.account_type = account.account_type
        req.account_id = account.account_id
        req.bank_no = str(bank_no)
        req.bank_account = str(bank_account)
        req.bank_pwd = str(bank_pwd)

        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryBankAmountWithSeq, seq, req)
        )

    def query_bank_transfer_stream(self, account, start_date, end_date, bank_no = '', bank_account = ''):
        """
        :param account - StockAccount: 资金账号
        :param start_date - str: 查询起始日期，如'20241125'
        :param end_date - str: 查询截至日期，如'20241129'
        :param bank_no - str: 银行编号，可通过query_bank_info查回
        :param bank_account - str: 银行账号
        :return: 返回BankTransferStream组成的list
        """
        req = _XTQC_.QueryBankTransferStreamReq()
        req.account_type = account.account_type
        req.account_id = account.account_id
        req.start_date = str(start_date)
        req.end_date = str(end_date)
        req.bank_no = str(bank_no)
        req.bank_account = str(bank_account)
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.queryBankTransferStreamWithSeq, seq, req)
        )

    def query_secu_account(self, account):
        """
        :param account - StockAccount: 资金账号
        :return: 返回SecuAccount结构组成的list
        """
        req = _XTQC_.QuerySecuAccountReq()
        req.account_type = account.account_type
        req.account_id = account.account_id
        
        seq = self.async_client.nextSeq()
        return self.common_op_sync_with_seq(
            seq,
            (self.async_client.querySecuAccountWithSeq, seq, req)
        )

    def ctp_transfer_option_to_future(self, opt_account_id, ft_account_id, balance):
        """
        :param opt_account_id - string: 期权资金账号
        :param ft_account_id - string: 期货资金账号
        :param balance - float: 转账金额
        :return: 返回内转结果
        """
        req = _XTQC_.CtpInternalTransferReq()
        req.opt_account_id = str(opt_account_id)
        req.ft_account_id = str(ft_account_id)
        req.direction = 515
        req.balance = float(balance)
        
        seq = self.async_client.nextSeq()
        result = self.common_op_sync_with_seq(
            seq,
            (self.async_client.ctpInternalTransferWithSeq, seq, req)
        )
        return result.success, result.msg

    def ctp_transfer_option_to_future_async(self, opt_account_id, ft_account_id, balance):
        """
        :param opt_account_id - string: 期权资金账号
        :param ft_account_id - string: 期货资金账号
        :param balance - float: 转账金额
        :return: 返回请求序号, 成功请求后的序号为大于0的正整数, 如果为-1表示请求失败
        """
        req = _XTQC_.CtpInternalTransferReq()
        req.opt_account_id = str(opt_account_id)
        req.ft_account_id = str(ft_account_id)
        req.direction = 515
        req.balance = float(balance)
        
        seq = self.async_client.nextSeq()
        self.cbs[seq] = self.callback.on_ctp_internal_transfer_async_response
        self.async_client.ctpInternalTransferWithSeq(seq, req)
        return seq

    def ctp_transfer_future_to_option(self, opt_account_id, ft_account_id, balance):
        """
        :param opt_account_id - string: 期权资金账号
        :param ft_account_id - string: 期货资金账号
        :param balance - float: 转账金额
        :return: 返回内转结果
        """
        req = _XTQC_.CtpInternalTransferReq()
        req.opt_account_id = str(opt_account_id)
        req.ft_account_id = str(ft_account_id)
        req.direction = 516
        req.balance = float(balance)
        
        seq = self.async_client.nextSeq()
        result = self.common_op_sync_with_seq(
            seq,
            (self.async_client.ctpInternalTransferWithSeq, seq, req)
        )
        return result.success, result.msg

    def ctp_transfer_future_to_option_async(self, opt_account_id, ft_account_id, balance):
        """
        :param opt_account_id - string: 期权资金账号
        :param ft_account_id - string: 期货资金账号
        :param balance - float: 转账金额
        :return: 返回请求序号, 成功请求后的序号为大于0的正整数, 如果为-1表示请求失败
        """
        req = _XTQC_.CtpInternalTransferReq()
        req.opt_account_id = str(opt_account_id)
        req.ft_account_id = str(ft_account_id)
        req.direction = 516
        req.balance = float(balance)
        
        seq = self.async_client.nextSeq()
        self.cbs[seq] = self.callback.on_ctp_internal_transfer_async_response
        self.async_client.ctpInternalTransferWithSeq(seq, req)
        return seq

