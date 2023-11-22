import json
import time
import traceback
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context

import ib_insync

from chanlun import config
from chanlun import rd, fun
from chanlun.exchange.exchange_ib import CmdEnum, ib_res_hkey


def run_tasks(client_id: int):
    """
    接收命令，并调用接口获取数据
    """
    log = fun.get_logger('ib_tasks.log')

    ib: ib_insync.IB = ib_insync.IB()
    ib_insync.util.allowCtrlC()

    def get_ib() -> ib_insync.IB:
        if ib.isConnected():
            return ib
        try:
            ib.connect(config.IB_HOST, config.IB_PORT, clientId=client_id, account=config.IB_ACCOUNT)
        except Exception as e:
            log.error(f'get ib connect error : {e}')
            time.sleep(10)
            return get_ib()
        return ib

    def search_stocks(search):
        """
        按照关键词搜索
        """
        stocks = get_ib().reqMatchingSymbols(search)
        res = []
        for s in stocks:
            if s.contract.currency == 'USD':
                code = get_code_by_contract(s.contract)
                if code != '':
                    res.append({'code': code, 'name': s.contract.description})
        return res

    def klines(code, durationStr, barSizeSetting, timeout):
        for i in range(2):
            contract = get_contract_by_code(code)
            bars = get_ib().reqHistoricalData(
                contract, endDateTime='',
                durationStr=durationStr,
                barSizeSetting=barSizeSetting,
                whatToShow='TRADES', useRTH=True, formatDate=1,
                timeout=timeout
            )
            klines_res = []
            for _b in bars:
                klines_res.append({
                    'code': code,
                    'date': fun.datetime_to_str(_b.date),
                    'open': _b.open, 'close': _b.close, 'high': _b.high, 'low': _b.low, 'volume': _b.volume
                })
            if len(klines_res) == 0:
                continue
            return klines_res
        return []

    def ticks(codes):
        contracts = [get_contract_by_code(code) for code in codes]
        tks = get_ib().reqTickers(*contracts)
        res = []
        for tk in tks:
            if tk is None or tk.last != tk.last:
                continue
            res.append({
                'code': get_code_by_contract(tk.contract), 'last': tk.last, 'buy1': tk.bid, 'sell1': tk.ask,
                'open': tk.open, 'high': tk.high, 'low': tk.low, 'volume': tk.volume,
                'rate': round((tk.last - tk.close) / tk.close * 100, 2)
            })
        return res

    def stock_info(code):
        contract = get_contract_by_code(code)
        details = get_ib().reqContractDetails(contract)
        if len(details) == 0:
            return None
        return {'code': code, 'name': details[0].longName}

    def balance():
        account = get_ib().accountSummary(account=config.IB_ACCOUNT)
        # Demo
        # {'AccruedCash': '561.00', 'AvailableFunds': '1000561.00', 'BuyingPower': '4002244.00',
        # 'EquityWithLoanValue': '1000561.00', 'ExcessLiquidity': '1000561.00', 'FullAvailableFunds': '1000561.00',
        # 'FullExcessLiquidity': '1000561.00', 'FullInitMarginReq': '0.00', 'FullMaintMarginReq': '0.00',
        # 'GrossPositionValue': '0.00', 'InitMarginReq': '0.00', 'LookAheadAvailableFunds': '1000561.00',
        # 'LookAheadExcessLiquidity': '1000561.00', 'LookAheadInitMarginReq': '0.00', 'LookAheadMaintMarginReq': '0.00',
        # 'MaintMarginReq': '0.00', 'NetLiquidation': '1000561.00', 'SMA': '1000561.00', 'TotalCashValue': '1000000.00'}
        info = {_a.tag: float(_a.value) for _a in account if _a.currency == 'USD'}
        return info

    def positions(code: str = ''):
        hold_positions = get_ib().positions(account=config.IB_ACCOUNT)
        hold_positions = [{
            'code': get_code_by_contract(_p.contract), 'account': _p.account,
            'avgCost': _p.avgCost, 'position': _p.position,
        } for _p in hold_positions]

        if code != '':
            for _p in hold_positions:
                if _p['code'] == code:
                    return _p
            return None
        return hold_positions

    def orders(code, type, amount):
        contract = get_contract_by_code(code)
        if type == 'buy':
            req_order = ib_insync.MarketOrder('BUY', amount)
        else:
            req_order = ib_insync.MarketOrder('SELL', amount)

        trade = get_ib().placeOrder(contract, req_order)
        while True:
            get_ib().sleep(1)
            if trade.isDone():
                break
        return {'price': trade.orderStatus.avgFillPrice, 'amount': trade.orderStatus.filled}

    def get_contract_by_code(code: str):
        """
        获取合约对象
        """
        if '_IND_' in code:
            return ib_insync.Index(symbol=code.split('_')[0], exchange=code.split('_')[2], currency='USD')
        elif '_FUT_' in code:
            return ib_insync.Future(symbol=code.split('_')[0], exchange=code.split('_')[2], currency='USD')
        elif '_CRYPTO_' in code:
            return ib_insync.Crypto(symbol=code.split('_')[0], exchange=code.split('_')[2], currency='USD')
        else:
            # 读取代码所属交易所信息，在合约中添加
            contract = ib_insync.Stock(symbol=code, exchange='SMART', currency='USD')
            details = get_ib().reqContractDetails(contract)
            if len(details) > 0:
                for d in details:
                    if d.contract.currency == 'USD':
                        contract.primaryExchange = d.contract.primaryExchange
                        break
            return contract

    def get_code_by_contract(contract: ib_insync.Contract):
        if contract.secType == 'STK':
            return contract.symbol
        elif contract.secType == 'IND':  # 指数添加 .IND 后缀
            return f'{contract.symbol}_IND_{contract.primaryExchange}'
        elif contract.secType == 'FUT':  # 期货添加 .FUT 后缀
            return f'{contract.symbol}_FUT_{contract.primaryExchange}'
        elif contract.secType == 'CRYPTO':  # 加密货币添加 .CRYPTO 后缀
            return f'{contract.symbol}_CRYPTO_{contract.primaryExchange}'
        return ''

    while True:
        cmd: str = ''
        args: str = ''
        res = None
        try:
            cmd, args = rd.Robj().blpop([
                CmdEnum.SEARCH_STOCKS.value,
                CmdEnum.KLINES.value,
                CmdEnum.TICKS.value,
                CmdEnum.STOCK_INFO.value,
                CmdEnum.BALANCE.value,
                CmdEnum.POSITIONS.value,
                CmdEnum.ORDERS.value,
            ], 0)
            s_time = time.time()
            args: dict = json.loads(args)
            info = ''
            if cmd == CmdEnum.SEARCH_STOCKS.value:
                # log.info(f'{client_id} Task Search Stocks: {args}')
                info = args['search']
                res = search_stocks(args['search'])
            elif cmd == CmdEnum.KLINES.value:
                # log.info(f'{client_id} Task Klines: {args}')
                info = f"{args['code']} - {args['durationStr']} - {args['barSizeSetting']}"
                res = klines(args['code'], args['durationStr'], args['barSizeSetting'], args['timeout'])
            elif cmd == CmdEnum.TICKS.value:
                # log.info(f'{client_id} Task Ticks: {args}')
                info = args['codes']
                res = ticks(args['codes'])
            elif cmd == CmdEnum.STOCK_INFO.value:
                # log.info(f'{client_id} Task Stock Info: {args}')
                info = args['code']
                res = stock_info(args['code'])
            elif cmd == CmdEnum.BALANCE.value:
                # log.info(f'{client_id} Task Balance: {args}')
                info = 'balance'
                res = balance()
            elif cmd == CmdEnum.POSITIONS.value:
                # log.info(f'{client_id} Task Positions: {args}')
                info = args['code']
                res = positions(args['code'])
            elif cmd == CmdEnum.ORDERS.value:
                # log.info(f'{client_id} Task Orders: {args}')
                info = args['code']
                res = orders(args['code'], args['type'], args['amount'])

            rd.Robj().lpush(args['key'], json.dumps(res))
            log.info(f'{client_id} Task CMD {cmd} [ {info} ] run times : {time.time() - s_time}')
        except Exception as e:
            log.error(f'{client_id} Task CMD {cmd} args {args} ERROR {e}')
            log.error(traceback.format_exc())


if __name__ == '__main__':
    print('Start Run')

    # 清空原来的队列
    for _k in [
        CmdEnum.SEARCH_STOCKS.value,
        CmdEnum.KLINES.value,
        CmdEnum.TICKS.value,
        CmdEnum.STOCK_INFO.value,
        CmdEnum.BALANCE.value,
        CmdEnum.POSITIONS.value,
        CmdEnum.ORDERS.value,
    ]:
        rd.Robj().delete(_k)

    # 清空原来的结果
    h_keys = rd.Robj().keys(f'{ib_res_hkey}*')
    for _k in h_keys:
        rd.Robj().delete(_k)

    # 启动 5 个客户端接收
    start_client_num = 5
    with ProcessPoolExecutor(
            start_client_num,
            mp_context=get_context("spawn")
    ) as executor:
        executor.map(run_tasks, [11,12,13,14,15])

    # run_tasks(0)
