from wtpy import BaseCtaStrategy, WtBarRecords
from wtpy import CtaContext

from chanlun import cl
from chanlun.backtesting.base import *
from chanlun.cl_interface import *


class BaseStrategy(BaseCtaStrategy):
    """
    缠论 wtpy 策略类
    """

    def __init__(self, name: str, strategy: Strategy, code: str, period: str):
        BaseCtaStrategy.__init__(self, name)

        self.code = code
        self.period = period

        # 用来保存缠论数据
        self.cl_datas: Dict[str, ICL] = {}

        # 基于缠论的策略
        self.STR = strategy

        # 记录持仓 TODO 实盘需要进行持久化
        self.positions: Dict[str, POSITION] = {}

    @staticmethod
    def bars_to_df_klines(code: str, bars: WtBarRecords) -> pd.DataFrame:
        """
        将 wtpy 的k线数据，转换成 缠论所需的 DataFram 数据
        """
        bars_df = bars.to_df()
        # Index(['date', 'bartime', 'open', 'high', 'low', 'close', 'settle', 'money',
        #        'volume', 'hold', 'diff'],
        #       dtype='object')
        bars_df['code'] = code
        bars_df['date'] = pd.to_datetime(bars_df['bartime'])
        return bars_df[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]

    def on_init(self, context: CtaContext):
        """
        初始化策略时，初始缠论数据
        """
        for _code in [self.code]:
            for _p in [self.period]:
                cl_kye = '%s_%s' % (_code, _p)
                bars = context.stra_get_bars(_code, _p, 2000, isMain=True)
                self.cl_datas[cl_kye] = cl.CL(_code, _p).process_klines(self.bars_to_df_klines(_code, bars))

        context.stra_log_text('Strategy inited')

    def get_poss(self, code) -> List[POSITION]:
        """
        获取代码的持仓记录
        """
        poss = []
        for _k in self.positions.keys():
            if code in poss:
                poss.append(self.positions[_k])
        return poss

    def get_cl_datas(self, code, context: CtaContext) -> List[ICL]:
        """
        获取 代码周期 的缠论数据
        """
        cds = []
        for period in [self.period]:
            cl_key = '%s_%s' % (code, period)
            df_bars = context.stra_get_bars(code, period, 10, isMain=True)
            self.cl_datas[cl_key].process_klines(self.bars_to_df_klines(code, df_bars))
            cds.append(self.cl_datas[cl_key])
        return cds

    def open_buy(self, context: CtaContext, code: str, amount: float, opt: Operation):
        """
        开仓买入
        """
        res = context.stra_enter_long(code, amount, 'enterlong')
        context.stra_log_text(opt.msg)
        pos: POSITION = POSITION(
            code=code, mmd=opt.mmd, type='long', balance=1, price=0, amount=amount,
            loss_price=opt.loss_price, open_msg=opt.msg, info=opt.info
        )
        pos_key = '%s_%s' % (code, opt.mmd)
        self.positions[pos_key] = pos
        return res

    def open_sell(self, context: CtaContext, code: str, amount: float, opt: Operation):
        """
        开仓卖出
        """
        res = context.stra_enter_short(code, amount, 'entershort')
        context.stra_log_text(opt.msg)
        pos: POSITION = POSITION(
            code=code, mmd=opt.mmd, type='short', balance=1, price=0, amount=amount,
            loss_price=opt.loss_price, open_msg=opt.msg, info=opt.info
        )
        pos_key = '%s_%s' % (code, opt.mmd)
        self.positions[pos_key] = pos
        return res

    def close_buy(self, context: CtaContext, code, opt: Operation):
        pos_key = '%s_%s' % (code, opt.mmd)
        if pos_key not in self.positions.keys():
            context.stra_log_text('平多仓，没有查找到对应的持仓记录：%s' % pos_key)
            return None
        pos: POSITION = self.positions[pos_key]
        res = context.stra_exit_long(code, pos.amount, 'exitlong')
        context.stra_log_text(opt.msg)

        del (self.positions[pos_key])
        return res

    def close_sell(self, context: CtaContext, code, opt: Operation):
        pos_key = '%s_%s' % (code, opt.mmd)
        if pos_key not in self.positions.keys():
            context.stra_log_text('平空仓，没有查找到对应的持仓记录：%s' % pos_key)
            return None
        pos: POSITION = self.positions[pos_key]
        res = context.stra_exit_short(code, pos.amount, 'exitshort')
        context.stra_log_text(opt.msg)

        del (self.positions[pos_key])
        return res

    def on_calculate(self, context: CtaContext):

        for code in [self.code]:
            # 根据实际交易品种，定义交易数量
            trdUnit = 1

            # 读取最新的行情数据，增量更新，不需要太多
            cds = self.get_cl_datas(code, context)
            # 读取当前仓位
            curPos = context.stra_get_position(code)

            if curPos == 0:
                # 当前空仓，判断是否可以开仓
                open_opts = self.STR.open(code, cds)
                for opt in open_opts:
                    if 'buy' in opt.mmd:
                        self.open_buy(context, code, trdUnit, opt)
                    elif 'sell' in opt.mmd:
                        self.open_sell(context, code, trdUnit, opt)
            elif curPos > 0:
                # 查找当前运行代码的持仓记录
                poss = self.get_poss(code)
                for pos in poss:
                    opt = self.STR.close(code, pos.mmd, pos, cds)
                    if opt is not False:
                        self.close_buy(context, code, opt)
            elif curPos < 0:
                # 查找当前运行代码的持仓记录
                poss = self.get_poss(code)
                for pos in poss:
                    opt = self.STR.close(code, pos.mmd, pos, cds)
                    if opt is not False:
                        self.close_sell(context, code, opt)
        return

    def on_tick(self, context: CtaContext, stdCode: str, newTick: dict):
        # context.stra_log_text ("on tick fired")
        return
