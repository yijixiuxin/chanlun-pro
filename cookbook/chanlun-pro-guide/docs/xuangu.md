# 选股操作指南

## 选股策略

chanlun-pro 支持基于缠论条件的选股功能。

### 基础选股流程

```python
from chanlun.exchange import get_exchange
from chanlun.base import Market
from chanlun.cl_utils import web_batch_get_cl_datas

# 1. 获取所有标的
ex = get_exchange(Market.A)
stocks = ex.all_stocks()

# 2. 筛选标的
run_codes = [s['code'] for s in stocks if s['code'].startswith('SH.6')]
```

### 缠论选股条件

```python
def check_stock(code, frequency='d'):
    """检查股票是否满足缠论条件"""
    ex = get_exchange(Market.A)
    klines = ex.klines(code, frequency, start_date='2024-01-01')

    cls = web_batch_get_cl_datas('a', code, {frequency: klines})
    cd = cls[0]

    # 条件1：存在买点
    mmds = cd.get_mmds()
    has_buy = any(m.type in ['1buy', '2buy', '3buy', 'l2buy', 'l3buy'] for m in mmds)

    # 条件2：存在背驰
    bcs = cd.get_bcs()
    has_bc = len(bcs) > 0

    # 条件3：笔完成
    bis = cd.get_bis()
    last_bi_done = bis[-1].is_done() if bis else False

    return has_buy and has_bc and last_bi_done
```

### 批量选股

```python
from concurrent.futures import ProcessPoolExecutor

def batch_xuangu(codes, frequency='d', max_workers=8):
    """多进程批量选股"""
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(check_stock, codes))

    return [code for code, ok in zip(codes, results) if ok]
```

## 参考脚本

- `script/crontab/xuangu_by_process.py` - 进程池选股
- `script/crontab/xuangu_by_same.py` - 同级别分解选股
- `script/crontab/run_history_xuangu.py` - 历史选股

## 选股策略类型

参考 `cookbook/docs/选股策略.md`：
- 背驰选股
- 买卖点选股
- 中枢震荡选股
- 同级别分解选股
