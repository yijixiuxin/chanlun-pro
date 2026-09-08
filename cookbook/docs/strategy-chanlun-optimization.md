## 策略缠论参数优化

---

相同的策略，在不同的缠论配置下，会有不一样的表现，可以通过 `OptimizationSetting` 查找策略最优缠论配置。

1. 首先按照正常的策略回测，配置并初始化回测类

```

bt_config = {...}
BT = backtest.BackTest(bt_config)

```

2. 初始化参数优化类，并添加需要跑的缠论参数

```

setting = OptimizationSetting()
setting.add_cl_parameter('bi_type', ['bi_type_old', 'bi_type_new'])
setting.add_cl_parameter('zs_bi_type', ['zs_type_bz', 'zs_type_dn', 'zs_type_fx'])
...

```

3. 执行并返回参数优化结果

```

res = BT.run_optimization(setting, max_workers=6, next_frequency='d', evaluate='profit_rate')
print(res)

```

### OptimizationSetting

文件：`src/chanlun/backtesting/optimize.py`

#### 方法： `add_cl_parameter`

参数说明：

`name`: 缠论配置项，例如 bi_type

`values`：缠论配置项，允许的配置值列表，例如 ['bi_type_old', 'bi_type_new']

安装自己需要，添加需要优化的缠论配置项，以及对应的值列表

---

执行参数优化，需要执行 `BackTest` 类的 `run_optimization` 方法

#### 方法： `run_optimization`

参数说明：

`optimization_setting` ： 参数优化对象

`max_workers` ：优化最大进程数，默认为 CPU 的核心数

`next_frequency` ：参数优化回测任务，每次循环的周期，按照策略允许的最大周期运行，可加速运行时间

`evaluate` ：结果评估类型， profit_rate （总盈亏之和），max_profit_rate（最大盈利之和），默认为 profit_rate