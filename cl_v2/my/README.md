# 个人私密的文件夹

----

* 这里可以实现自己的策略、实盘交易类 *

### 策略继承 strategy.Strategy
> 需要实现其中的 look、stare 方法  
> look 方法用来检查并发送 开仓 信号  
> stare 方法用来检查当前持仓的 平仓 信号


### 实盘交易类继承 trader.Trader
> 实现其中的 open_buy、open_sell、close_buy、close_sell 方法，实现交易功能
