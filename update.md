## 更新记录

---

#### 2021-12-13

* 交易所行情 pandas 数据中，data 统一为 pandas 中的 datetime 类型；
* cl.py 缠论处理类，增加 增量更新方法，后续可追加计算后续行情数据；
* 优化策略回测时间，比之前节省 80% 时间；得益于 缠论增量更新功能；
* 其他 bug 修复

> 影响：  
> Trader.run 方法参数类型修改，原来传递 原始Kline，修改为传递计算好的 缠论数据  
> Trader 类初始方法，删除 is_save_kline 参数  
> Exchange 类中，klines 方法，返回的 DataFrame 数据中的 date 字段，类型需要是 pandas 的 datetime 类型 