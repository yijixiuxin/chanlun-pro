from xtquant import xtdata
# 获取上海期货交易所所有标的的代码
# 注意：板块名称必须准确，QMT 中上期所的板块名为 "上期所"
SF_list = xtdata.get_stock_list_in_sector("上证期权")

# 主力合约列表
# 注意：QMT 中没有 "主力板块"，但有 "连续合约" 板块，或者可以通过后缀筛选主力合约
main_list = xtdata.get_stock_list_in_sector("连续合约")

# 显示列表返回值的前十个代码
print(f"上期所合约数量: {len(SF_list)}")
print(SF_list[:10])

print(f"连续合约数量: {len(main_list)}")
print(main_list[:10])

# 打印所有可用板块，方便参考
print("\n--- 可用板块列表 ---")
all_sectors = xtdata.get_sector_list()
print(all_sectors)
#--- 可用板块列表 ---
#['上期所', '上证A股', '上证B股', '上证期权', '上证转债', '中金所', '京市A股', '创业板', '大商所', '沪市ETF', '沪市债券', '沪市基金', '沪市指数', '沪深A股', '沪深B股', '沪深ETF', '沪深京A股', '沪深债券', '沪深基金', '沪深指数', '沪深转债', '深市ETF', '深市债券', '深市基金', '深市指数', '深证A股', '深证B股', '深证期权', '深证转债', '科创板', '科创板CDR', '能源中心', '连续合 约', '郑商所', '香港联交所指数', '香港联交所股票']