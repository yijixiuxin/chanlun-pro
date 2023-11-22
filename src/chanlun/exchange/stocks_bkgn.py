import json
import os
import random
import time
from typing import Tuple

import akshare as ak
from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams
from tqdm.auto import tqdm

"""
股票板块概念
"""


class StocksBKGN(object):
    def __init__(self):
        self.file_name = (
            os.path.split(os.path.realpath(__file__))[0] + "/stocks_bkgn.json"
        )

        self.cache_file_bk = None

    def reload_ths_bkgn(self):
        """
        下载更新保存新的板块概念信息
        通过 同花顺 接口获取板块概念

        """
        error_msgs = []
        stock_industrys = {}
        ak_industry = ak.stock_board_industry_name_ths()
        for _, b in tqdm(ak_industry.iterrows()):
            b_name = b["name"]
            b_code = b["code"]
            try_nums = 0
            while True:
                try:
                    time.sleep(random.randint(4, 5))
                    # 获取板块的成分股
                    b_stocks = ak.stock_board_cons_ths(b_code)
                    for _, s in b_stocks.iterrows():
                        s_code = s["代码"]
                        if s_code not in stock_industrys.keys():
                            stock_industrys[s_code] = []
                        stock_industrys[s_code].append(b_name)
                except Exception as e:
                    time.sleep(60)
                    try_nums += 1
                    if try_nums >= 10:
                        msg = f"{b_name} {b_code} 行业板块获取成分股异常：{e}"
                        error_msgs.append(msg)
                        print(msg)
                        break
                finally:
                    break

        stock_concepts = {}
        ak_concept = ak.stock_board_concept_name_ths()
        for _, b in tqdm(ak_concept.iterrows()):
            b_name = b["概念名称"]
            b_code = b["代码"]
            try_nums = 0
            while True:
                try:
                    time.sleep(random.randint(4, 5))
                    # 获取概念的成分股
                    b_stocks = ak.stock_board_cons_ths(b_code)
                    for _, s in b_stocks.iterrows():
                        s_code = s["代码"]
                        if s_code not in stock_concepts.keys():
                            stock_concepts[s_code] = []
                        stock_concepts[s_code].append(b_name)
                except Exception as e:
                    time.sleep(60)
                    try_nums += 1
                    if try_nums >= 10:
                        msg = f"{b_name} {b_code} 概念板块获取成分股异常：{e}"
                        error_msgs.append(msg)
                        print(msg)
                        break
                finally:
                    break

        with open(self.file_name, "w", encoding="utf-8") as fp:
            json.dump({"hy": stock_industrys, "gn": stock_concepts}, fp)

        print("错误信息：", error_msgs)
        return True

    def reload_dfcf_bkgn(self):
        """
        下载更新保存新的板块概念信息
        通过 东方财富 接口获取板块概念

        """
        error_msgs = []
        stock_industrys = {}
        ak_industry = ak.stock_board_industry_name_em()
        for _, b in tqdm(ak_industry.iterrows()):
            b_name = b["板块名称"]
            b_code = b["板块代码"]
            try_nums = 0
            while True:
                try:
                    time.sleep(random.randint(1, 3))
                    # 获取板块的成分股
                    b_stocks = ak.stock_board_industry_cons_em(b_name)
                    for _, s in b_stocks.iterrows():
                        s_code = s["代码"]
                        if s_code not in stock_industrys.keys():
                            stock_industrys[s_code] = []
                        stock_industrys[s_code].append(b_name)
                except Exception as e:
                    time.sleep(60)
                    try_nums += 1
                    if try_nums >= 10:
                        msg = f"{b_name} {b_code} 行业板块获取成分股异常：{e}"
                        error_msgs.append(msg)
                        print(msg)
                        break
                finally:
                    break

        stock_concepts = {}
        ak_concept = ak.stock_board_concept_name_em()
        for _, b in tqdm(ak_concept.iterrows()):
            b_name = b["板块名称"]
            b_code = b["板块代码"]
            try_nums = 0
            while True:
                try:
                    time.sleep(random.randint(1, 3))
                    # 获取概念的成分股
                    b_stocks = ak.stock_board_concept_cons_em(b_name)
                    for _, s in b_stocks.iterrows():
                        s_code = s["代码"]
                        if s_code not in stock_concepts.keys():
                            stock_concepts[s_code] = []
                        stock_concepts[s_code].append(b_name)
                except Exception as e:
                    time.sleep(60)
                    try_nums += 1
                    if try_nums >= 10:
                        msg = f"{b_name} {b_code} 概念板块获取成分股异常：{e}"
                        error_msgs.append(msg)
                        print(msg)
                        break
                finally:
                    break

        with open(self.file_name, "w", encoding="utf-8") as fp:
            json.dump({"hy": stock_industrys, "gn": stock_concepts}, fp)

        print("错误信息：", error_msgs)
        return True

    def reload_tdx_bkgn(self):
        """
        下载更新保存新的板块概念信息
        通过 通达信 接口获取板块概念

        """
        stock_industrys = {}  # 保存行业的股票信息
        stock_concepts = {}  # 保存概念的股票信息

        # tdx_host = best_ip.select_best_ip('stock')
        tdx_host = {"ip": "221.194.181.176", "port": 7709}
        api = TdxHq_API(raise_exception=True, auto_retry=True)
        with api.connect(tdx_host["ip"], tdx_host["port"]):
            # 获取行业
            hy_infos = api.get_and_parse_block_info(TDXParams.BLOCK_DEFAULT)
            for _hy in hy_infos:
                _code = _hy["code"]
                if _code not in stock_industrys.keys():
                    stock_industrys[_code] = []
                stock_industrys[_code].append(_hy["blockname"])
                stock_industrys[_code] = list(set(stock_industrys[_code]))

            # 获取概念
            gn_infos = api.get_and_parse_block_info(TDXParams.BLOCK_GN)
            for _gn in gn_infos:
                _code = _gn["code"]
                if _code not in stock_concepts.keys():
                    stock_concepts[_code] = []
                stock_concepts[_code].append(_gn["blockname"])
                stock_concepts[_code] = list(set(stock_concepts[_code]))

        with open(self.file_name, "w", encoding="utf-8") as fp:
            json.dump({"hy": stock_industrys, "gn": stock_concepts}, fp)

        return True

    def file_bkgns(self) -> Tuple[dict, dict]:
        if self.cache_file_bk is None:
            with open(self.file_name, "r", encoding="utf-8") as fp:
                bkgns = json.load(fp)
            self.cache_file_bk = bkgns
        return self.cache_file_bk["hy"], self.cache_file_bk["gn"]

    def get_code_bkgn(self, code: str):
        """
        获取代码板块概念
        """
        code = (
            code.replace("SZ.", "")
            .replace("SH.", "")
            .replace("SZSE.", "")
            .replace("SHSE.", "")
        )
        hys, gns = self.file_bkgns()
        code_hys = []
        code_gns = []
        if code in hys.keys():
            code_hys = hys[code]
        if code in gns.keys():
            code_gns = gns[code]
        return {"HY": code_hys, "GN": code_gns}

    def get_codes_by_hy(self, hy_name):
        """
        根据行业名称，获取其中的股票代码列表
        """
        hys, gns = self.file_bkgns()
        codes = []
        for _code, _hys in hys.items():
            if hy_name in _hys:
                codes.append(_code)

        return codes

    def get_codes_by_gn(self, gn_name):
        """
        根据概念名称，获取其中的股票代码列表
        """
        hys, gns = self.file_bkgns()
        codes = []
        for _code, _gns in gns.items():
            if gn_name in _gns:
                codes.append(_code)

        return codes


if __name__ == "__main__":
    """
    更新行业概念信息并保存
    """
    bkgn = StocksBKGN()
    # 重新更新并保存行业与板块信息
    bkgn.reload_ths_bkgn()

    # 所有行业概念
    hys, gns = bkgn.file_bkgns()
    all_hy_names = []
    all_gn_names = []
    for _c, _v in hys.items():
        all_hy_names += _v
        all_hy_names = list(set(all_hy_names))
    for _c, _v in gns.items():
        all_gn_names += _v
        all_gn_names = list(set(all_gn_names))

    print("行业")
    print(all_hy_names)
    print("概念")
    print(all_gn_names)

    # '养殖业', '机场航运', '钢铁', '肉制品', '其他食品', '物流', '产业地产', '油服工程',
    # '软饮料', '饲料', '棉纺', '医疗服务', '机器人', '改性塑料', '保险及其他',
    # '商业地产', '特钢', '综合', '证券Ⅲ', '计算机设备Ⅲ', '涤纶', '其他小金属',
    # '港口', '白酒', '其他农产品加工', '农业服务', '建筑材料', '通信设备', '其他通信设备',
    # '电力', '电池', '商业物业经营', '保险', '医药商业', '互联网电商Ⅲ', '仪器仪表Ⅲ',
    # '纺织化学用品', '汽车服务', '机床工具', '化学制药', '其他自动化设备', '金属制品',
    # '百货零售', '军工电子', '自动化设备', '数字媒体', '专用设备', '果蔬加工', '半导体设备',
    # '彩电', '零售', '氟化工', '文娱用品', '贸易Ⅲ', '其他塑料制品', '集成电路设计',
    # '海洋捕捞', '氯碱', '粮食种植', '其他生物制品', '旅游综合', '综合环境治理', '耐火材料',
    # '化妆品', '复合肥', '能源金属', '其他化学制品', '环保设备', '楼宇设备', '工程机械',
    # '其他银行', '体育', '煤炭开采', '铁路运输', '钛白粉', '动物保健', '农商行', '房屋建设',
    # '油品石化贸易', '造纸Ⅲ', '航空运输', '电子化学品', '激光设备', '住宅开发', '石油加工',
    # '磁性材料', '调味发酵品', '餐饮', '黑色家电', '稀土', '汽车服务Ⅲ', '通信终端及配件',
    # '粮油加工', '医疗设备', '啤酒', '广告营销', '专业服务', 'IT服务', '水泥', '大气治理',
    # '有机硅', '乘用车', '国有大型银行', '教育', '包装印刷', '血液制品', '火电', '涂料油墨',
    # '建筑装饰', '汽车零部件', '瓷砖地板', '航空装备', '印刷', '医药商业Ⅲ', '燃气', '林业',
    # '服装', '种子生产', '金属新材料', '服装家纺', '航天装备', '化学制剂', '疫苗', '景点及旅游',
    # '农用机械', '新能源发电', '小金属', '白色家电', '房地产服务Ⅲ', '其他金属新材料', '纯碱',
    # '纺织服装设备', '乳品', '造纸', '通信线缆及配套', '其他黑色家电', '铜', '教育Ⅲ',
    # '装饰园林', '专业连锁', '酒店及餐饮', '传媒', '食品及饲料添加剂', '能源及重型设备',
    # '水产养殖', '分立器件', '消费电子', '工控设备', '普钢', '合成树脂', '鞋帽及其他', '
    # 其他家用轻工', '航海装备', '饰品', '线缆部件及其他', '其他交运设备', '其他白色家电',
    # '空调', '饮料制造', '农药', '原料药', '公交', '管材', '贸易', '冶钢原料', '化工合成材料',
    # '集成电路制造', '电气自控设备', '贵金属Ⅲ', '家具', '固废治理', '非金属材料', '电机',
    # '风电设备', '旅游零售', '化学制品', '有线电视网络', '影视院线', '其他酒类',
    # '厨卫电器Ⅲ', '油气开采', '体外诊断', '消费电子零部件及组装', '焦炭加工', '高速公路',
    # '软件开发', '其他化学原料', '无机盐', '被动元件', '纺织制造', '光学光电子', '医疗美容',
    # '通信服务', '水务及水治理', '汽车零部件Ⅲ', '房地产服务', '工业金属', '粘胶', '人工景点',
    # '膜材料', '其他通用设备', '轨交设备', '其他种植业', '印制电路板', '专业工程', '其他电子',
    # '印染', '铝', '半导体及元件', '制冷空调设备', '家用轻工', '商用载货车', '种植业与林业',
    # '畜禽养殖', '包装', '化学原料', '其他社会服务', '铅锌', '油气开采及服务', '氨纶',
    # '医疗耗材', '休闲食品', '商用载客车', '港口航运', '其他传媒', '炭黑', '通信服务Ⅲ',
    # '物流Ⅲ', '其他专用设备', 'LED', '汽车整车', '冰洗', '计算机应用', '品牌消费电子',
    # '小家电', '家纺', '电子化学品Ⅲ', '股份制银行', '酒店', '厨卫电器', '银行',
    # '互联网电商', '环保', '仪器仪表', '光学元件', '煤炭开采加工', '电能综合服务', '城商行',
    # '出版', '农产品加工', '多元金融', '房地产开发', '医疗器械', '证券', '医疗研发外包',
    # '计算机设备', '小家电Ⅲ', '地面兵装', '贵金属', '其他医疗服务', '其他橡胶制品',
    # '农业综合', '燃气Ⅲ', '热力', '美容护理', '非汽车交运', '公路铁路运输', '通用设备',
    # '食品加工制造', '综合Ⅲ', '其他电源设备', '光伏设备', '面板', '中药', '石油加工贸易',
    # '民爆用品', '生物制品', '机场', '其他纤维', '航运', '玻璃玻纤', '游戏', '集成电路封测',
    # '钾肥', '印刷包装机械', '磷肥及磷化工', '氮肥', '基础建设', '辅料', '国防军工', '半导体材料',
    # '输变电设备', '其他建材', '其他电子Ⅲ', '通信网络设备及器件', '聚氨酯', '其他纺织',
    # '非金属材料Ⅲ', '水电', '磨具磨料', '自然景点', '中药Ⅲ', '工程咨询服务',
    # '其他社会服务Ⅲ', '电力设备'

    # 概念
    # '石墨烯', '智能交通', '数字经济', '成飞概念', '云计算', '创新药', '虚拟现实',
    # '动物疫苗', '石墨电极', 'PET铜箔', '语音技术', '乡村振兴', '注册制次新股', 'DRG/DIP',
    # '智能家居', '消毒剂', '特色小镇', 'PPP概念', '世界杯', '新型烟草', '无人机', '新冠特效药',
    # '农业种植', '沪股通', '抗病毒面料', '汽车电子', 'MCU芯片', '托育服务', '养老概念',
    # '页岩气', '网络安全', '宠物经济', '自由贸易港', '抽水蓄能', '核电', '毛发医疗', '新疆振兴',
    # '无人零售', '丙烯酸', '航运概念', '粮食概念', '中俄贸易概念', '民爆概念', '超清视频',
    # '疫情监测', '新股与次新股', '可降解塑料', '手机游戏', '互联网彩票', '新零售',
    # '细胞免疫治疗', '参股银行', '智能电网', '粤港澳大湾区', '文化传媒', '磷化工', '污水处理',
    # '国家大基金持股', 'ERP概念', '超导概念', '军民融合', '移动支付', '太赫兹', '海南自贸区',
    # '无线耳机', '金属铅', '高压快充', 'MR（混合现实）', '金属锌', '脑机接口', '超级品牌',
    # '首发新股', '时空大数据', '独角兽概念', '虚拟电厂', '无线充电', '工业4.0', 'BC电池',
    # 'NMN概念', '预制菜', '可燃冰', '猴痘概念', '5G', '核准制次新股', '区块链', '烟草', '钴',
    # '供销社', '租售同权', '储能', 'PCB概念', '民营医院', '央企国企改革', '熊去氧胆酸',
    # '培育钻石', '云办公', '苹果概念', '工业大麻', '风电', '互联网医疗', '环氧丙烷',
    # '两轮车', '融资融券', '核污染防治', '华为鲲鹏', '蚂蚁金服概念', '电力物联网', '共享单车',
    # '3D打印', '土地流转', '数据安全', '三季报预增', '光热发电', '锂电池', '物业管理',
    # '网红经济', '海峡两岸', '数字货币', '绿色电力', '水泥概念', '阿尔茨海默概念', '存储芯片',
    # '人造肉', '高压氧舱', '电子身份证', '跨境电商', '信创', '节能照明', '眼科医疗', '禽流感',
    # '鸿蒙概念', '液冷服务器', '股权转让', '物联网', '一带一路', '空气能热泵', '减肥药',
    # '健康中国', '传感器', '参股新三板', '医美概念', '同花顺漂亮100', '深圳国企改革', '智慧政务',
    # '冰雪产业', '智能音箱', '汽车热管理', '短剧游戏', 'TOPCON电池', 'AIGC概念', '盐湖提锂',
    # 'HJT电池', '新材料概念', '数据确权', '医药电商', '互联网保险', '华为海思概念股',
    # '黑龙江自贸区', '燃料电池', '中韩自贸区', '北交所概念', '智能穿戴', '噪声防治',
    # '杭州亚运会', '国家大基金持股 ', '比亚迪概念', '社区团购', '电子竞技', '创投',
    # '富士康概念', '数据中心', '中芯国际概念', '壳资源', '深股通', '汽车芯片', '摘帽',
    # '雄安新区', '华为欧拉', '国企改革', '国资云', '上海国企改革', '宁德时代概念', 'Web3.0',
    # '量子科技', '稀缺资源', '减速器', '军工', '钛白粉概念', '抗原检测', '智能座舱', '卫星导航',
    # '债转股(AMC概念)', '有机硅概念', '煤炭概念', '猪肉', '安防', '天然气', '染料',
    # '医疗废物处理', '数字孪生', '网络游戏', '专精特新', '固废处理', '数据要素', '金属镍',
    # '芬太尼', '青蒿素', '光刻胶', '星闪概念', '柔性直流输电', '生物质能发电', '芯片概念',
    # '云游戏', '基因测序', '超超临界发电', '共同富裕示范区', '智能制造', '金改', 'POE胶膜',
    # '养鸡', '赛马概念', '特斯拉', 'ChatGPT概念', '钒电池', '足球概念', '东数西算（算力）',
    # '消费电子概念', '车联网', '毫米波雷达', '京津冀一体化', 'PM2.5', '免税店', '参股保险',
    # '露营经济', '航空发动机', '共封装光学（CPO）', '电子纸', '光伏概念', '人脸识别', '乳业',
    # '横琴新区', 'WiFi 6', '华为昇腾', '中字头股票', '阿里巴巴概念', '黄金概念', 'MLOps概念',
    # '地下管网', '高端装备', '标普道琼斯A股', '血氧仪', '国产航母', '工业互联网', 'NFT概念',
    # '肝炎概念', '虚拟数字人', '送转填权', '医疗器械概念', '一体化压铸', '装配式建筑',
    # '信托概念', '稀土永磁', '胎压监测', '大飞机', '在线教育', '通用航空', '草甘膦', '腾讯概念',
    # '净水概念', '分拆上市意愿', '钙钛矿电池', '动力电池回收', 'OLED', '长三角一体化', '啤酒概念',
    # '期货概念', '蒙脱石散', '无人驾驶', '冷链物流', 'EDR概念', '国产软件', '充电桩', '高铁',
    # '新冠治疗', '网约车', '人民币贬值受益', '体育产业', '互联网金融', '生物疫苗', '新冠检测',
    # '先进封装', '上海自贸区', '工业母机', '汽车拆解概念', '福建自贸区', '大数据', '化肥',
    # '家庭医生', '光伏建筑一体化', '抖音概念', '幽门螺杆菌概念', '特钢概念', '口罩', '转基因',
    # '室外经济', '海工装备', '硅能源', '中船系', '恒大概念', '智能医疗', '快手概念', '数字水印',
    # '碳中和', '食品安全', '算力租赁', 'ST板块', '低辐射玻璃（Low-E）', '英伟达概念', '流感',
    # '机器人概念', '6G概念', '光刻机', '生态农业', '家用电器', '代糖概念', '边缘计算',
    # '新型工业化', '农机', '换电概念', 'ETC', '知识产权保护', '特高压', '华为汽车', '职业教育',
    # '智能物流', '方舱医院', '空间计算', '氢能源', '数字乡村', '仿制药一致性评价', '新能源汽车',
    # '小金属概念', 'C2M概念', '生物医药', 'MiniLED', '证金持股', '网络直播', '国产操作系统',
    # 'F5G概念', '天津自贸区', '举牌', '电子商务', '农村电商', '广东自贸区', '垃圾分类',
    # '水利', '华为概念', '柔性屏', 'PVDF概念', '参股券商', '在线旅游', '金属回收', '大豆',
    # '元宇宙', '牙科医疗', '机器视觉', '百度概念', '人工智能', 'MicroLED概念', '建筑节能',
    # '煤化工', '氟化工概念', '小米概念', '新型城镇化', '跨境支付（CIPS）', '科创次新股',
    # '玉米', '拼多多概念', '土壤修复', '金属铜', '智慧灯杆', '同花顺中特估100', '集成电路概念',
    # '重组蛋白', '供应链金融', '燃料乙醇', '碳纤维', '超级电容', '节能环保', '钠离子电池',
    # '智慧城市', '碳交易', '辅助生殖', '白酒概念', '三胎概念', '统一大市场', '第三代半导体',
    # 'MSCI概念', '固态电池', '俄乌冲突概念', 'CRO概念'

    # 获取代码的板块概念信息
    # code_bkgn = bkgn.get_code_bkgn("301080")
    # print(code_bkgn)

    # 根据行业获取其中的代码
    # codes = bkgn.get_codes_by_hy('珠宝首饰')
    # print(codes)

    # 根据概念获取其中的代码
    # codes = bkgn.get_codes_by_gn('电子竞技')
    # print(codes)
