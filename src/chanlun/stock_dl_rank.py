import json
import time

from chanlun import rd


class StockDLHYRank(object):
    """
    行业动量排行
    """

    def __init__(self):
        pass

    def add_dl_rank(self, dl_json):
        """
        添加新的动量排行
        :param dl_json:
        :return:
        """

        def reformat_code(code):
            symbol = code[:6]
            exchange = code[-5:]
            return f'SH.{symbol}' if exchange == '.XSHG' else f'SZ.{symbol}'

        today = time.strftime('%Y-%m-%d')
        dl_rank = json.loads(dl_json)
        i = 0
        for dl in dl_rank:
            i += 1
            dl[1]['num'] = i
            dl[1]['diff_num'] = 0
            dl[1]['diff_score'] = 0

        ranks = rd.dl_hy_rank_query()
        if len(ranks) == 0 or (today in ranks and len(ranks) == 1):
            # 之前没有保存过，直接保存
            ranks[today] = dl_rank
            rd.dl_hy_rank_save(ranks)
            return True

        if today in ranks:
            pre_rank = list(ranks.values())[-2]
        else:
            pre_rank = list(ranks.values())[-1]

        for dl in dl_rank:
            pre_hy = self._find_hycode_info(pre_rank, dl[0])
            if pre_hy is None:
                continue
            dl[1]['diff_num'] = pre_hy['num'] - dl[1]['num']
            dl[1]['diff_score'] = dl[1]['score'] - pre_hy['score']
            dl[1]['json_stocks'] = []
            for i in range(0, len(dl[1]['cf_stocks'])):
                dl[1]['json_stocks'].append(
                    {'code': reformat_code(dl[1]['cf_stocks'][i]), 'name': dl[1]['cf_names'][i]})
            dl[1]['json_stocks'] = json.dumps(dl[1]['json_stocks'], ensure_ascii=False)

        ranks[today] = dl_rank
        rd.dl_hy_rank_save(ranks)
        return True

    def query(self, length=5):
        """
        查询动量排行数据
        :return:
        """
        ranks = rd.dl_hy_rank_query()
        keys = list(ranks.keys())
        return {k: ranks[k] for k in keys[-length:]}

    def _find_hycode_info(self, ranks, hycode):
        """
        在排行中查找行业信息
        :param ranks:
        :param hycode:
        :return:
        """
        return next((r[1] for r in ranks if r[0] == hycode), None)


class StockDLGNRank(object):
    """
    概念动量排行
    """

    def __init__(self):
        pass

    def add_dl_rank(self, dl_json):
        """
        添加新的动量排行
        :param dl_json:
        :return:
        """

        def reformat_code(code):
            symbol = code[:6]
            exchange = code[-5:]
            return f'SH.{symbol}' if exchange == '.XSHG' else f'SZ.{symbol}'

        today = time.strftime('%Y-%m-%d')
        dl_rank = json.loads(dl_json)
        i = 0
        for dl in dl_rank:
            i += 1
            dl[1]['num'] = i
            dl[1]['diff_num'] = 0
            dl[1]['diff_score'] = 0

        ranks = rd.dl_gn_rank_query()
        if len(ranks) == 0 or (today in ranks and len(ranks) == 1):
            # 之前没有保存过，直接保存
            ranks[today] = dl_rank
            rd.dl_gn_rank_save(ranks)
            return True

        if today in ranks:
            pre_rank = list(ranks.values())[-2]
        else:
            pre_rank = list(ranks.values())[-1]

        for dl in dl_rank:
            pre_gn = self._find_gncode_info(pre_rank, dl[0])
            if pre_gn is None:
                continue
            dl[1]['diff_num'] = pre_gn['num'] - dl[1]['num']
            dl[1]['diff_score'] = dl[1]['score'] - pre_gn['score']
            dl[1]['json_stocks'] = []
            for i in range(0, len(dl[1]['cf_stocks'])):
                dl[1]['json_stocks'].append(
                    {'code': reformat_code(dl[1]['cf_stocks'][i]), 'name': dl[1]['cf_names'][i]})
            dl[1]['json_stocks'] = json.dumps(dl[1]['json_stocks'], ensure_ascii=False)

        ranks[today] = dl_rank
        rd.dl_gn_rank_save(ranks)
        return True

    def query(self, length=5):
        """
        查询动量排行数据
        :return:
        """
        ranks = rd.dl_gn_rank_query()
        keys = list(ranks.keys())
        return {k: ranks[k] for k in keys[-length:]}

    def _find_gncode_info(self, ranks, gncode):
        """
        在排行中查找行业信息
        :param ranks:
        :param gncode:
        :return:
        """
        return next((r[1] for r in ranks if r[0] == gncode), None)
