from chanlun.cl_utils import query_cl_chart_config, web_batch_get_cl_datas
from tqdm.auto import tqdm
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from chanlun.exchange.exchange_tdx import ExchangeTDX

ex = ExchangeTDX()
cache_freqs = ["d", "30m"]


def process_cache_code_cd(code):
    cl_config = query_cl_chart_config("a", "SH.000001")
    for f in cache_freqs:
        try:
            klines = ex.klines(code, f)
            web_batch_get_cl_datas("a", code, {f: klines}, cl_config)
        except Exception as e:
            print(f"Error : {code} {f}")


if __name__ == "__main__":

    # 5个进程同时处理
    with ProcessPoolExecutor(
        max_workers=5, mp_context=get_context("spawn")
    ) as executor:
        # 获取要缓存计算的股票代码
        cache_codes = ex.all_stocks()
        cache_codes = [
            _s["code"]
            for _s in cache_codes
            if _s["code"][0:5] in ["SH.60", "SZ.00", "SZ.30"]
        ]
        print("cache_codes:", len(cache_codes))
        bar = tqdm(total=len(cache_codes))
        for _ in executor.map(process_cache_code_cd, cache_codes):
            bar.update(1)
    print("Done")
