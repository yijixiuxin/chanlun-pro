"""
交易日工具

交易日历数据通过 akshare（ak.tool_trade_date_hist_sina）获取，并缓存到 数据目录 的 json 下，
每次读取时如果当前日期大于等于缓存中的最后一个交易日（或要查询的日期超出缓存范围），
则重新从 akshare 更新并缓存

用法：
    from chanlun.tools import trade_date

    trade_date.is_trade_date()                    # 今天是否交易日
    trade_date.is_trade_date("2024-10-01")        # 指定日是否交易日
    trade_date.next_trade_date("2024-09-30")      # 下一个交易日
    trade_date.prev_trade_date("2024-10-08")      # 上一个交易日
    trade_date.is_weekend("2024-09-28")           # 是否周末
    trade_date.is_last_trade_date_before_holiday("2024-09-30")  # 是否节假日前最后一个交易日
    trade_date.get_holiday_info("2024-09-30")     # 节假日前最后一个交易日的休市信息
"""

import bisect
import datetime
import json
import pathlib
import threading

import akshare as ak

from chanlun import fun
from chanlun.config import get_data_path

# 缓存文件名称，保存在 数据目录 的 json 下
CACHE_FILE_NAME = "trade_date.json"

_logger = fun.get_logger("trade_date.log")

_lock = threading.Lock()
# 内存中的交易日列表（YYYY-MM-DD 字符串，升序）
_trade_dates: list[str] | None = None
# 查询日期超出交易日历范围时，是否已经尝试过更新（避免重复请求 akshare）
_out_of_range_updated = False


def _cache_file() -> pathlib.Path:
    json_path = get_data_path() / "json"
    if json_path.is_dir() is False:
        json_path.mkdir(parents=True)
    return json_path / CACHE_FILE_NAME


def _date_to_str(_date=None) -> str:
    """
    将日期参数统一转换为 YYYY-MM-DD 字符串，不传则使用当前日期
    """
    if _date is None:
        return datetime.date.today().strftime("%Y-%m-%d")  # noqa: DTZ011
    if isinstance(_date, datetime.datetime):
        return _date.strftime("%Y-%m-%d")
    if isinstance(_date, datetime.date):
        return _date.strftime("%Y-%m-%d")
    if isinstance(_date, str):
        _s = _date.strip()
        for _format in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y%m%d"):
            try:
                return datetime.datetime.strptime(_s, _format).strftime("%Y-%m-%d")
            except ValueError:
                continue
    raise ValueError(f"不支持的日期参数：{_date}")


def _index(trade_dates: list[str], date_str: str) -> int | None:
    """
    交易日列表中使用二分查找指定日期，存在返回下标，不存在返回 None
    """
    idx = bisect.bisect_left(trade_dates, date_str)
    if idx < len(trade_dates) and trade_dates[idx] == date_str:
        return idx
    return None


def _read_cache() -> list[str] | None:
    cache_file = _cache_file()
    if cache_file.exists() is False:
        return None
    try:
        with cache_file.open("r", encoding="utf-8") as f:
            _trade_dates = json.load(f)["trade_dates"]
        return _trade_dates if _trade_dates else None
    except Exception as e:  # noqa: BLE001
        _logger.warning(f"读取交易日缓存文件 {cache_file} 异常：{e!s}")
        return None


def _write_cache(trade_dates: list[str]):
    cache_file = _cache_file()
    with cache_file.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "update_time": fun.datetime_to_str(datetime.datetime.now()),
                "trade_dates": trade_dates,
            },
            f,
            ensure_ascii=False,
        )
    _logger.info(f"交易日历缓存到 {cache_file}")


def _update() -> list[str]:
    """
    通过 akshare 获取交易日历，并写入缓存文件
    """
    df = ak.tool_trade_date_hist_sina()
    trade_dates = sorted(
        {
            _d.strftime("%Y-%m-%d")
            if isinstance(_d, (datetime.date, datetime.datetime))
            else str(_d)[:10]
            for _d in df["trade_date"].tolist()
        }
    )
    if len(trade_dates) == 0:
        raise Exception("akshare 获取的交易日历数据为空")  # noqa: TRY002
    _write_cache(trade_dates)
    _logger.info(
        f"更新交易日历 {trade_dates[0]} ~ {trade_dates[-1]}，共 {len(trade_dates)} 个交易日"
    )
    return trade_dates


def _get_trade_dates(date_str: str | None = None) -> list[str]:
    """
    获取交易日列表，需要更新时（当前日期大于等于最后一个交易日，或查询日期超出缓存范围）重新获取并缓存
    """
    global _trade_dates, _out_of_range_updated
    with _lock:
        if _trade_dates is None:
            _trade_dates = _read_cache()

        need_update = not _trade_dates
        if need_update is False:
            if _date_to_str() >= _trade_dates[-1]:
                need_update = True
            elif (
                date_str
                and date_str > _trade_dates[-1]
                and _out_of_range_updated is False
            ):
                # 查询的日期超出交易日历范围（如查询明年的日期），尝试更新一次
                need_update = True
                _out_of_range_updated = True

        if need_update:
            try:
                _trade_dates = _update()
            except Exception as e:
                if not _trade_dates:
                    raise
                _logger.warning(f"更新交易日历失败，使用本地缓存数据：{e!s}")
        return _trade_dates


def get_trade_dates(force_update: bool = False) -> list[str]:
    """
    获取所有交易日（YYYY-MM-DD 字符串，升序）
    """
    global _trade_dates
    if force_update:
        with _lock:
            _trade_dates = _update()
        return list(_trade_dates)
    return list(_get_trade_dates())


def is_weekend(_date=None) -> bool:
    """
    指定日期（默认今天）是否周末（周六、周日）
    """
    _d = datetime.datetime.strptime(_date_to_str(_date), "%Y-%m-%d").date()
    return _d.weekday() in (5, 6)


def is_trade_date(_date=None) -> bool:
    """
    指定日期（默认今天）是否交易日
    """
    date_str = _date_to_str(_date)
    return _index(_get_trade_dates(date_str), date_str) is not None


def next_trade_date(_date=None, contain_self: bool = False) -> str | None:
    """
    获取下一个交易日
    :param _date:       指定日期，默认当前日期
    :param contain_self: 指定日期本身是交易日时，是否返回其本身，默认 False
    :return: 交易日 YYYY-MM-DD 字符串，超出交易日历范围返回 None
    """
    date_str = _date_to_str(_date)
    trade_dates = _get_trade_dates(date_str)
    # contain_self 时查找第一个大于等于指定日期的交易日，否则查找第一个大于指定日期的交易日
    idx = (
        bisect.bisect_left(trade_dates, date_str)
        if contain_self
        else bisect.bisect_right(trade_dates, date_str)
    )
    return trade_dates[idx] if idx < len(trade_dates) else None


def prev_trade_date(_date=None, contain_self: bool = False) -> str | None:
    """
    获取上一个交易日
    :param _date:       指定日期，默认当前日期
    :param contain_self: 指定日期本身是交易日时，是否返回其本身，默认 False
    :return: 交易日 YYYY-MM-DD 字符串，超出交易日历范围返回 None
    """
    date_str = _date_to_str(_date)
    trade_dates = _get_trade_dates(date_str)
    idx = bisect.bisect_left(trade_dates, date_str)
    if contain_self and idx < len(trade_dates) and trade_dates[idx] == date_str:
        return date_str
    return trade_dates[idx - 1] if idx > 0 else None


def get_holiday_info(_date=None) -> dict | None:
    """
    获取指定日期（默认今天）作为“节假日前最后一个交易日”的休市信息

    如果指定日期不是交易日，或其与下一个交易日之间只有正常的周末休市（没有额外休市的工作日），返回 None
    :return: {
        trade_date:        节假日前最后一个交易日,
        next_trade_date:   节假日后第一个交易日,
        holiday_start:     休市开始日期（交易日次日）,
        holiday_end:       休市结束日期（下一交易日的前一天）,
        holiday_days:      休市自然日天数,
        holiday_workdays:  休市中占用的工作日数量,
    }
    """
    date_str = _date_to_str(_date)
    trade_dates = _get_trade_dates(date_str)
    if _index(trade_dates, date_str) is None:
        return None

    next_date_str = next_trade_date(date_str)
    if next_date_str is None:
        return None

    holiday_start = datetime.datetime.strptime(
        date_str, "%Y-%m-%d"
    ).date() + datetime.timedelta(days=1)
    holiday_end = datetime.datetime.strptime(
        next_date_str, "%Y-%m-%d"
    ).date() - datetime.timedelta(days=1)
    holiday_days = (holiday_end - holiday_start).days + 1
    # 统计休市期间的工作日（周一 ~ 周五）数量，为 0 则说明只是正常的周末休市，不算节假日
    holiday_workdays = sum(
        1
        for _i in range(holiday_days)
        if (holiday_start + datetime.timedelta(days=_i)).weekday() < 5
    )
    if holiday_workdays == 0:
        return None

    return {
        "trade_date": date_str,
        "next_trade_date": next_date_str,
        "holiday_start": holiday_start.strftime("%Y-%m-%d"),
        "holiday_end": holiday_end.strftime("%Y-%m-%d"),
        "holiday_days": holiday_days,
        "holiday_workdays": holiday_workdays,
    }


def is_last_trade_date_before_holiday(_date=None) -> bool:
    """
    指定日期（默认今天）是否为节假日前最后一个交易日
    """
    return get_holiday_info(_date) is not None


if __name__ == "__main__":
    print(
        f"交易日历：{get_trade_dates()[0]} ~ {get_trade_dates()[-1]}，共 {len(get_trade_dates())} 个交易日"
    )
    for _date in [
        None,
        "2024-09-28",
        "2024-09-30",
        "2024-10-01",
        "2024-10-08",
        "2024-04-30",
    ]:
        print(
            f"{_date_to_str(_date)} 交易日:{is_trade_date(_date)} 周末:{is_weekend(_date)} "
            f"上一交易日:{prev_trade_date(_date)} 下一交易日:{next_trade_date(_date)} "
            f"节假日前最后一个交易日:{is_last_trade_date_before_holiday(_date)} {get_holiday_info(_date)}"
        )
