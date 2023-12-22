import base64
import hashlib
import hmac
import time
import urllib.parse

import requests

from chanlun.cl_interface import *
from chanlun.db import db
from chanlun import config


def config_get_proxy():
    db_proxy = db.cache_get("req_proxy")
    if db_proxy is not None and db_proxy["host"] != "" and db_proxy["port"] != "":
        return db_proxy
    return {
        "host": config.PROXY_HOST,
        "port": config.PROXY_PORT,
    }


def config_get_dingding_keys(market):
    db_dd_key = db.cache_get("dd_keys")
    if db_dd_key is not None and db_dd_key["token"] != "" and db_dd_key["secret"] != "":
        return db_dd_key
    if market == "a":
        return config.DINGDING_KEY_A
    if market == "a":
        return config.DINGDING_KEY_HK
    if market == "us":
        return config.DINGDING_KEY_US
    if market == "futures":
        return config.DINGDING_KEY_FUTURES
    if market == "currency":
        return config.DINGDING_KEY_CURRENCY

    return None


def send_dd_msg(market: str, msg: Union[str, dict]):
    """
    发送钉钉消息
    https://open.dingtalk.com/document/robots/custom-robot-access

    :param market:
    :param msg: 如果类型是 str 则发送文本消息，dict 发送 markdown 消息 (dict demo {'title': '标题', 'text': 'markdown内容'})
    :return:
    """
    dd_info = config_get_dingding_keys(market)
    if dd_info is None or dd_info["token"] == "" or dd_info["secret"] == "":
        return True

    url = "https://oapi.dingtalk.com/robot/send?access_token=%s&timestamp=%s&sign=%s"

    def sign():
        timestamp = str(round(time.time() * 1000))
        secret = dd_info["secret"]
        secret_enc = secret.encode("utf-8")
        string_to_sign = "{}\n{}".format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode("utf-8")
        hmac_code = hmac.new(
            secret_enc, string_to_sign_enc, digestmod=hashlib.sha256
        ).digest()
        _sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return timestamp, _sign

    t, s = sign()
    url = url % (dd_info["token"], t, s)
    if isinstance(msg, str):
        res = requests.post(
            url,
            json={
                "msgtype": "text",
                "text": {"content": msg},
            },
        )
    else:
        res = requests.post(url, json={"msgtype": "markdown", "markdown": msg})

    # print(res.text)
    return True


if __name__ == "__main__":
    pass
