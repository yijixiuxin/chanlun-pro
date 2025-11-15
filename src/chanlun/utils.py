"""
Utilities for external message sending and configuration helpers.

This module centralizes small configuration accessors and wrappers to
send messages to DingTalk and Feishu, with light caching via the DB
cache interface. Functions include clear docstrings and type hints,
and minor fixes to market key mapping.
"""

import base64
import hashlib
import hmac
import json
import time
import urllib.parse
from typing import Dict, Optional, Union

import lark_oapi as lark
import requests
from lark_oapi.api.im.v1 import (
    CreateMessageRequest,
    CreateMessageRequestBody,
    CreateMessageResponse,
)

from chanlun import config
from chanlun.db import db


def config_get_proxy() -> Dict[str, str]:
    """
    Get HTTP proxy configuration.

    Priority: DB cache key `req_proxy` overrides defaults if present.

    Returns a dict with `host` and `port`.
    """
    db_proxy = db.cache_get("req_proxy")
    if db_proxy is not None and db_proxy.get("host") and db_proxy.get("port"):
        return db_proxy
    return {"host": config.PROXY_HOST, "port": config.PROXY_PORT}


def config_get_dingding_keys(market: str) -> Optional[Dict[str, str]]:
    """
    Return DingTalk robot keys for the given market.

    DB cache `dd_keys` overrides defaults when present and complete.
    Market mapping:
    - a -> `config.DINGDING_KEY_A`
    - hk -> `config.DINGDING_KEY_HK`
    - us -> `config.DINGDING_KEY_US`
    - futures -> `config.DINGDING_KEY_FUTURES`
    - currency -> `config.DINGDING_KEY_CURRENCY`
    """
    db_dd_key = db.cache_get("dd_keys")
    if db_dd_key is not None and db_dd_key.get("token") and db_dd_key.get("secret"):
        return db_dd_key
    if market == "a":
        return config.DINGDING_KEY_A
    if market == "hk":  # fixed wrong duplicate 'a' condition
        return config.DINGDING_KEY_HK
    if market == "us":
        return config.DINGDING_KEY_US
    if market == "futures":
        return config.DINGDING_KEY_FUTURES
    if market == "currency":
        return config.DINGDING_KEY_CURRENCY

    return None


def config_get_feishu_keys(market: str) -> Dict[str, str]:
    """
    Get Feishu app credentials and target user.

    DB cache `fs_keys` overrides defaults if present and complete.
    """
    db_fs_key = db.cache_get("fs_keys")
    if (
        db_fs_key is not None
        and db_fs_key.get("fs_app_id")
        and db_fs_key.get("fs_app_secret")
        and db_fs_key.get("fs_user_id")
    ):
        return {
            "app_id": db_fs_key["fs_app_id"],
            "app_secret": db_fs_key["fs_app_secret"],
            "user_id": db_fs_key["fs_user_id"],
        }
    keys = config.FEISHU_KEYS.get("default", {}).copy()
    if market in config.FEISHU_KEYS.keys():
        keys = config.FEISHU_KEYS[market].copy()
    keys["user_id"] = config.FEISHU_KEYS["user_id"]
    return keys


# 旧版的API接口已经下架，不能新增了，后续使用 飞书的 消息接口
def send_dd_msg(market: str, msg: Union[str, Dict[str, str]]) -> bool:
    """
    发送钉钉消息（自定义机器人）。

    - `msg` 为字符串时发送文本消息。
    - `msg` 为字典时发送 markdown 消息，形如 `{"title": "标题", "text": "markdown内容"}`。
    """
    dd_info = config_get_dingding_keys(market)
    if dd_info is None or dd_info["token"] == "" or dd_info["secret"] == "":
        return True  # no-op when not configured

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
        requests.post(
            url,
            json={
                "msgtype": "text",
                "text": {"content": msg},
            },
        )
    else:
        requests.post(url, json={"msgtype": "markdown", "markdown": msg})

    # print(res.text)
    return True


def send_fs_msg(market: str, title: str, contents: Union[str, list]) -> bool:
    """
    发送飞书消息（富文本 post）。
    """
    fs_key = config_get_feishu_keys(market)
    if (
        fs_key is None
        or fs_key["app_id"] == ""
        or fs_key["app_secret"] == ""
        or fs_key["user_id"] == ""
    ):
        return True  # no-op when not configured
    # 创建client
    client = (
        lark.Client.builder()
        .app_id(fs_key["app_id"])
        .app_secret(fs_key["app_secret"])
        .log_level(lark.LogLevel.WARNING)
        .build()
    )
    # https://open.feishu.cn/document/server-docs/im-v1/message-content-description/create_json
    if isinstance(contents, str):
        msg_content = {
            "zh_cn": {
                "title": title,
                "content": [[{"tag": "text", "text": f"{contents} \n"}]],
            }
        }
    else:
        msg_content = {
            "zh_cn": {
                "title": title,
                "content": [[]],
            }
        }
        for _c in contents:
            if _c.startswith("img_"):  # 支持图片消息
                msg_content["zh_cn"]["content"][0].append(
                    {"tag": "img", "image_key": f"{_c}"}
                )
            else:
                msg_content["zh_cn"]["content"][0].append(
                    {"tag": "text", "text": f"{_c} \n"}
                )

    msg_content = json.dumps(msg_content, ensure_ascii=False)
    # 构造请求对象
    request: CreateMessageRequest = (
        CreateMessageRequest.builder()
        .receive_id_type("user_id")
        .request_body(
            CreateMessageRequestBody.builder()
            .receive_id(fs_key["user_id"])
            .msg_type("post")
            .content(msg_content)
            .build()
        )
        .build()
    )

    # 发起请求
    response: CreateMessageResponse = client.im.v1.message.create(request)
    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.im.v1.message.create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}"
        )
    return True


if __name__ == "__main__":
    send_fs_msg(
        "us", "这里是选股的测试消息", ["运行完成", "选出300只股票", "用时1000小时"]
    )
