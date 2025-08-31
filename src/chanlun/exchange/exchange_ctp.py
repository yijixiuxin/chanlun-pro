#!/usr/bin/env python
import hashlib
import os
import os.path
import tempfile
import threading
import time

import ctp
import pytest


@pytest.fixture(scope="module")
def spi(front, broker, user, password, app, auth):
    assert front and broker and user and password and app and auth, "missing arguments"
    _spi = TraderSpi(front, broker, user, password, app, auth)
    th = threading.Thread(target=_spi.run)
    th.daemon = True
    th.start()
    secs = 5
    while secs:
        if not (_spi.connected and _spi.authed and _spi.loggedin):
            secs -= 1
            time.sleep(1)
        else:
            break
    return _spi


class TraderSpi(ctp.CThostFtdcTraderSpi):
    def __init__(self, front, broker_id, user_id, password, app_id, auth_code):
        ctp.CThostFtdcTraderSpi.__init__(self)

        self.front = front
        self.broker_id = broker_id
        self.user_id = user_id
        self.password = password
        self.app_id = app_id
        self.auth_code = auth_code

        self.request_id = 0
        self.connected = False
        self.authed = False
        self.loggedin = False

        self.api = self.create()

    def create(self):
        dir = "".join(("ctp", self.broker_id, self.user_id)).encode("UTF-8")
        dir = hashlib.md5(dir).hexdigest()
        dir = os.path.join(tempfile.gettempdir(), dir, "Trader") + os.sep
        if not os.path.isdir(dir):
            os.makedirs(dir)
        return ctp.CThostFtdcTraderApi.CreateFtdcTraderApi(dir)

    def run(self):
        self.api.RegisterSpi(self)
        self.api.RegisterFront(self.front)
        self.api.Init()
        self.api.Join()

    def auth(self):
        field = ctp.CThostFtdcReqAuthenticateField()
        field.BrokerID = self.broker_id
        field.UserID = self.user_id
        field.AppID = self.app_id
        field.AuthCode = self.auth_code
        self.request_id += 1
        self.api.ReqAuthenticate(field, self.request_id)

    def login(self):
        field = ctp.CThostFtdcReqUserLoginField()
        field.BrokerID = self.broker_id
        field.UserID = self.user_id
        field.Password = self.password
        self.request_id += 1
        self.api.ReqUserLogin(field, self.request_id)

    def OnFrontConnected(self):
        print("OnFrontConnected")
        self.connected = True
        self.auth()

    def OnRspAuthenticate(
        self,
        pRspAuthenticateField: ctp.CThostFtdcRspAuthenticateField,
        pRspInfo: ctp.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        print("OnRspAuthenticate:", pRspInfo.ErrorID, pRspInfo.ErrorMsg)
        if pRspInfo.ErrorID == 0:
            self.authed = True
            self.login()

    def OnRspUserLogin(
        self,
        pRspUserLogin: ctp.CThostFtdcRspUserLoginField,
        pRspInfo: ctp.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        print("OnRspUserLogin", pRspInfo.ErrorID, pRspInfo.ErrorMsg)
        if pRspInfo.ErrorID == 0:
            self.loggedin = True

    def OnRspError(
        self, pRspInfo: ctp.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool
    ):
        print("OnRspError:", pRspInfo.ErrorID, pRspInfo.ErrorMsg)

    def __del__(self):
        self.api.RegisterSpi(None)
        self.api.Release()


def test_init(spi):
    assert spi.connected and spi.authed and spi.loggedin
