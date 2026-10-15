"""QMT（大 QMT / XtItClient）交易端 GUI 自动化。

账号资金框向上扩大，持仓保留24格

大 QMT 桥接服务（Redis / 外部文件 / 网络）不可用时，改用 pyautogui 模拟鼠标键盘
操作交易端界面完成交易：

    * 登录：登录窗选择「交易」标签后登录（只有交易界面，元素少、坐标稳定）
    * 下单：买入 / 卖出；trade_target 支持「按目标持仓数量」成交
    * 查询：持仓 / 委托 / 资金 —— 列表右键「复制数据」后读剪贴板，
            取到全部列（不受界面列宽遮挡影响），也不依赖 OCR
    * 撤单：定位委托行 -> 勾选 -> 「选中撤单」

OCR 仅用于：定位右键菜单项 / 下拉项 / 弹窗按钮，以及读取下单面板的
证券名称、最新价、可买可卖。列表数据一律走剪贴板。

路径、账号、密码都不是本模块的配置，必须在构造时传入：

    t = QMTGuiTrader(
        exe_path=r"D:\\国金证券QMT交易端\\bin.x64\\XtItClient.exe",
        account="888********",
        password="******",
    )
    t.ready()
    t.balance(); t.positions(); t.orders()
    t.trade_target("SH.603811", "sell", 7200)   # 目标持仓 7200 股

界面坐标集中在下面「界面标定」区，以本机 QMT 最大化窗口 2560x1392 为基准，
窗口尺寸变化时按比例换算。
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import psutil
import pyautogui
import pyperclip
from PIL import Image

from chanlun import fun

# ======================================================================
# 一、界面标定（基准窗口 2560x1392，坐标为控件中心点）
# ======================================================================

DESIGN_W, DESIGN_H = 2560, 1392

# 登录窗口（无边框 Qt，实测 624x443）：窗口内相对坐标
LOGIN_TAB_TRADE = (312, 218)  # 「交易」标签（居中那个）
LOGIN_PASSWORD = (300, 294)  # 交易密码输入框
LOGIN_BUTTON = (240, 353)  # 登录按钮

# 主界面左侧一级导航
NAV_TRADE = (27, 87)

# 下单面板
PANEL_TAB_BUY = (269, 135)  # 竖排页签「买入」
PANEL_TAB_SELL = (269, 196)  # 竖排页签「卖出」
PANEL_PRICE_TYPE = (505, 131)  # 报价方式下拉框的箭头（点文本框不会展开）
PANEL_PRICE_TYPE_LIMIT = (404, 146)  # 下拉项「指定价」（优先按 OCR 定位）
PANEL_CODE = (564, 131)  # 证券代码输入框
PANEL_PRICE = (399, 342)  # 委托价输入框
PANEL_AMOUNT = (376, 437)  # 总量输入框
PANEL_SUBMIT = (569, 434)  # 「买入」/「卖出」按钮

# OCR 区域（x1, y1, x2, y2）
R_NAV_TABS = (70, 40, 150, 65)  # 顶部「股票交易」页签（判断是否在交易页）
R_SUBMIT = (500, 405, 640, 465)  # 下单按钮文字（判断买/卖面板）
R_PANEL = (255, 100, 720, 480)  # 下单面板
R_PRICE_TYPE = (372, 116, 552, 148)  # 报价方式当前值
R_DIALOG = (700, 380, 2000, 900)  # 下单确认弹窗 / 提示框

# 右下停靠列表页签
TAB_POSITION = (1015, 82)
TAB_ORDER = (1209, 82)
TAB_BALANCE = (1028, 705)

# 列表内右键位置（表格空白处同样有效）
RC_POSITION = (1300, 400)
RC_ORDER = (1300, 400)
RC_BALANCE = (1072, 820)
MENU_SIZE = (420, 300)  # 右键菜单相对点击点的包围盒

# 下单确认弹窗（「下达任务」）
DIALOG_CONFIRM = (1556, 787)

PROC_NAME = "XtItClient.exe"  # 启动闪屏窗口名，需排除
MAIN_WINDOW_KEY = "国金证券QMT交易端"  # 登录窗与主窗口标题都含此串

# ======================================================================
# 二、OCR
# ======================================================================

_OCR_ENGINE = None


def _get_ocr():
    global _OCR_ENGINE
    if _OCR_ENGINE is None:
        from rapidocr_onnxruntime import RapidOCR

        _OCR_ENGINE = RapidOCR()
    return _OCR_ENGINE


def ocr_image(img: Image.Image, scale: float = 2.0):
    """识别 PIL 图像，返回 [{'text','cx','cy'}]（坐标为原图坐标）。"""
    img = img.convert("RGB")
    if scale != 1.0:
        img = img.resize(
            (int(img.width * scale), int(img.height * scale)), Image.LANCZOS
        )
    res, _ = _get_ocr()(img)
    items = []
    for pts, text, _score in res or []:
        xs = [p[0] / scale for p in pts]
        ys = [p[1] / scale for p in pts]
        items.append(
            {
                "text": str(text).strip(),
                "cx": (min(xs) + max(xs)) / 2,
                "cy": (min(ys) + max(ys)) / 2,
            }
        )
    return items


def to_number(value) -> float:
    """界面/剪贴板文本转数字（兼容 '4 9275.00'、'7300股'、'-4.40%'）。"""
    if value is None:
        return 0.0
    text = re.sub(r"[^0-9.\-]", "", str(value).replace(" ", "").replace(",", ""))
    if text.count("-") > 1:
        text = "-" + text.replace("-", "")
    try:
        return float(text)
    except ValueError:
        return 0.0


def parse_tsv(text: str):
    """把「复制数据」得到的 TSV 文本解析成 [ {列名: 值} ]。"""
    lines = [ln for ln in (text or "").splitlines() if ln.strip()]
    if len(lines) < 2:
        return []
    header = [h.strip() for h in lines[0].split("\t")]
    rows = []
    for line in lines[1:]:
        cells = line.split("\t")
        cells += [""] * (len(header) - len(cells))
        rows.append(dict(zip(header, cells)))
    return rows


def cell(row: dict, *names):
    """按候选列名取值（同一列在不同版本可能叫法不同）。"""
    for name in names:
        if name in row and str(row[name]).strip() != "":
            return row[name]
    return ""


def _is_number_text(text: str) -> bool:
    """判断 OCR 文本是否是数字（'-+'、'股' 这类按钮文字不算）。"""
    return bool(re.fullmatch(r"[\d.,%+\-]+", text)) and any(c.isdigit() for c in text)


# ======================================================================
# 三、下单面板信息
# ======================================================================


@dataclass
class QmtPanelInfo:
    """从下单面板读到的股票信息。"""

    code: str = ""
    name: str = ""
    last: float = 0.0
    bid1: float = 0.0  # 买一价
    ask1: float = 0.0  # 卖一价
    can_buy: int = 0
    can_sell: int = 0
    max_buy: int = 0
    max_sell: int = 0
    order_price: float = 0.0
    order_amount: int = 0


# ======================================================================
# 四、GUI 交易客户端
# ======================================================================


class QMTGuiTrader:
    """通过 pyautogui + 剪贴板/OCR 操作 QMT 交易端。"""

    def __init__(self, exe_path: str, account: str, password: str, log=None):
        if not exe_path or not account or not password:
            raise ValueError("必须提供 QMT 客户端路径、资金账号、交易密码")

        self.exe_path = exe_path
        self.account = account
        self.password = password

        self._log = log or fun.get_logger("qmt_gui.log")
        self._win = None
        self._rect = None

        pyautogui.PAUSE = 0.1

    def log(self, msg: str):
        self._log.info("[QMT-GUI] %s", msg)

    # ------------------------------------------------------------------
    # 窗口 / 进程
    # ------------------------------------------------------------------
    def _find_process(self):
        target = os.path.normpath(self.exe_path).lower()
        for proc in psutil.process_iter(["pid", "exe"]):
            try:
                exe = proc.info.get("exe") or ""
            except Exception:  # noqa: BLE001
                continue
            if exe and os.path.normpath(exe).lower() == target:
                return proc.info
        return None

    def _windows(self, pid=None):
        """枚举本 QMT 进程的顶层窗口（排除启动闪屏）。"""
        from pywinauto import Desktop

        out = []
        for win in Desktop(backend="uia").windows():
            try:
                if pid is not None and win.process_id() != pid:
                    continue
                title = win.window_text() or ""
                cls = (win.class_name() or "").lower()
            except Exception:  # noqa: BLE001
                continue
            if title.strip().lower() == PROC_NAME.replace(".exe", "").lower():
                continue
            if cls.startswith("qt") or MAIN_WINDOW_KEY[:2] in title:
                out.append(win)
        return out

    def _cached_window(self):
        if self._win is None:
            return None
        try:
            return self._win if self._win.is_visible() else None
        except Exception:  # noqa: BLE001
            return None

    def window(self, refresh: bool = False):
        """返回 QMT 主窗口（标题带账号前缀的那个）。"""
        if not refresh:
            win = self._cached_window()
            if win is not None:
                return win

        proc = self._find_process()
        if proc is None:
            raise RuntimeError(f"QMT 进程未运行：{self.exe_path}")
        wins = self._windows(proc["pid"])
        if not wins:
            raise RuntimeError("未找到 QMT 主窗口")

        main = next(
            (w for w in wins if self.account in (w.window_text() or "")),
            max(wins, key=lambda w: w.rectangle().width() * w.rectangle().height()),
        )
        self._win = main
        self._rect = main.rectangle()
        return main

    def rect(self):
        win = self.window()
        try:
            self._rect = win.rectangle()
        except Exception:  # noqa: BLE001
            pass
        return self._rect

    def is_logged_in(self) -> bool:
        proc = self._find_process()
        if proc is None:
            return False
        return any(
            self.account in (w.window_text() or "") for w in self._windows(proc["pid"])
        )

    @staticmethod
    def _foreground(handle):
        """把窗口放到前台（模拟 ALT 键，否则 Windows 不允许抢前台）。"""
        import ctypes

        user32 = ctypes.windll.user32
        user32.ShowWindow(handle, 9)
        user32.keybd_event(0x12, 0, 0, 0)
        user32.keybd_event(0x12, 0, 2, 0)
        user32.SetForegroundWindow(handle)
        user32.BringWindowToTop(handle)
        time.sleep(0.6)

    def activate(self):
        """把主窗口放到前台（已在前台则跳过，避免每次都枚举桌面窗口）。"""
        import ctypes

        win = self._cached_window() or self.window(refresh=True)
        if ctypes.windll.user32.GetForegroundWindow() != win.handle:
            self._foreground(win.handle)
        self._rect = win.rectangle()
        return win

    # ------------------------------------------------------------------
    # 坐标换算 / 鼠标键盘
    # ------------------------------------------------------------------
    def to_screen(self, pos):
        rc = self.rect()
        return (
            int(rc.left + pos[0] * rc.width() / DESIGN_W),
            int(rc.top + pos[1] * rc.height() / DESIGN_H),
        )

    def _to_screen_region(self, region):
        rc = self.rect()
        x1, y1, x2, y2 = region
        return (
            int(rc.left + x1 * rc.width() / DESIGN_W),
            int(rc.top + y1 * rc.height() / DESIGN_H),
            max(1, int((x2 - x1) * rc.width() / DESIGN_W)),
            max(1, int((y2 - y1) * rc.height() / DESIGN_H)),
        )

    def click(self, pos, is_screen=False):
        x, y = pos if is_screen else self.to_screen(pos)
        pyautogui.click(int(x), int(y))
        time.sleep(0.35)

    def _type(self, text, clear=True):
        if clear:
            pyautogui.hotkey("ctrl", "a")
            time.sleep(0.1)
            pyautogui.press("delete")
            time.sleep(0.1)
        pyautogui.typewrite(str(text), interval=0.06)
        time.sleep(0.25)

    # ------------------------------------------------------------------
    # OCR 定位
    # ------------------------------------------------------------------
    def screenshot(self, region=None) -> Image.Image:
        return pyautogui.screenshot(region=region).convert("RGB")

    def ocr_region(self, region, scale: float = 2.0):
        """识别屏幕区域，返回带屏幕坐标的 OCR 结果。"""
        box = self._to_screen_region(region)
        items = ocr_image(self.screenshot(box), scale=scale)
        for it in items:
            it["cx"] += box[0]
            it["cy"] += box[1]
        return items

    def _click_text(self, region, text: str, fallback=None) -> bool:
        """在区域内找到文字并点击（屏幕坐标）。"""
        for it in self.ocr_region(region):
            if text in re.sub(r"\s", "", it["text"]):
                self.click((it["cx"], it["cy"]), is_screen=True)
                return True
        if fallback is not None:
            self.click(fallback)
            return True
        return False

    # ------------------------------------------------------------------
    # 登录
    # ------------------------------------------------------------------
    def start_client(self):
        self.log(f"启动 QMT：{self.exe_path}")
        subprocess.Popen(
            [self.exe_path],
            cwd=str(Path(self.exe_path).parent),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def ensure_login(self, timeout: int = 120) -> bool:
        """确保客户端已启动且已登录。"""
        proc = self._find_process()
        if proc is None:
            self.start_client()
            deadline = time.time() + 90
            while time.time() < deadline and proc is None:
                time.sleep(3)
                proc = self._find_process()
            if proc is None:
                self.log("QMT 启动超时")
                return False

        if self.is_logged_in():
            self.log("QMT 已登录")
            self.window(refresh=True)
            return True

        login_win, deadline = None, time.time() + timeout
        while time.time() < deadline and login_win is None:
            wins = [
                w
                for w in self._windows(proc["pid"])
                if self.account not in (w.window_text() or "")
            ]
            if wins:
                login_win = max(
                    wins, key=lambda w: w.rectangle().width() * w.rectangle().height()
                )
                break
            time.sleep(2)
        if login_win is None:
            self.log("未找到登录窗口")
            return False

        self.log("检测到登录窗口，选择「交易」标签后自动登录")
        self._do_login(login_win)

        deadline = time.time() + 90
        while time.time() < deadline:
            time.sleep(3)
            if self.is_logged_in():
                self.log("QMT 登录成功")
                self.window(refresh=True)
                return True
        self.log("QMT 登录失败")
        return False

    def _do_login(self, login_win):
        self._foreground(login_win.handle)
        rc = login_win.rectangle()

        def click(rel):
            pyautogui.click(rc.left + rel[0], rc.top + rel[1])
            time.sleep(0.4)

        click(LOGIN_TAB_TRADE)  # 只保留交易界面
        click(LOGIN_PASSWORD)
        self._type(self.password)
        click(LOGIN_BUTTON)
        time.sleep(1.0)

    # ------------------------------------------------------------------
    # 页面导航
    # ------------------------------------------------------------------
    def goto_trade_page(self):
        """确保停在「股票交易」页（只 OCR 顶部那一小块页签，开销很小）。"""
        self.activate()
        items = self.ocr_region(R_NAV_TABS)
        if not any("股票交易" in it["text"] for it in items):
            self.log("切换到交易页")
            self.click(NAV_TRADE)
            time.sleep(1.0)

    def _show_tab(self, tab_pos):
        self.goto_trade_page()
        self.click(tab_pos)
        time.sleep(0.5)

    def ready(self):
        """确保已登录并停在交易页。"""
        if not self.is_logged_in() and not self.ensure_login():
            raise RuntimeError("QMT 未登录")
        self.goto_trade_page()
        return self

    # ------------------------------------------------------------------
    # 列表数据：右键「复制数据」-> 剪贴板
    # ------------------------------------------------------------------
    def _copy_table(self, tab_pos, click_pos):
        """切到页签，右键「复制数据」，返回解析后的行列表。"""
        self._show_tab(tab_pos)
        pyperclip.copy("")
        pyautogui.rightClick(*self.to_screen(click_pos))
        time.sleep(0.7)

        region = (
            click_pos[0],
            click_pos[1],
            click_pos[0] + MENU_SIZE[0],
            click_pos[1] + MENU_SIZE[1],
        )
        if not self._click_text(region, "复制数据"):
            pyautogui.press("escape")
            time.sleep(0.3)
            raise RuntimeError("右键菜单未找到「复制数据」")

        time.sleep(0.45)
        text = pyperclip.paste()
        if not (text or "").strip():
            raise RuntimeError("「复制数据」后剪贴板为空")
        return parse_tsv(text)

    def positions(self, code: str = ""):
        """查询持仓。"""
        rows = self._copy_table(TAB_POSITION, RC_POSITION)
        result = []
        for row in rows:
            pure = re.sub(r"\D", "", cell(row, "证券代码"))
            if len(pure) != 6:
                continue
            result.append(
                {
                    "code": self.pure_to_tdx(pure),
                    "name": cell(row, "证券名称"),
                    "amount": int(to_number(cell(row, "当前拥股", "当前股数"))),
                    "can_sell_amount": int(to_number(cell(row, "可用数量"))),
                    "frozen_amount": int(to_number(cell(row, "冻结数量"))),
                    "price": to_number(cell(row, "成本价")),
                    "last_price": to_number(cell(row, "最新价")),
                    "market_value": to_number(cell(row, "市值")),
                    "position_profit": to_number(cell(row, "持仓盈亏")),
                    "profit": to_number(cell(row, "盈亏比例")),
                    "position_cost": to_number(cell(row, "持仓成本")),
                }
            )
        if code:
            tdx = self.pure_to_tdx(self.code_to_pure(code))
            result = [p for p in result if p["code"] == tdx]
        return result

    def orders(self, code: str = ""):
        """查询当日委托（含已撤/废单）。"""
        rows = self._copy_table(TAB_ORDER, RC_ORDER)
        result = []
        for row in rows:
            pure = re.sub(r"\D", "", cell(row, "证券代码"))
            if len(pure) != 6:
                continue
            result.append(
                {
                    "order_id": re.sub(r"\s", "", cell(row, "合同编号")),
                    "local_id": re.sub(r"\s", "", cell(row, "订单编号")),
                    "code": self.pure_to_tdx(pure),
                    "name": cell(row, "证券名称"),
                    "direction": cell(row, "买卖标记"),
                    "order_volume": int(to_number(cell(row, "委托量"))),
                    "remain_volume": int(to_number(cell(row, "委托剩余量"))),
                    "traded_volume": int(to_number(cell(row, "成交数量"))),
                    "status": cell(row, "委托状态"),
                    "price": to_number(cell(row, "委托价格")),
                    "traded_price": to_number(cell(row, "成交均价")),
                    "status_msg": cell(row, "废单原因"),
                    "order_time": cell(row, "委托时间"),
                }
            )
        if code:
            tdx = self.pure_to_tdx(self.code_to_pure(code))
            result = [o for o in result if o["code"] == tdx]
        return result

    def balance(self):
        """查询账号资金。"""
        rows = self._copy_table(TAB_BALANCE, RC_BALANCE)
        row = rows[0] if rows else {}
        total_assets = to_number(cell(row, "总资产"))
        return {
            "account": cell(row, "资金账号"),
            "account_name": cell(row, "账号名称"),
            "total_money": total_assets,
            "cash": to_number(cell(row, "可用金额")),
            "market_value": to_number(cell(row, "总市值", "股票总市值")),
            "total_assets": total_assets,
            "position_profit": to_number(cell(row, "持仓盈亏")),
            "total_debit": to_number(cell(row, "总负债")),
            "assure_asset": to_number(cell(row, "净资产")),
            "withdraw_cash": to_number(cell(row, "可取金额")),
            "today_profit": to_number(cell(row, "今日账号盈亏")),
        }

    def order_detail(self, order_id: str):
        """按合同编号/订单编号查询单笔委托。"""
        for order in self.orders():
            if order_id in (order["order_id"], order["local_id"]):
                return {"order": order}
        return {"order": None}

    # ------------------------------------------------------------------
    # 下单面板
    # ------------------------------------------------------------------
    def submit_button_label(self) -> str:
        """读下单按钮文字（买入/卖出），确认面板处于哪一侧。"""
        for it in self.ocr_region(R_SUBMIT):
            text = re.sub(r"\s", "", it["text"])
            if text in ("买入", "卖出"):
                return text
        return ""

    def ensure_panel(self, want_buy: bool, retry: int = 3) -> bool:
        """把下单面板切到「买入」或「卖出」。"""
        want = "买入" if want_buy else "卖出"
        for _ in range(retry):
            self.goto_trade_page()
            self.click(PANEL_TAB_BUY if want_buy else PANEL_TAB_SELL)
            time.sleep(0.7)
            if self.submit_button_label() == want:
                return True
        self.log(f"下单面板未能切到「{want}」")
        return False

    def set_price_type_limit(self, retry: int = 3) -> bool:
        """把报价方式切成「指定价」，使委托价可输入；切换后回读确认。"""
        for _ in range(retry):
            if any("指定价" in it["text"] for it in self.ocr_region(R_PRICE_TYPE)):
                return True
            self.click(PANEL_PRICE_TYPE)  # 展开下拉
            time.sleep(0.6)
            dropdown = (360, PANEL_PRICE_TYPE[1] + 2, 640, 300)
            if not self._click_text(dropdown, "指定价", PANEL_PRICE_TYPE_LIMIT):
                pyautogui.press("escape")
            time.sleep(0.4)
        self.log("报价方式未能切到「指定价」")
        return False

    def set_code(self, code: str, retry: int = 3) -> QmtPanelInfo:
        """输入证券代码，并回读面板确认。"""
        pure = self.code_to_pure(code)
        for _ in range(retry):
            self.click(PANEL_CODE)
            self._type(pure)
            pyautogui.press("enter")
            time.sleep(1.0)
            info = self.read_panel()
            if info.code == pure:
                return info
        return self.read_panel()

    def read_panel(self) -> QmtPanelInfo:
        """读取下单面板：代码 / 名称 / 最新价 / 可买 / 可卖 / 委托价 / 总量。"""
        items = self.ocr_region(R_PANEL)
        info = QmtPanelInfo()

        for it in items:  # 名称：右上角那一行
            if 400 <= it["cx"] <= 900 and 100 <= it["cy"] <= 122:
                text = re.sub(r"\s", "", it["text"])
                if text and not re.fullmatch(r"[\d.,%+\-]+", text):
                    info.name = it["text"]
                    break
        for it in items:  # 代码：名称下方那一行
            if 500 <= it["cx"] <= 700 and 122 <= it["cy"] <= 142:
                m = re.search(r"(\d{6})", it["text"])
                if m:
                    info.code = m.group(1)
                    break
        info.last = to_number(self._value_right_of(items, "最新"))

        # 五档价格列（x≈604）：卖一在「最新」上方、买一在下方，取紧邻的两行
        last_item = next(
            (it for it in items if re.sub(r"\s", "", it["text"]) == "最新"), None
        )
        if last_item is not None:
            prices = [
                it
                for it in items
                if 575 <= it["cx"] <= 635 and to_number(it["text"]) > 0
            ]
            above = [it for it in prices if it["cy"] < last_item["cy"] - 5]
            below = [it for it in prices if it["cy"] > last_item["cy"] + 5]
            if above:
                info.ask1 = to_number(max(above, key=lambda i: i["cy"])["text"])
            if below:
                info.bid1 = to_number(min(below, key=lambda i: i["cy"])["text"])

        # 左侧「标签 + 数值」行（OCR 可能把标签拆成多段，按行聚合再匹配）
        for row in self._group_rows([it for it in items if it["cx"] < 500]):
            row.sort(key=lambda it: it["cx"])
            texts = [re.sub(r"\s", "", it["text"]) for it in row]
            nums = [t for t in texts if _is_number_text(t)]
            labels = "".join(t for t in texts if not _is_number_text(t))
            for label, attr, caster in (
                ("最大可买", "max_buy", int),
                ("最大可卖", "max_sell", int),
                ("可买", "can_buy", int),
                ("可卖", "can_sell", int),
                ("委托价", "order_price", float),
                ("总量", "order_amount", int),
            ):
                if label in labels and nums and not getattr(info, attr, None):
                    setattr(info, attr, caster(to_number(nums[0])))
        if info.last <= 0:
            info.last = info.order_price
        return info

    @staticmethod
    def _group_rows(items, tol: float = 12.0):
        rows = []
        for it in sorted(items, key=lambda i: i["cy"]):
            for row in rows:
                if abs(it["cy"] - row[0]["cy"]) <= tol:
                    row.append(it)
                    break
            else:
                rows.append([it])
        return rows

    @staticmethod
    def _value_right_of(items, label: str):
        anchor = next(
            (it for it in items if re.sub(r"\s", "", it["text"]) == label), None
        )
        if anchor is None:
            return None
        best = None
        for it in items:
            if it["cx"] <= anchor["cx"] + 2 or abs(it["cy"] - anchor["cy"]) > 9:
                continue
            if best is None or it["cx"] < best["cx"]:
                best = it
        return best["text"] if best else None

    def set_price(self, price: float):
        self.click(PANEL_PRICE)
        self._type(f"{price:.2f}")

    def set_amount(self, amount: int):
        self.click(PANEL_AMOUNT)
        self._type(str(int(amount)))

    def panel_info(self, code: str, want_buy: bool = True) -> QmtPanelInfo:
        """切到下单面板并录入代码，返回面板信息（名称/最新价等）。"""
        if not self.ensure_panel(want_buy):
            return QmtPanelInfo()
        return self.set_code(code)

    def last_price(self, code: str) -> float:
        return self.panel_info(code, want_buy=True).last

    # ------------------------------------------------------------------
    # 下单
    # ------------------------------------------------------------------
    def buy(self, code: str, amount: int, price: float = 0.0):
        return self.order(code, "buy", amount, price)

    def sell(self, code: str, amount: int, price: float = 0.0):
        return self.order(code, "sell", amount, price)

    def order(self, code: str, o_type: str, amount: int, price: float = 0.0):
        """按给定数量下单（不做目标持仓换算）。

        Args:
            price: 委托价；<=0 时用面板带出的最新价

        Returns:
            dict: {price, amount, order_id, status, done}
        """
        is_buy = o_type == "buy"
        amount = int(amount)
        if is_buy:
            amount = amount // 100 * 100
        if amount <= 0:
            return self._fail("下单数量无效")

        if not self.ensure_panel(want_buy=is_buy):
            return self._fail(f"下单面板未能切到{'买入' if is_buy else '卖出'}")

        info = self.set_code(code)
        if not info.code:
            return self._fail(f"下单面板无法确认代码 {code}")

        if not is_buy:
            if info.can_sell <= 0:
                return self._fail(f"{code} 可卖数量为 0")
            if amount > info.can_sell:
                self.log(f"卖出数量 {amount} 超过可卖 {info.can_sell}，按可卖数量下单")
                amount = info.can_sell

        order_price = float(price) if price else info.last
        if order_price <= 0:
            return self._fail(f"{code} 未取到有效价格")

        if not self.set_price_type_limit():
            return self._fail("报价方式未能切换到「指定价」")

        self.set_price(order_price)
        self.set_amount(amount)
        time.sleep(0.3)

        # 回读面板，确认价格/数量写进去了（避免点错字段就提交）
        verify = self.read_panel()
        if (
            abs(verify.order_price - order_price) > 0.005
            or verify.order_amount != amount
        ):
            self.set_price(order_price)
            self.set_amount(amount)
            time.sleep(0.3)
            verify = self.read_panel()
        if (
            abs(verify.order_price - order_price) > 0.005
            or verify.order_amount != amount
        ):
            return self._fail(
                f"下单面板填写失败（价 {verify.order_price} / 量 {verify.order_amount}）"
            )

        return self._submit(is_buy, info.code, amount, order_price)

    def _submit(self, is_buy: bool, code: str, amount: int, price: float):
        side = "买入" if is_buy else "卖出"
        before_ids = {o["order_id"] for o in self.orders(code)}

        self.log(f"点击「{side}」：{code} {price:.2f} x {amount}")
        self.click(PANEL_SUBMIT)
        time.sleep(1.3)

        items = self.ocr_region(R_DIALOG)
        text = " ".join(it["text"] for it in items)

        # 正常下单确认弹窗「下达任务」
        if "下达任务" in text:
            self._click_text(R_DIALOG, "确定", DIALOG_CONFIRM)
            time.sleep(1.5)
            self._dismiss_popup()
            order = self._find_new_order(code, is_buy, amount, price, before_ids)
            if order is None:
                return self._fail(
                    "已提交但未回读到委托", price=price, amount=amount, done=True
                )
            accepted = order["status"] in ("已报", "已成", "部分成交")
            self.log(f"{side}结果：{order['status']} {code} x{order['order_volume']}")
            return {
                "price": price,
                "amount": order["order_volume"] if accepted else 0,
                "order_id": order["order_id"],
                "status": order["status"]
                + (f" {order['status_msg']}" if order["status_msg"] else ""),
                "done": accepted,
            }

        # 其它提示框（资金不足 / 价格越界 / 非交易时间等）
        msg = max(
            (it["text"] for it in items if len(it["text"]) >= 6),
            key=len,
            default="下单被拦截",
        )
        self.log(f"下单被拦截：{msg}")
        self._click_text(R_DIALOG, "确定")
        return self._fail(msg, price=price)

    def _dismiss_popup(self):
        """下单后若弹出提示框，点掉「确定」。"""
        items = self.ocr_region(R_DIALOG)
        if any("下达任务" in it["text"] for it in items):
            return
        self._click_text(R_DIALOG, "确定")

    def _find_new_order(self, code, is_buy, amount, price, before_ids, timeout=8):
        deadline = time.time() + timeout
        while True:
            for order in self.orders(code):
                if order["order_id"] in before_ids or order["order_volume"] != amount:
                    continue
                if abs(order["price"] - price) > 0.005:
                    continue
                if ("买入" in order["direction"]) != is_buy:
                    continue
                return order
            if time.time() >= deadline:
                return None
            time.sleep(1.5)

    @staticmethod
    def _fail(status: str, price: float = 0.0, amount: int = 0, done: bool = False):
        return {
            "price": price,
            "amount": amount,
            "order_id": "",
            "status": status,
            "done": done,
        }

    # ------------------------------------------------------------------
    # 按目标持仓数量交易（对齐 ExchangeQMT.order 的语义）
    # ------------------------------------------------------------------
    def trade_target(
        self,
        code: str,
        o_type: str,
        amount: int,
        price: float = 0.0,
        args: dict | None = None,
    ):
        """把持仓调整到目标数量：先查当前持仓，按差值下单；未成交则撤单重下。

        Args:
            code: 证券代码（SH.603811 / 603811）
            o_type: buy / sell
            amount: 目标持仓数量（股），买入向下取整到 100 的整数倍
            price: 委托价，0 表示用面板最新价
            args: max_attempt 最大下单次数（默认 5）、wait_fill 每笔等待成交秒数（默认 3）

        Returns:
            dict: {price, amount, done, attempt, position}
                  amount 为本次实际成交数量，position 为最终持仓数量
        """
        args = args or {}
        max_attempt = int(args.get("max_attempt", 3))
        wait_fill = float(args.get("wait_fill", 4.0))
        price = float(args.get("price", price) or 0)

        is_buy = o_type == "buy"
        target = int(amount)
        if is_buy:
            target = target // 100 * 100

        start_amount = self.position_amount(code)
        total_filled, traded_price, attempt, order_id = 0, 0.0, 0, ""

        # 每一轮都按「实际持仓」重算差值：即使上一笔没被正确识别，
        # 也不会重复下单超买/超卖。
        while attempt < max_attempt:
            current = self.position_amount(code)
            if is_buy:
                need = target - current
            else:
                # 卖出受可用数量限制（A股 T+1）
                need = min(current - target, self.position_amount(code, can_sell=True))
            if need <= 0:
                break

            attempt += 1
            res = self.order(code, o_type, need, price=price)
            order_id = res["order_id"] or order_id
            if not res["done"]:
                self.log(f"目标交易中断：{res['status']}")
                break
            traded_price = traded_price or res["price"]

            filled = self._wait_fill(order_id, wait_fill)
            total_filled += filled
            if filled > 0:
                continue
            if order_id:  # 未成交 -> 撤单，下一轮按最新持仓重算
                self.order_cancel(order_id)
            time.sleep(1.0)

        final_amount = self.position_amount(code)
        traded = (
            max(0, final_amount - start_amount)
            if is_buy
            else max(0, start_amount - final_amount)
        )
        return {
            "price": traded_price or price,
            "amount": traded or total_filled,
            "done": (is_buy and final_amount >= target)
            or (not is_buy and final_amount <= target),
            "attempt": attempt,
            "position": final_amount,
        }

    def position_amount(self, code: str, can_sell: bool = False) -> int:
        """查询指定代码的持仓数量（can_sell=True 时取可用数量）。"""
        positions = self.positions(code)
        if not positions:
            return 0
        return positions[0]["can_sell_amount" if can_sell else "amount"]

    def _wait_fill(self, order_id: str, timeout: float) -> int:
        """轮询委托列表，返回该笔委托的已成交数量（成交即返回）。"""
        deadline = time.time() + timeout
        while True:
            for order in self.orders():
                if order["order_id"] == order_id and order["traded_volume"] > 0:
                    return order["traded_volume"]
            if time.time() >= deadline:
                return 0
            time.sleep(0.5)

    # ------------------------------------------------------------------
    # 撤单
    # ------------------------------------------------------------------
    def order_cancel(self, order_id: str = "", code: str = ""):
        """撤销委托：按合同编号（或订单编号、证券代码）定位委托行后撤单。"""
        self._show_tab(TAB_ORDER)
        items = self.ocr_region((990, 70, 2560, 500))
        select_x = next(
            (int(it["cx"]) for it in items if re.sub(r"\s", "", it["text"]) == "选择"),
            None,
        )

        want_id = re.sub(r"\D", "", str(order_id or ""))
        want_code = re.sub(r"\D", "", code or "")
        table = self._copy_table(TAB_ORDER, RC_ORDER)

        target_y = None
        for idx, row in enumerate(table):
            cid = re.sub(r"\D", "", cell(row, "合同编号"))
            lid = re.sub(r"\D", "", cell(row, "订单编号"))
            if want_id and any(want_id in got for got in (cid, lid) if got):
                target_y = idx
                break
            if want_code and re.sub(r"\D", "", cell(row, "证券代码")) == want_code:
                target_y = idx
                break

        if target_y is None:
            self.log(f"委托列表未找到可撤委托：order_id={order_id} code={code}")
            return {"canceled": False, "order_id": order_id, "status": "未找到委托"}

        # 数据行 y：表头下一行起，每行 22px（与界面行高一致）
        row_y = 151 + target_y * 22
        self.click((select_x or 1078, row_y), is_screen=True)
        time.sleep(0.4)
        if not self._click_text((990, 92, 1700, 120), "选中撤单"):
            self.log("未找到「选中撤单」按钮")
            return {"canceled": False, "order_id": order_id, "status": "未找到撤单按钮"}
        time.sleep(1.2)
        self._dismiss_popup()
        self.log(f"已撤单：order_id={order_id} code={code}")
        return {"canceled": True, "order_id": order_id, "status": "已提交撤单"}

    # ------------------------------------------------------------------
    # 代码格式
    # ------------------------------------------------------------------
    @staticmethod
    def code_to_pure(code: str) -> str:
        """SH.603811 / 603811.SH -> 603811。"""
        for part in re.split(r"[.\s]", str(code).strip()):
            if re.fullmatch(r"\d{6}", part):
                return part
        return "".join(re.findall(r"\d{6}", str(code)))[:6]

    @staticmethod
    def pure_to_tdx(pure: str) -> str:
        """603811 -> SH.603811。"""
        code = str(pure).strip()
        if len(code) != 6 or not code.isdigit():
            return code
        if code[0] in "659":
            return f"SH.{code}"
        if code[0] in "0123":
            return f"SZ.{code}"
        if code[0] in "48":
            return f"BJ.{code}"
        return f"SH.{code}"

    def close(self):
        self._win = None
        self._rect = None


# ======================================================================
# 五、自检
# ======================================================================


def _main():
    import argparse
    import json

    parser = argparse.ArgumentParser(description="QMT 交易端 GUI 自动化自检")
    parser.add_argument("--exe", required=True, help="XtItClient.exe 路径")
    parser.add_argument("--account", required=True, help="资金账号")
    parser.add_argument("--password", required=True, help="交易密码")
    parser.add_argument("--login", action="store_true", help="未登录则自动启动并登录")
    args = parser.parse_args()

    trader = QMTGuiTrader(args.exe, args.account, args.password)
    if args.login:
        trader.ensure_login()
    trader.ready()

    print("资金：", json.dumps(trader.balance(), ensure_ascii=False))
    print("持仓：", json.dumps(trader.positions(), ensure_ascii=False))
    print("委托：", json.dumps(trader.orders(), ensure_ascii=False))


if __name__ == "__main__":
    _main()
