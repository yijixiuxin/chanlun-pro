"""
统一的密钥/凭证安全工具：

- ``get_flask_secret_key()``：返回 Flask 会话密钥；按 env > config > 持久化文件 顺序解析。
- ``get_fernet()`` / ``encrypt_str`` / ``decrypt_str``：基于 Flask 密钥派生的对称加密，
  用于把飞书 app_secret 等敏感配置加密落库。
- ``mask_secret()``：用于前端回显时遮蔽中间字符。
- ``verify_login_password()``：常量时间比较登录密码，并兼容 ``pbkdf2:`` 哈希形式。

约束：
- 不抛出运行时致命异常；密钥派生失败时退化为可读的 ValueError。
- 历史明文 fs_app_secret 在 ``decrypt_str()`` 中视为旧数据原样返回，调用方下次写入会自动改写为密文。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken

from chanlun import config


_FERNET_SALT = b"chanlun_pro_fs_keys_v1"  # 固定 salt：保证同一 SECRET_KEY 派生出稳定的 Fernet 密钥
_SECRET_FILE_NAME = ".flask_secret_key"


def _persisted_secret_path():
    """密钥持久化文件路径；放在用户数据目录下，避免和源码混在一起。"""
    return config.get_data_path() / _SECRET_FILE_NAME


def get_flask_secret_key() -> str:
    """
    解析顺序：
    1. 环境变量 ``CHANLUN_FLASK_SECRET_KEY``
    2. ``config.FLASK_SECRET_KEY``（用户在 config.py 显式配置）
    3. 数据目录下 ``.flask_secret_key`` 文件（首次运行随机生成 32 字节并写入）
    """
    env_key = os.environ.get("CHANLUN_FLASK_SECRET_KEY")
    if env_key:
        return env_key

    cfg_key = getattr(config, "FLASK_SECRET_KEY", "") or ""
    if cfg_key:
        return cfg_key

    path = _persisted_secret_path()
    if path.exists():
        key = path.read_text(encoding="utf-8").strip()
        if key:
            return key

    key = secrets.token_hex(32)
    path.write_text(key, encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except (OSError, NotImplementedError):
        pass  # Windows 下 chmod 没有等价行为，忽略
    return key


def get_fernet() -> Fernet:
    """从 Flask SECRET_KEY 派生 Fernet 实例（PBKDF2-HMAC-SHA256，固定 salt）。"""
    secret = get_flask_secret_key().encode("utf-8")
    derived = hashlib.pbkdf2_hmac("sha256", secret, _FERNET_SALT, 100_000, dklen=32)
    return Fernet(base64.urlsafe_b64encode(derived))


_ENC_PREFIX = "enc::v1::"


def encrypt_str(plaintext: Optional[str]) -> str:
    """加密字符串；空值原样返回空字符串。带版本前缀以便日后轮换。"""
    if not plaintext:
        return ""
    token = get_fernet().encrypt(plaintext.encode("utf-8")).decode("ascii")
    return _ENC_PREFIX + token


def decrypt_str(value: Optional[str]) -> str:
    """
    解密字符串：
    - 空值返回空字符串
    - 带 ``enc::v1::`` 前缀按密文解密
    - 否则视为历史明文原样返回（调用方下次写入会自动改写为密文）
    """
    if not value:
        return ""
    if not value.startswith(_ENC_PREFIX):
        return value
    token = value[len(_ENC_PREFIX):]
    try:
        return get_fernet().decrypt(token.encode("ascii")).decode("utf-8")
    except (InvalidToken, ValueError):
        return ""  # 密钥变更等导致解密失败：返回空，调用方将走默认配置


def mask_secret(value: Optional[str], head: int = 2, tail: int = 2) -> str:
    """前端回显遮蔽：保留首尾少量字符，中间用 ``*`` 代替；过短则全部遮蔽。"""
    if not value:
        return ""
    if len(value) <= head + tail:
        return "*" * len(value)
    return f"{value[:head]}{'*' * (len(value) - head - tail)}{value[-tail:]}"


def verify_login_password(submitted: str, expected: str) -> bool:
    """
    登录密码验证：
    - ``expected`` 以 ``pbkdf2:`` / ``scrypt:`` / ``argon2:`` 开头时走 ``werkzeug.security``
      的安全哈希校验。
    - 否则按明文常量时间比较（``hmac.compare_digest``），兼容现有用户的 ``LOGIN_PWD`` 配置。
    """
    if submitted is None or expected is None:
        return False
    if expected.startswith(("pbkdf2:", "scrypt:", "argon2:")):
        from werkzeug.security import check_password_hash
        try:
            return check_password_hash(expected, submitted)
        except (ValueError, TypeError):
            return False
    return hmac.compare_digest(submitted.encode("utf-8"), expected.encode("utf-8"))
