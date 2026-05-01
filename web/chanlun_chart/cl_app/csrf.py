"""
CSRF 保护单例。

放在独立模块中而非 ``cl_app/__init__.py``，避免蓝图模块（如 ``tv.py``
需要使用 ``@csrf.exempt``）形成 ``cl_app -> blueprints -> cl_app`` 的循环导入。
"""

from flask_wtf.csrf import CSRFProtect

csrf = CSRFProtect()
