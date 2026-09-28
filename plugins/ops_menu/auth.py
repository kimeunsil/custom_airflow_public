"""Ops Menu 접근 제어: 로그인 + 허용 역할(OPS_MENU_ROLES, 기본 Admin,Op)."""

from __future__ import annotations

import os
from functools import wraps

from flask import abort
from flask_login import current_user

ALLOWED_ROLES = {r.strip() for r in os.environ.get("OPS_MENU_ROLES", "Admin,Op").split(",") if r.strip()}


def current_username() -> str:
    return getattr(current_user, "username", None) or "anonymous"


def require_ops_role(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if not getattr(current_user, "is_authenticated", False):
            abort(401)
        roles = {r.name for r in getattr(current_user, "roles", [])}
        if not roles & ALLOWED_ROLES:
            abort(403)
        return view(*args, **kwargs)

    return wrapper
