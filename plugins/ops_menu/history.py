"""Ops Menu 작업 이력 (logs/ops_menu/history.jsonl)."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from airflow.configuration import conf


def ops_dir() -> Path:
    d = Path(conf.get("logging", "base_log_folder")) / "ops_menu"
    d.mkdir(parents=True, exist_ok=True)
    return d


def record(action: str, user: str, **detail) -> None:
    entry = {"at": datetime.now(UTC).isoformat(timespec="seconds"), "user": user, "action": action, **detail}
    with open(ops_dir() / "history.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")


def recent(limit: int = 50) -> list[dict]:
    path = ops_dir() / "history.jsonl"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()[-limit:]
    out = []
    for line in reversed(lines):
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def tail(path: Path, max_bytes: int = 200_000) -> str:
    if not path.exists():
        return ""
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        f.seek(max(0, size - max_bytes))
        return f.read().decode("utf-8", errors="replace")
