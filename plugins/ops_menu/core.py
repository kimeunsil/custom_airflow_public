"""플러그인의 순수 로직 (Flask/DB 없이 테스트 가능)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime

import pendulum

_DAG_ID = re.compile(r"^[A-Za-z0-9_.-]{1,250}$")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ValidationError(ValueError):
    pass


# ── Backfill ────────────────────────────────────────────────────


@dataclass(frozen=True)
class BackfillRequest:
    dag_id: str
    start_date: date
    end_date: date
    mode: str  # "backfill" | "clear"
    reset_dagruns: bool = False
    only_failed: bool = False


MAX_BACKFILL_DAYS = 366


def parse_backfill_request(form: dict) -> BackfillRequest:
    dag_id = (form.get("dag_id") or "").strip()
    if not _DAG_ID.match(dag_id):
        raise ValidationError("dag_id 형식이 올바르지 않습니다")
    raw_start, raw_end = (form.get("start_date") or "").strip(), (form.get("end_date") or "").strip()
    try:
        if not (_ISO_DATE.match(raw_start) and _ISO_DATE.match(raw_end)):
            raise ValueError
        start, end = date.fromisoformat(raw_start), date.fromisoformat(raw_end)
    except ValueError as e:
        raise ValidationError("날짜는 YYYY-MM-DD 형식이어야 합니다") from e
    if start > end:
        raise ValidationError("시작일이 종료일보다 늦습니다")
    if (end - start).days > MAX_BACKFILL_DAYS:
        raise ValidationError(f"한 번에 {MAX_BACKFILL_DAYS}일을 넘길 수 없습니다")
    mode = form.get("mode", "backfill")
    if mode not in ("backfill", "clear"):
        raise ValidationError("mode 는 backfill 또는 clear 입니다")
    truthy = ("1", "true", "on", "yes")
    return BackfillRequest(
        dag_id=dag_id,
        start_date=start,
        end_date=end,
        mode=mode,
        reset_dagruns=str(form.get("reset_dagruns", "")).lower() in truthy,
        only_failed=str(form.get("only_failed", "")).lower() in truthy,
    )


def build_command(req: BackfillRequest) -> list[str]:
    """셸을 거치지 않는 인자 리스트로 만든다 (입력값이 명령으로 해석될 여지 없음)."""
    s, e = req.start_date.isoformat(), req.end_date.isoformat()
    if req.mode == "clear":
        cmd = ["airflow", "tasks", "clear", "--start-date", s, "--end-date", e, "--yes"]
        if req.only_failed:
            cmd.append("--only-failed")
    else:
        cmd = ["airflow", "dags", "backfill", "--start-date", s, "--end-date", e]
        if req.reset_dagruns:
            cmd += ["--reset-dagruns", "--yes"]
        if req.only_failed:
            cmd.append("--rerun-failed-tasks")
    cmd.append(req.dag_id)
    return cmd


# ── Schedule-window pause ───────────────────────────────────────


@dataclass(frozen=True)
class Window:
    start_hour: int  # 포함
    end_hour: int  # 미포함, 24 = 자정


def parse_window(form: dict) -> Window:
    try:
        start, end = int(form.get("start_hour", "")), int(form.get("end_hour", ""))
    except ValueError as e:
        raise ValidationError("시작/종료 시각은 정수(시)여야 합니다") from e
    if not (0 <= start < end <= 24):
        raise ValidationError("0 ≤ 시작 < 종료 ≤ 24 이어야 합니다")
    return Window(start, end)


def window_bounds_utc(day: date, window: Window, tz) -> tuple[datetime, datetime]:
    """로컬 날짜·시간대 구간 → UTC [start, end)"""
    base = pendulum.datetime(day.year, day.month, day.day, tz=tz)
    return (
        base.add(hours=window.start_hour).in_timezone("UTC"),
        base.add(hours=window.end_hour).in_timezone("UTC"),
    )
