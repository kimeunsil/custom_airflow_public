"""SQL 파일 경로 → DAG 명세 해석 (Airflow 의존성 없음, 단위 테스트 대상).

디렉터리 규칙::

    sql/daily/<HH>/<dag_id>.sql                 매일 HH시
    sql/weekly/<요일>/<HH>/<dag_id>.sql          매주 <요일> HH시   (monday ... sunday)
    sql/monthly/D<일>/<HH>/<dag_id>.sql          매월 <일>일 HH시

파일 첫머리의 주석으로 추가 설정을 선언할 수 있다::

    -- description: 일별 활성 사용자 집계
    -- tags: mart, user
    -- depends_on: my-project.raw.events:event_date, my-project.raw.users:updated_date
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

WEEKDAYS = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]  # cron: 0=일
_DAG_ID = re.compile(r"^[A-Za-z0-9_.-]+$")
_HEADER = re.compile(r"^\s*--\s*(description|tags|depends_on)\s*:\s*(.*)$", re.IGNORECASE)


class SqlDagSpecError(ValueError):
    pass


@dataclass(frozen=True)
class Dependency:
    table: str  # project.dataset.table
    partition_column: str


@dataclass
class SqlDagSpec:
    dag_id: str
    schedule: str
    cadence: str  # daily | weekly | monthly
    sql: str
    path: str
    description: str = ""
    tags: list[str] = field(default_factory=list)
    depends_on: list[Dependency] = field(default_factory=list)


def _hour(part: str, path: Path) -> int:
    if not part.isdigit() or not 0 <= int(part) <= 23:
        raise SqlDagSpecError(f"{path}: 시간 디렉터리는 0~23 이어야 합니다 (현재 '{part}')")
    return int(part)


def schedule_from_path(rel: Path) -> tuple[str, str]:
    """sql 루트 기준 상대경로 → (cron, cadence)"""
    parts = rel.parts
    cadence = parts[0] if parts else ""
    if cadence == "daily" and len(parts) == 3:
        return f"0 {_hour(parts[1], rel)} * * *", cadence
    if cadence == "weekly" and len(parts) == 4:
        day = parts[1].lower()
        if day not in WEEKDAYS:
            raise SqlDagSpecError(f"{rel}: 요일 디렉터리는 {WEEKDAYS} 중 하나여야 합니다")
        return f"0 {_hour(parts[2], rel)} * * {WEEKDAYS.index(day)}", cadence
    if cadence == "monthly" and len(parts) == 4:
        m = re.fullmatch(r"D(\d{1,2})", parts[1])
        if not m or not 1 <= int(m.group(1)) <= 28:
            raise SqlDagSpecError(f"{rel}: 월간 디렉터리는 D1~D28 형식이어야 합니다")
        return f"0 {_hour(parts[2], rel)} {int(m.group(1))} * *", cadence
    raise SqlDagSpecError(
        f"{rel}: 경로 규칙 위반 — daily/<HH>/, weekly/<요일>/<HH>/, monthly/D<일>/<HH>/ 중 하나여야 합니다"
    )


def parse_header(sql: str) -> dict:
    meta: dict = {"description": "", "tags": [], "depends_on": []}
    for line in sql.splitlines():
        if not line.strip():
            continue
        m = _HEADER.match(line)
        if not m:
            if line.lstrip().startswith("--"):
                continue  # 일반 주석은 건너뛰고 계속 헤더로 본다
            break  # 첫 SQL 문장부터는 헤더 아님
        key, value = m.group(1).lower(), m.group(2).strip()
        if key == "description":
            meta["description"] = value
        elif key == "tags":
            meta["tags"] = [t.strip() for t in value.split(",") if t.strip()]
        elif key == "depends_on":
            for item in filter(None, (v.strip() for v in value.split(","))):
                table, sep, col = item.partition(":")
                if not sep or table.count(".") != 2 or not col:
                    raise SqlDagSpecError(f"depends_on 형식은 project.dataset.table:partition_column 입니다 ({item})")
                meta["depends_on"].append(Dependency(table, col))
    return meta


def discover(sql_root: str | Path) -> list[SqlDagSpec]:
    root = Path(sql_root)
    specs: list[SqlDagSpec] = []
    for path in sorted(root.rglob("*.sql")):
        rel = path.relative_to(root)
        schedule, cadence = schedule_from_path(rel)
        dag_id = path.stem
        if not _DAG_ID.match(dag_id):
            raise SqlDagSpecError(f"{rel}: DAG id 로 쓸 수 없는 파일명입니다")
        sql = path.read_text(encoding="utf-8")
        meta = parse_header(sql)
        specs.append(SqlDagSpec(dag_id=dag_id, schedule=schedule, cadence=cadence, sql=sql, path=str(rel), **meta))

    seen: dict[str, str] = {}
    for s in specs:
        if s.dag_id in seen:
            raise SqlDagSpecError(f"DAG id 중복: {s.dag_id} ({seen[s.dag_id]}, {s.path})")
        seen[s.dag_id] = s.path
    return specs
