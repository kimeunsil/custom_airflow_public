"""태스크/스케줄러 로그 파일 정리.

보관 일수는 Variable `maintenance_log_retention_days`(기본 30).
Variable `maintenance_log_cleanup_dry_run` 이 "true" 면 삭제 대상만 출력한다.
logs/ 는 모든 컨테이너가 같은 볼륨을 쓰므로 한 곳에서만 실행하면 된다.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from common.defaults import DEFAULT_ARGS, PLATFORM_START_DATE

log = logging.getLogger(__name__)


def cleanup_logs(base_dir: str, retention_days: int, dry_run: bool = False, now: float | None = None) -> dict:
    """base_dir 아래에서 retention_days 보다 오래된 파일을 지우고, 비게 된 디렉터리도 정리한다."""
    cutoff = (now or time.time()) - retention_days * 86400
    removed_files, removed_bytes, removed_dirs = 0, 0, 0
    base = Path(base_dir)
    if not base.is_dir():
        return {"files": 0, "bytes": 0, "dirs": 0}

    for root, _dirs, files in os.walk(base, topdown=False):
        for name in files:
            p = Path(root) / name
            try:
                st = p.stat()
            except FileNotFoundError:
                continue
            if st.st_mtime < cutoff:
                removed_files += 1
                removed_bytes += st.st_size
                if not dry_run:
                    p.unlink(missing_ok=True)
        # 기준 디렉터리 자체는 남긴다
        if Path(root) != base and not dry_run:
            try:
                Path(root).rmdir()  # 비어 있을 때만 성공
                removed_dirs += 1
            except OSError:
                pass

    return {"files": removed_files, "bytes": removed_bytes, "dirs": removed_dirs}


def _run():
    retention = int(Variable.get("maintenance_log_retention_days", default_var=30))
    dry_run = Variable.get("maintenance_log_cleanup_dry_run", default_var="false").lower() == "true"
    base = conf.get("logging", "base_log_folder")
    result = cleanup_logs(base, retention, dry_run)
    log.info("log cleanup (%s, %sd, dry_run=%s): %s", base, retention, dry_run, result)
    return result


with DAG(
    dag_id="maintenance_airflow_log_cleanup",
    description="오래된 Airflow 로그 파일 정리",
    schedule="0 4 * * *",
    start_date=PLATFORM_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["maintenance"],
) as dag:
    PythonOperator(task_id="cleanup_logs", python_callable=_run)
