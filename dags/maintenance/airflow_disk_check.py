"""서버 디스크 사용량 점검. 사용률이 임계치(Variable `maintenance_disk_alert_percent`, 기본 80)를 넘으면 Slack 알림."""

from __future__ import annotations

import shutil
import socket

from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from common.alerts import send_slack
from common.defaults import DEFAULT_ARGS, PLATFORM_START_DATE


def disk_report(path: str) -> dict:
    total, used, free = shutil.disk_usage(path)
    gib = 2**30
    return {
        "path": path,
        "total_gib": round(total / gib, 1),
        "used_gib": round(used / gib, 1),
        "free_gib": round(free / gib, 1),
        "used_percent": round(used / total * 100, 1),
    }


def _check():
    threshold = float(Variable.get("maintenance_disk_alert_percent", default_var=80))
    r = disk_report(conf.get("logging", "base_log_folder"))
    msg = (
        f"`{socket.gethostname()}` disk {r['used_percent']}% used "
        f"({r['used_gib']} / {r['total_gib']} GiB, free {r['free_gib']} GiB)"
    )
    if r["used_percent"] >= threshold:
        send_slack(f":warning: {msg} — 임계치 {threshold}% 초과")
    return r


with DAG(
    dag_id="maintenance_airflow_disk_check",
    description="Airflow 서버 디스크 사용량 점검",
    schedule="5 */6 * * *",
    start_date=PLATFORM_START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["maintenance"],
) as dag:
    PythonOperator(task_id="check_disk", python_callable=_check)
