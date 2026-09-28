"""전날 DAG 실행 결과를 상태별로 집계해 Slack 으로 보고. 실패한 DAG 목록도 함께 보낸다."""

from __future__ import annotations

import os
from collections import Counter

from airflow import DAG
from airflow.models import DagModel, DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session

from common.alerts import send_slack
from common.defaults import DEFAULT_ARGS, LOCAL_TZ, PLATFORM_START_DATE

REPORT_CHANNEL = os.environ.get("REPORT_SLACK_CHANNEL", "#airflow-report")
SELF_DAG_ID = "maintenance_airflow_daily_report"


def build_report(runs: list[tuple[str, str]], date_label: str) -> str:
    """runs: (dag_id, state) 목록"""
    counts = Counter(state for _, state in runs)
    failed = sorted({dag_id for dag_id, state in runs if state == "failed"})
    lines = [f":mega: *Airflow daily report* ({date_label})"]
    lines += [f"• {state}: {cnt}" for state, cnt in sorted(counts.items())]
    lines.append(f"• total: {sum(counts.values())}")
    if failed:
        lines.append("*Failed DAGs*: " + ", ".join(f"`{d}`" for d in failed))
    return "\n".join(lines)


@provide_session
def _report(data_interval_start=None, data_interval_end=None, session=None, **_):
    rows = (
        session.query(DagRun.dag_id, DagRun.state)
        .join(DagModel, DagModel.dag_id == DagRun.dag_id)
        .filter(
            DagRun.start_date >= data_interval_start,
            DagRun.start_date < data_interval_end,
            DagModel.is_active.is_(True),
            DagModel.is_paused.is_(False),
            DagRun.dag_id != SELF_DAG_ID,
        )
        .all()
    )
    label = data_interval_start.in_timezone(LOCAL_TZ).to_date_string()
    text = build_report([(r.dag_id, str(r.state)) for r in rows], label)
    send_slack(text, channel=REPORT_CHANNEL)
    return text


with DAG(
    dag_id=SELF_DAG_ID,
    description="전날 DAG 실행 결과 요약",
    schedule="0 10 * * *",
    start_date=PLATFORM_START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["maintenance"],
) as dag:
    PythonOperator(task_id="report", python_callable=_report)
