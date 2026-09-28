"""Slack 알림 콜백.

    from common.alerts import task_failure_alert, dag_success_alert

    default_args = {"on_failure_callback": task_failure_alert}
    DAG(..., on_success_callback=dag_success_alert)

Slack 토큰은 Airflow Connection(`ALERT_SLACK_CONN_ID`, 기본 slack_api_default)에만 저장하고
코드나 환경변수에 두지 않습니다. 연결이 없거나 전송에 실패해도 태스크 결과에는 영향을 주지 않습니다.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

SLACK_CONN_ID = os.environ.get("ALERT_SLACK_CONN_ID", "slack_api_default")
ALERT_CHANNEL = os.environ.get("ALERT_SLACK_CHANNEL", "#airflow-alerts")


def send_slack(text: str, channel: str | None = None, blocks: list | None = None) -> bool:
    """Slack 메시지 전송. 실패하면 False 를 돌려주고 예외를 올리지 않는다."""
    try:
        from airflow.providers.slack.hooks.slack import SlackHook

        SlackHook(slack_conn_id=SLACK_CONN_ID).client.chat_postMessage(
            channel=channel or ALERT_CHANNEL, text=text, blocks=blocks
        )
        return True
    except Exception as e:  # 알림 실패가 파이프라인 실패로 번지지 않도록
        log.warning("Slack 전송 실패(%s): %s", SLACK_CONN_ID, e)
        return False


def format_task_message(context: dict, status: str) -> str:
    ti = context["task_instance"]
    emoji = {"failed": ":red_circle:", "success": ":large_blue_circle:", "retry": ":large_yellow_circle:"}[status]
    lines = [
        f"{emoji} *Task {status}*",
        f"*DAG*: {ti.dag_id}",
        f"*Task*: {ti.task_id}",
        f"*Logical date*: {context.get('logical_date')}",
        f"*Try*: {ti.try_number}",
    ]
    if status != "success":
        exc = context.get("exception")
        if exc:
            lines.append(f"*Error*: `{str(exc)[:300]}`")
        # 로그 URL 은 [webserver] base_url 로 만들어지므로 외부 주소로 설정해 두면 바로 열린다
        lines.append(f"*Log*: {ti.log_url}")
    return "\n".join(lines)


def task_failure_alert(context: dict) -> None:
    send_slack(format_task_message(context, "failed"))


def task_retry_alert(context: dict) -> None:
    send_slack(format_task_message(context, "retry"))


def dag_success_alert(context: dict) -> None:
    dag_run = context["dag_run"]
    send_slack(f":large_blue_circle: *DAG success* `{dag_run.dag_id}` ({context.get('logical_date')})")
