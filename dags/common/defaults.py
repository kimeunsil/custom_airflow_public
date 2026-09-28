"""여러 DAG 에서 공통으로 쓰는 기본값."""

from datetime import timedelta

import pendulum
from airflow.settings import TIMEZONE  # [core] default_timezone

from common.alerts import task_failure_alert

LOCAL_TZ = TIMEZONE

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_alert,
}

# 새로 만드는 DAG 의 기준 시작일. catchup=False 와 함께 써서 과거 실행이 쌓이지 않게 한다
PLATFORM_START_DATE = pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ)
