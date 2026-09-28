"""모든 DAG 가 import 오류 없이 로드되고 기본 규칙을 지키는지 검사."""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_loaded(dagbag):
    expected = {
        "maintenance_airflow_db_cleanup",
        "maintenance_airflow_log_cleanup",
        "maintenance_airflow_disk_check",
        "maintenance_airflow_daily_report",
        "mart_daily_active_users",
        "mart_weekly_top_contents",
        "mart_monthly_active_users",
    }
    assert expected <= set(dagbag.dag_ids)


@pytest.mark.parametrize("attr", ["tags", "catchup"])
def test_dag_conventions(dagbag, attr):
    for dag_id, dag in dagbag.dags.items():
        if attr == "tags":
            assert dag.tags, f"{dag_id}: tags 가 비어 있음"
        else:
            assert dag.catchup is False, f"{dag_id}: catchup=False 여야 함"


def test_all_tasks_have_failure_alert(dagbag):
    from common.alerts import task_failure_alert

    for dag in dagbag.dags.values():
        for task in dag.tasks:
            cb = task.on_failure_callback
            cbs = cb if isinstance(cb, list) else [cb]
            assert task_failure_alert in cbs, f"{dag.dag_id}.{task.task_id}: 실패 알림 누락"
