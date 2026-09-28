from pathlib import Path

import pendulum
import pytest

from common.sql_dag_spec import SqlDagSpecError, discover, parse_header, schedule_from_path


@pytest.mark.parametrize(
    "rel, cron",
    [
        ("daily/9/a.sql", "0 9 * * *"),
        ("daily/09/a.sql", "0 9 * * *"),
        ("weekly/monday/10/a.sql", "0 10 * * 1"),
        ("weekly/Sunday/0/a.sql", "0 0 * * 0"),
        ("monthly/D1/9/a.sql", "0 9 1 * *"),
        ("monthly/D15/23/a.sql", "0 23 15 * *"),
    ],
)
def test_schedule_from_path(rel, cron):
    assert schedule_from_path(Path(rel))[0] == cron


@pytest.mark.parametrize(
    "rel", ["daily/24/a.sql", "weekly/mon/9/a.sql", "monthly/1/9/a.sql", "monthly/D31/9/a.sql", "hourly/1/a.sql"]
)
def test_invalid_paths(rel):
    with pytest.raises(SqlDagSpecError):
        schedule_from_path(Path(rel))


def test_parse_header():
    sql = (
        "-- description: 설명\n-- 그냥 주석\n-- tags: a, b\n"
        "-- depends_on: p.d.t1:dt, p.d.t2:day\nSELECT 1\n-- tags: ignored"
    )
    meta = parse_header(sql)
    assert meta["description"] == "설명"
    assert meta["tags"] == ["a", "b"]
    assert [(d.table, d.partition_column) for d in meta["depends_on"]] == [("p.d.t1", "dt"), ("p.d.t2", "day")]


def test_bad_dependency_format():
    with pytest.raises(SqlDagSpecError):
        parse_header("-- depends_on: dataset.table\nSELECT 1")


def test_duplicate_dag_id(tmp_path):
    for rel in ("daily/9/x.sql", "weekly/monday/9/x.sql"):
        p = tmp_path / rel
        p.parent.mkdir(parents=True)
        p.write_text("SELECT 1")
    with pytest.raises(SqlDagSpecError, match="중복"):
        discover(tmp_path)


def test_generated_dag_structure():
    from airflow.models import DagBag

    dag = DagBag(include_examples=False).get_dag("mart_daily_active_users")
    assert dag.schedule_interval == "0 9 * * *"
    assert {t.task_id for t in dag.tasks} == {"run_sql", "check_source_0_app_events"}
    assert dag.get_task("run_sql").upstream_task_ids == {"check_source_0_app_events"}


def test_target_date_rendering():
    """실행 시점(KST) 기준 전날이 SQL 에 들어가는지 실제 템플릿 렌더링으로 확인."""
    from airflow.models import DagBag

    dag = DagBag(include_examples=False).get_dag("mart_daily_active_users")
    task = dag.get_task("run_sql")
    # 2024-03-10 09:00 KST 실행 = data_interval_end 2024-03-10T00:00Z
    end = pendulum.datetime(2024, 3, 10, 0, 0, tz="UTC")
    ctx = {"data_interval_end": end, **dag.user_defined_macros}
    rendered = task.render_template(task.configuration["query"]["query"], ctx, jinja_env=dag.get_template_env())
    assert "event_date = '2024-03-09'" in rendered
    assert "{{" not in rendered
