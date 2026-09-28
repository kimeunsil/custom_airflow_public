"""sql/ 디렉터리의 .sql 파일 하나당 BigQuery DAG 하나를 자동 생성한다.

- 스케줄은 디렉터리 경로로 결정 (common/sql_dag_spec.py 참고)
- 파일 헤더의 `depends_on` 에 선언한 원천 테이블에 대상 날짜 파티션이 적재됐는지 먼저 확인
- SQL 안에서 `{{ target_date }}`(실행 시점 기준 전날, YYYY-MM-DD) 와 Airflow 기본 매크로 사용 가능

새 집계 테이블이 필요하면 SQL 파일만 추가하면 되고 파이썬 코드는 건드리지 않는다.
"""

from __future__ import annotations

import os
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator

from common.defaults import DEFAULT_ARGS, LOCAL_TZ, PLATFORM_START_DATE
from common.sql_dag_spec import SqlDagSpec, discover

SQL_ROOT = Path(__file__).parent / "sql"
GCP_CONN_ID = os.environ.get("SQL_DAG_GCP_CONN_ID", "google_cloud_default")
BQ_LOCATION = os.environ.get("SQL_DAG_BQ_LOCATION", "US")


def target_date(data_interval_end) -> str:
    """실행 시점(로컬 타임존)의 전날. 일/주/월 배치 모두 '어제까지 적재된 데이터' 기준으로 집계한다."""
    return data_interval_end.in_timezone(LOCAL_TZ).subtract(days=1).to_date_string()


def build_dag(spec: SqlDagSpec) -> DAG:
    dag = DAG(
        dag_id=spec.dag_id,
        description=spec.description or f"SQL DAG from sql/{spec.path}",
        doc_md=f"자동 생성 DAG — 원본: `dags/sql/{spec.path}`\n\n```sql\n{spec.sql}\n```",
        schedule=spec.schedule,
        start_date=PLATFORM_START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        user_defined_macros={"target_date_of": target_date},
        render_template_as_native_obj=False,
        tags=["sql-dag", spec.cadence, *spec.tags],
    )
    with dag:
        run = BigQueryInsertJobOperator(
            task_id="run_sql",
            gcp_conn_id=GCP_CONN_ID,
            location=BQ_LOCATION,
            configuration={
                "query": {
                    # target_date 매크로를 SQL 앞에 set 으로 주입
                    "query": "{% set target_date = target_date_of(data_interval_end) %}" + spec.sql,
                    "useLegacySql": False,
                }
            },
        )
        for i, dep in enumerate(spec.depends_on):
            check = BigQueryCheckOperator(
                task_id=f"check_source_{i}_{dep.table.split('.')[-1]}",
                gcp_conn_id=GCP_CONN_ID,
                location=BQ_LOCATION,
                use_legacy_sql=False,
                sql=(
                    "{% set target_date = target_date_of(data_interval_end) %}"
                    f"SELECT COUNT(*) > 0 FROM `{dep.table}` WHERE {dep.partition_column} = '{{{{ target_date }}}}'"
                ),
                retries=6,  # 원천 적재가 늦으면 30분 간격으로 3시간까지 재확인
                retry_delay=1800,
            )
            check >> run
    return dag


for _spec in discover(SQL_ROOT):
    globals()[_spec.dag_id] = build_dag(_spec)
