"""메타 DB 정리: 보관 기간이 지난 dag_run / task_instance / log / xcom 등 삭제.

Airflow 내장 `airflow db clean` 을 사용한다. 보관 일수는 Variable `maintenance_db_retention_days`(기본 90).
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

from common.defaults import DEFAULT_ARGS, PLATFORM_START_DATE

with DAG(
    dag_id="maintenance_airflow_db_cleanup",
    description="Airflow 메타 DB 오래된 레코드 정리",
    schedule="30 3 * * *",
    start_date=PLATFORM_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["maintenance"],
) as dag:
    BashOperator(
        task_id="db_clean",
        bash_command=(
            "airflow db clean "
            "--clean-before-timestamp "
            "'{{ macros.ds_add(ds, -(var.value.get(\"maintenance_db_retention_days\", 90) | int)) }}' "
            "--skip-archive --yes"
        ),
    )
