"""테스트용 Airflow 환경: 임시 AIRFLOW_HOME + SQLite 메타 DB."""

import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_home = tempfile.mkdtemp(prefix="airflow-test-")

os.environ.setdefault("AIRFLOW_HOME", _home)
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(ROOT / "dags")
os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = str(ROOT / "plugins")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "true"
os.environ["AIRFLOW__CORE__DEFAULT_TIMEZONE"] = "Asia/Seoul"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{_home}/airflow.db"
os.environ["AIRFLOW__LOGGING__BASE_LOG_FOLDER"] = f"{_home}/logs"
os.environ["AIRFLOW__WEBSERVER__WTF_CSRF_ENABLED"] = "false"

sys.path[:0] = [str(ROOT / "dags"), str(ROOT / "plugins")]
