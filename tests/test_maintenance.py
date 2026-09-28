import os
import time

from maintenance.airflow_daily_report import build_report
from maintenance.airflow_log_cleanup import cleanup_logs


def test_cleanup_logs(tmp_path):
    old = tmp_path / "dag_id=a" / "run_id=1" / "attempt=1.log"
    new = tmp_path / "dag_id=a" / "run_id=2" / "attempt=1.log"
    for p in (old, new):
        p.parent.mkdir(parents=True)
        p.write_text("x" * 10)
    now = time.time()
    os.utime(old, (now - 40 * 86400, now - 40 * 86400))

    dry = cleanup_logs(str(tmp_path), 30, dry_run=True, now=now)
    assert dry["files"] == 1 and old.exists()

    res = cleanup_logs(str(tmp_path), 30, now=now)
    assert res == {"files": 1, "bytes": 10, "dirs": 1}
    assert not old.exists() and not old.parent.exists() and new.exists()
    assert tmp_path.exists()


def test_build_report():
    text = build_report([("a", "success"), ("b", "failed"), ("b", "failed"), ("c", "success")], "2024-03-09")
    assert "success: 2" in text and "failed: 2" in text and "total: 4" in text
    assert "`b`" in text and "`a`" not in text
