"""Ops Menu 플러그인: 입력 검증·명령 생성 단위 테스트 + 실제 웹서버 앱으로 권한/동작 확인."""

from datetime import date

import pendulum
import pytest

from ops_menu.core import (
    BackfillRequest,
    ValidationError,
    Window,
    build_command,
    parse_backfill_request,
    parse_window,
    window_bounds_utc,
)

# ── 순수 로직 ──────────────────────────────────────────────


def test_parse_backfill_ok():
    req = parse_backfill_request({"dag_id": "my_dag", "start_date": "2024-01-01", "end_date": "2024-01-03"})
    assert req == BackfillRequest("my_dag", date(2024, 1, 1), date(2024, 1, 3), "backfill")


@pytest.mark.parametrize(
    "form",
    [
        {"dag_id": "a; rm -rf /", "start_date": "2024-01-01", "end_date": "2024-01-02"},
        {"dag_id": "a", "start_date": "2024-01-05", "end_date": "2024-01-02"},
        {"dag_id": "a", "start_date": "20240101", "end_date": "2024-01-02"},
        {"dag_id": "a", "start_date": "2020-01-01", "end_date": "2024-01-02"},
        {"dag_id": "a", "start_date": "2024-01-01", "end_date": "2024-01-02", "mode": "delete"},
    ],
)
def test_parse_backfill_rejects(form):
    with pytest.raises(ValidationError):
        parse_backfill_request(form)


def test_build_command():
    b = BackfillRequest("d", date(2024, 1, 1), date(2024, 1, 2), "backfill", reset_dagruns=True)
    assert build_command(b) == [
        "airflow", "dags", "backfill", "--start-date", "2024-01-01", "--end-date", "2024-01-02",
        "--reset-dagruns", "--yes", "d",
    ]  # fmt: skip
    c = BackfillRequest("d", date(2024, 1, 1), date(2024, 1, 2), "clear", only_failed=True)
    assert build_command(c)[:3] == ["airflow", "tasks", "clear"] and "--only-failed" in build_command(c)


def test_window():
    assert parse_window({"start_hour": "0", "end_hour": "7"}) == Window(0, 7)
    for bad in ({"start_hour": "7", "end_hour": "7"}, {"start_hour": "-1", "end_hour": "3"}, {"start_hour": "x"}):
        with pytest.raises(ValidationError):
            parse_window(bad)
    s, e = window_bounds_utc(date(2024, 3, 10), Window(0, 7), pendulum.timezone("Asia/Seoul"))
    assert (str(s), str(e)) == ("2024-03-09 15:00:00+00:00", "2024-03-09 22:00:00+00:00")


# ── 웹서버 통합 ────────────────────────────────────────────


@pytest.fixture(scope="module")
def app():
    from airflow.utils import db

    db.resetdb()
    from airflow.www.app import create_app

    flask_app = create_app(testing=True)
    flask_app.config["WTF_CSRF_ENABLED"] = False
    sm = flask_app.appbuilder.sm
    for username, role in (("ops", "Op"), ("viewer", "Viewer")):
        if not sm.find_user(username=username):
            sm.add_user(username, username, "u", f"{username}@example.com", sm.find_role(role), "pw")
    return flask_app


def _login(app, username):
    c = app.test_client()
    r = c.post("/login/", data={"username": username, "password": "pw"})
    assert r.status_code in (200, 302)
    return c


def test_menu_registered(app):
    from airflow import plugins_manager

    plugins_manager.initialize_web_ui_plugins()
    names = {v["name"] for v in plugins_manager.flask_appbuilder_views}
    assert {"Backfill", "Schedule Pause"} <= names


def test_anonymous_and_viewer_blocked(app):
    assert app.test_client().get("/ops/backfill/").status_code in (302, 401)
    assert _login(app, "viewer").get("/ops/backfill/").status_code == 403


def test_pages_render_for_op(app):
    c = _login(app, "ops")
    for url in ("/ops/backfill/", "/ops/schedule-pause/"):
        r = c.get(url)
        assert r.status_code == 200, url
        assert "최근 작업 이력" in r.get_data(as_text=True)
    assert "sp-preset" in c.get("/ops/schedule-pause/").get_data(as_text=True)


def test_backfill_unknown_dag_rejected(app):
    r = _login(app, "ops").post(
        "/ops/backfill/run", data={"dag_id": "nope", "start_date": "2024-01-01", "end_date": "2024-01-01"}
    )
    assert r.status_code == 400 and "존재하지 않는" in r.get_json()["error"]


def test_schedule_pause_roundtrip(app):
    from airflow.models import DagModel
    from airflow.settings import TIMEZONE
    from airflow.utils.session import create_session

    now = pendulum.now(TIMEZONE)
    hour = now.hour
    in_window = now.start_of("hour").add(minutes=30).in_timezone("UTC")
    with create_session() as s:
        for dag_id in ("t_target", "t_excluded"):
            s.merge(DagModel(dag_id=dag_id, is_active=True, is_paused=False, next_dagrun_create_after=in_window))
        s.flush()
        from airflow.models import DagTag

        s.merge(DagTag(name="realtime", dag_id="t_excluded"))

    c = _login(app, "ops")
    form = {"start_hour": str(hour), "end_hour": str(hour + 1), "exclude_tag": "realtime"}
    preview = c.post("/ops/schedule-pause/preview", data=form).get_json()
    assert [d["dag_id"] for d in preview["dags"]] == ["t_target"]

    res = c.post("/ops/schedule-pause/apply", data={**form, "action": "pause"}).get_json()
    assert res["changed"] == ["t_target"]
    with create_session() as s:
        assert s.get(DagModel, "t_target").is_paused is True
        assert s.get(DagModel, "t_excluded").is_paused is False

    res = c.post("/ops/schedule-pause/apply", data={**form, "action": "unpause"}).get_json()
    assert res["changed"] == ["t_target"]
