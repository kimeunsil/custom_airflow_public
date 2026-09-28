"""Ops Menu 화면.

Backfill        : DAG 를 기간 지정으로 backfill / clear. 웹서버 요청과 분리된 백그라운드 프로세스로
                  실행하고, 화면은 로그 파일을 폴링해 진행 상황을 보여준다.
Schedule Pause  : 오늘 지정 시간대에 실행 예정인 DAG 를 일괄 일시정지/재개.
                  원천 데이터 적재 지연이나 DW 점검 시간에 연쇄 실패를 막기 위한 용도.
"""

from __future__ import annotations

import re
import subprocess
import uuid

import pendulum
from airflow.models import DagModel, DagTag
from airflow.settings import TIMEZONE
from airflow.utils.session import create_session
from flask import jsonify, request
from flask_appbuilder import BaseView, expose

from ops_menu import history
from ops_menu.auth import current_username, require_ops_role
from ops_menu.core import (
    ValidationError,
    build_command,
    parse_backfill_request,
    parse_window,
    window_bounds_utc,
)

_JOB_ID = re.compile(r"^[a-f0-9]{32}$")


def _bad_request(e: Exception):
    return jsonify({"ok": False, "error": str(e)}), 400


class BackfillView(BaseView):
    route_base = "/ops/backfill"
    default_view = "index"

    @expose("/")
    @require_ops_role
    def index(self):
        return self.render_template("ops_menu/backfill.html", history=history.recent())

    @expose("/run", methods=["POST"])
    @require_ops_role
    def run(self):
        try:
            req = parse_backfill_request(request.form)
        except ValidationError as e:
            return _bad_request(e)
        with create_session() as session:
            if not session.query(DagModel.dag_id).filter(DagModel.dag_id == req.dag_id).first():
                return _bad_request(ValidationError(f"존재하지 않는 DAG: {req.dag_id}"))

        cmd = build_command(req)
        job_id = uuid.uuid4().hex
        log_path = history.ops_dir() / f"backfill_{job_id}.log"
        with open(log_path, "w", encoding="utf-8") as out:
            out.write("$ " + " ".join(cmd) + "\n")
            out.flush()
            # 웹서버 워커와 분리된 세션으로 실행 → 요청 타임아웃/워커 재시작의 영향을 받지 않음
            subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)  # noqa: S603

        history.record(req.mode, current_username(), dag_id=req.dag_id, command=cmd, job_id=job_id)
        return jsonify({"ok": True, "job_id": job_id, "command": " ".join(cmd)})

    @expose("/log/<job_id>")
    @require_ops_role
    def log(self, job_id: str):
        if not _JOB_ID.match(job_id):
            return _bad_request(ValidationError("잘못된 job id"))
        return history.tail(history.ops_dir() / f"backfill_{job_id}.log"), 200, {"Content-Type": "text/plain"}


class SchedulePauseView(BaseView):
    route_base = "/ops/schedule-pause"
    default_view = "index"

    @expose("/")
    @require_ops_role
    def index(self):
        return self.render_template("ops_menu/schedule_pause.html", history=history.recent(), timezone=TIMEZONE.name)

    @staticmethod
    def _targets(session, window, exclude_tag: str | None):
        today = pendulum.now(TIMEZONE).date()
        start, end = window_bounds_utc(today, window, TIMEZONE)
        q = session.query(DagModel).filter(
            DagModel.is_active.is_(True),
            DagModel.next_dagrun_create_after >= start,
            DagModel.next_dagrun_create_after < end,
        )
        if exclude_tag:
            tagged = session.query(DagTag.dag_id).filter(DagTag.name == exclude_tag)
            q = q.filter(DagModel.dag_id.not_in(tagged))
        return q.order_by(DagModel.dag_id).all(), (start, end)

    @expose("/preview", methods=["POST"])
    @require_ops_role
    def preview(self):
        try:
            window = parse_window(request.form)
        except ValidationError as e:
            return _bad_request(e)
        exclude = request.form.get("exclude_tag", "").strip() or None
        with create_session() as session:
            dags, (s, e) = self._targets(session, window, exclude)
            rows = [
                {"dag_id": d.dag_id, "is_paused": d.is_paused, "next_run": str(d.next_dagrun_create_after)}
                for d in dags
            ]
        return jsonify({"ok": True, "window_utc": [str(s), str(e)], "dags": rows})

    @expose("/apply", methods=["POST"])
    @require_ops_role
    def apply(self):
        try:
            window = parse_window(request.form)
        except ValidationError as e:
            return _bad_request(e)
        action = request.form.get("action")
        if action not in ("pause", "unpause"):
            return _bad_request(ValidationError("action 은 pause 또는 unpause"))
        exclude = request.form.get("exclude_tag", "").strip() or None
        paused = action == "pause"
        with create_session() as session:
            dags, _ = self._targets(session, window, exclude)
            changed = [d.dag_id for d in dags if d.is_paused != paused]
            for d in dags:
                d.is_paused = paused
        history.record(
            action,
            current_username(),
            window=f"{window.start_hour:02d}-{window.end_hour:02d}",
            exclude_tag=exclude,
            dags=changed,
        )
        return jsonify({"ok": True, "action": action, "changed": changed})
