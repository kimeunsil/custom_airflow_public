"""웹 UI 상단 'Ops Menu' 에 Backfill / Schedule Pause 화면을 추가하는 플러그인."""

from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from ops_menu.views import BackfillView, SchedulePauseView

ops_menu_bp = Blueprint("ops_menu", __name__, template_folder="ops_menu/templates")


class OpsMenuPlugin(AirflowPlugin):
    name = "ops_menu"
    flask_blueprints = [ops_menu_bp]
    appbuilder_views = [
        {"name": "Backfill", "category": "Ops Menu", "view": BackfillView()},
        {"name": "Schedule Pause", "category": "Ops Menu", "view": SchedulePauseView()},
    ]
