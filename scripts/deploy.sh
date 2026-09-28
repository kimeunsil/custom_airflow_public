#!/usr/bin/env bash
# shellcheck disable=SC2029  # 원격 명령의 변수는 의도적으로 로컬에서 확장
# shellcheck disable=SC2029  # 원격 명령의 변수는 의도적으로 로컬에서 확장
# 서버에 DAG/플러그인/compose 파일을 동기화하고 필요한 컨테이너만 반영한다.
#
#   DEPLOY_HOST=airflow.example.internal DEPLOY_USER=deploy DEPLOY_DIR=/srv/airflow \
#   WORKERS=3 scripts/deploy.sh [--restart]
#
# - dags/ 변경은 스케줄러가 주기적으로 다시 읽으므로 재시작 없이 반영된다.
# - plugins/ 나 compose 설정이 바뀌면 --restart 로 컨테이너를 재생성한다 (worker 는 warm shutdown).
# - 서버의 .env(비밀값)는 동기화 대상이 아니다.
set -euo pipefail

: "${DEPLOY_HOST:?DEPLOY_HOST 필요}"
: "${DEPLOY_USER:?DEPLOY_USER 필요}"
DEPLOY_DIR="${DEPLOY_DIR:-/srv/airflow}"
WORKERS="${WORKERS:-1}"
RESTART=false
[[ "${1:-}" == "--restart" ]] && RESTART=true

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REMOTE="${DEPLOY_USER}@${DEPLOY_HOST}"
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=accept-new)

echo "▶ sync → ${REMOTE}:${DEPLOY_DIR}"
ssh "${SSH_OPTS[@]}" "$REMOTE" "mkdir -p '${DEPLOY_DIR}/logs'"
rsync -az --delete -e "ssh ${SSH_OPTS[*]}" \
  --exclude '__pycache__' --exclude '*.pyc' \
  "$ROOT/dags" "$ROOT/plugins" "$REMOTE:${DEPLOY_DIR}/"
rsync -az -e "ssh ${SSH_OPTS[*]}" "$ROOT/docker-compose.yml" "$ROOT/docker-compose.infra.yml" "$ROOT/Makefile" "$REMOTE:${DEPLOY_DIR}/"

if $RESTART; then
  echo "▶ restart containers (workers=${WORKERS})"
  ssh "${SSH_OPTS[@]}" "$REMOTE" "cd '${DEPLOY_DIR}' && test -f .env && \
    docker compose up -d --remove-orphans --scale airflow-worker=${WORKERS} && \
    docker compose ps"
else
  echo "▶ dags/plugins 동기화만 수행 (컨테이너 재시작 없음)"
fi
