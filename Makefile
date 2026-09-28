.PHONY: init up down restart ps logs scale test lint

COMPOSE := docker compose

# .env 가 없으면 예시에서 복사하고 Fernet / Webserver secret 키를 채운다
init:
	@test -f .env || cp .env.example .env
	@grep -q '^AIRFLOW__CORE__FERNET_KEY=.\+' .env || \
	  sed -i.bak "s|^AIRFLOW__CORE__FERNET_KEY=.*|AIRFLOW__CORE__FERNET_KEY=$$(python3 -c 'import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())')|" .env
	@grep -q '^AIRFLOW__WEBSERVER__SECRET_KEY=.\+' .env || \
	  sed -i.bak "s|^AIRFLOW__WEBSERVER__SECRET_KEY=.*|AIRFLOW__WEBSERVER__SECRET_KEY=$$(python3 -c 'import secrets;print(secrets.token_hex(32))')|" .env
	@rm -f .env.bak
	@mkdir -p logs
	@grep -q '^AIRFLOW_UID=' .env && sed -i.bak "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$$(id -u)|" .env && rm -f .env.bak
	@echo ".env 준비 완료 — POSTGRES/REDIS/ADMIN 비밀번호를 변경하세요"

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

# 실행 중 태스크를 마무리한 뒤 재기동 (worker 는 warm shutdown)
restart:
	$(COMPOSE) up -d --force-recreate --no-deps airflow-webserver airflow-scheduler airflow-triggerer airflow-worker

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100 $(s)

scale:
	$(COMPOSE) up -d --no-recreate --scale airflow-worker=$(n)

test:
	pytest -q

lint:
	ruff check . && ruff format --check .
