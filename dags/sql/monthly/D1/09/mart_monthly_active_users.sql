-- description: 월간 활성 사용자 수 (전월)
-- tags: mart, user
-- depends_on: my-project.raw.app_events:event_date

DELETE FROM `my-project.mart.monthly_active_users`
WHERE stat_month = DATE_TRUNC(DATE('{{ target_date }}'), MONTH);

INSERT INTO `my-project.mart.monthly_active_users` (stat_month, mau)
SELECT
  DATE_TRUNC(DATE('{{ target_date }}'), MONTH) AS stat_month,
  COUNT(DISTINCT user_id) AS mau
FROM `my-project.raw.app_events`
WHERE event_date BETWEEN DATE_TRUNC(DATE('{{ target_date }}'), MONTH) AND DATE('{{ target_date }}');
