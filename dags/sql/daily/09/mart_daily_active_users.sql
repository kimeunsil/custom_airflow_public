-- description: 일별 활성 사용자 수 집계
-- tags: mart, user
-- depends_on: my-project.raw.app_events:event_date

MERGE `my-project.mart.daily_active_users` T
USING (
  SELECT
    DATE('{{ target_date }}') AS stat_date,
    platform,
    COUNT(DISTINCT user_id) AS dau
  FROM `my-project.raw.app_events`
  WHERE event_date = '{{ target_date }}'
  GROUP BY platform
) S
ON T.stat_date = S.stat_date AND T.platform = S.platform
WHEN MATCHED THEN UPDATE SET dau = S.dau
WHEN NOT MATCHED THEN INSERT (stat_date, platform, dau) VALUES (S.stat_date, S.platform, S.dau);
