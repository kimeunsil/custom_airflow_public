-- description: 지난 7일 인기 콘텐츠 Top 100
-- tags: mart, content
-- depends_on: my-project.raw.play_logs:play_date

CREATE OR REPLACE TABLE `my-project.mart.weekly_top_contents` AS
SELECT
  DATE_SUB(DATE('{{ target_date }}'), INTERVAL 6 DAY) AS week_start,
  DATE('{{ target_date }}') AS week_end,
  content_id,
  COUNT(*) AS play_cnt,
  COUNT(DISTINCT user_id) AS listener_cnt
FROM `my-project.raw.play_logs`
WHERE play_date BETWEEN DATE_SUB(DATE('{{ target_date }}'), INTERVAL 6 DAY) AND DATE('{{ target_date }}')
GROUP BY content_id
QUALIFY ROW_NUMBER() OVER (ORDER BY play_cnt DESC) <= 100;
